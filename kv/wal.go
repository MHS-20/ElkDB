package kv

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/MHS-20/ElkDB/btree"
)

const (
	walSig     = "ElkWAL\000"
	walVersion = uint32(1)
)

const (
	walBeginTX  byte = 1
	walPageData byte = 2
	walCommitTX byte = 3
)

type WAL struct {
	fp   *os.File
	path string
}

func OpenWAL(path string) (*WAL, error) {
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, err
	}
	wal := &WAL{fp: fp, path: path}
	if fi.Size() == 0 {
		header := make([]byte, 16)
		copy(header, walSig)
		binary.LittleEndian.PutUint32(header[8:], walVersion)
		if _, err := fp.Write(header); err != nil {
			fp.Close()
			return nil, err
		}
	}
	return wal, nil
}

func (wal *WAL) Close() error {
	return wal.fp.Close()
}

func (wal *WAL) Sync() error {
	return wal.fp.Sync()
}

func (wal *WAL) HasData() (bool, error) {
	fi, err := wal.fp.Stat()
	if err != nil {
		return false, err
	}
	return fi.Size() > 16, nil
}

func (wal *WAL) BeginTX(txID uint64) error {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, txID)
	return wal.writeRecord(walBeginTX, payload)
}

func (wal *WAL) PageData(txID uint64, pageNum uint64, data []byte) error {
	payload := make([]byte, 8+8+btree.PageSize)
	binary.LittleEndian.PutUint64(payload, txID)
	binary.LittleEndian.PutUint64(payload[8:], pageNum)
	copy(payload[16:], data)
	return wal.writeRecord(walPageData, payload)
}

type commitState struct {
	Root        uint64
	FreeHead    uint64
	PageFlushed uint64
}

func (wal *WAL) CommitTX(txID uint64, state commitState) error {
	payload := make([]byte, 8+8+8+8)
	binary.LittleEndian.PutUint64(payload, txID)
	binary.LittleEndian.PutUint64(payload[8:], state.Root)
	binary.LittleEndian.PutUint64(payload[16:], state.FreeHead)
	binary.LittleEndian.PutUint64(payload[24:], state.PageFlushed)
	return wal.writeRecord(walCommitTX, payload)
}

func (wal *WAL) writeRecord(recType byte, payload []byte) error {
	buf := make([]byte, 1+4+4+len(payload))
	buf[0] = recType
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(buf[1:], crc)
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(payload)))
	copy(buf[9:], payload)
	_, err := wal.fp.Write(buf)
	return err
}

type walEntry struct {
	pageNum uint64
	data    []byte
}

func (wal *WAL) readCommitted() ([]walEntry, *commitState, error) {
	fi, err := wal.fp.Stat()
	if err != nil {
		return nil, nil, err
	}
	if fi.Size() <= 16 {
		return nil, nil, nil
	}

	data := make([]byte, fi.Size())
	if _, err := wal.fp.ReadAt(data, 0); err != nil {
		return nil, nil, fmt.Errorf("read WAL: %w", err)
	}

	committed := map[uint64]bool{}
	var lastState *commitState
	txPages := map[uint64][]walEntry{}

	pos := int64(16)
	for pos+9 <= int64(len(data)) {
		recType := data[pos]
		crc := binary.LittleEndian.Uint32(data[pos+1:])
		payloadLen := binary.LittleEndian.Uint32(data[pos+5:])

		if pos+9+int64(payloadLen) > int64(len(data)) {
			break
		}

		payload := data[pos+9 : pos+9+int64(payloadLen)]
		if crc != crc32.ChecksumIEEE(payload) {
			break
		}

		switch recType {
		case walBeginTX:
			txID := binary.LittleEndian.Uint64(payload)
			if _, ok := txPages[txID]; !ok {
				txPages[txID] = nil
			}

		case walPageData:
			txID := binary.LittleEndian.Uint64(payload)
			pageNum := binary.LittleEndian.Uint64(payload[8:])
			pg := make([]byte, btree.PageSize)
			copy(pg, payload[16:])
			txPages[txID] = append(txPages[txID], walEntry{pageNum, pg})

		case walCommitTX:
			txID := binary.LittleEndian.Uint64(payload)
			committed[txID] = true
			lastState = &commitState{
				Root:        binary.LittleEndian.Uint64(payload[8:]),
				FreeHead:    binary.LittleEndian.Uint64(payload[16:]),
				PageFlushed: binary.LittleEndian.Uint64(payload[24:]),
			}
		}

		pos += 9 + int64(payloadLen)
	}

	var entries []walEntry
	for txID, txEntries := range txPages {
		if committed[txID] {
			entries = append(entries, txEntries...)
		}
	}

	lastIdx := map[uint64]int{}
	for i, e := range entries {
		lastIdx[e.pageNum] = i
	}
	var deduped []walEntry
	for i, e := range entries {
		if lastIdx[e.pageNum] == i {
			deduped = append(deduped, e)
		}
	}

	return deduped, lastState, nil
}

func (wal *WAL) Recover(kv *KV) error {
	entries, state, err := wal.readCommitted()
	if err != nil {
		return err
	}
	if state == nil || len(entries) == 0 {
		return wal.reset()
	}
	return wal.checkpointApply(kv, entries, state)
}

func (wal *WAL) Checkpoint(kv *KV) error {
	entries, state, err := wal.readCommitted()
	if err != nil {
		return err
	}
	if state == nil || len(entries) == 0 {
		return wal.reset()
	}
	return wal.checkpointApply(kv, entries, state)
}

func (wal *WAL) checkpointApply(kv *KV, entries []walEntry, state *commitState) error {
	npages := int(state.PageFlushed)
	if err := extendFile(kv, npages); err != nil {
		return fmt.Errorf("checkpoint extend file: %w", err)
	}
	if err := extendMmap(kv, npages); err != nil {
		return fmt.Errorf("checkpoint extend mmap: %w", err)
	}

	kv.mmapMu.Lock()
	for _, e := range entries {
		copy(pageGetMapped(kv.mmap.chunks, e.pageNum).Data, e.data)
	}
	kv.mmapMu.Unlock()

	kv.tree.root = state.Root
	kv.free.Head = state.FreeHead
	kv.page.flushed = state.PageFlushed
	kv.pageAlloc = state.PageFlushed

	if err := masterStore(kv); err != nil {
		return fmt.Errorf("checkpoint master store: %w", err)
	}
	if !kv.NoSync {
		if err := kv.fp.Sync(); err != nil {
			return fmt.Errorf("checkpoint fsync: %w", err)
		}
	}

	return wal.reset()
}

func (wal *WAL) reset() error {
	if err := wal.fp.Truncate(0); err != nil {
		return err
	}
	if _, err := wal.fp.Seek(0, 0); err != nil {
		return err
	}
	header := make([]byte, 16)
	copy(header, walSig)
	binary.LittleEndian.PutUint32(header[8:], walVersion)
	if _, err := wal.fp.Write(header); err != nil {
		return err
	}
	return nil
}
