// Package kv provides a transactional key-value store backed by a memory-mapped
// file. It layers two responsibilities: a pager that manages the mmap region,
// page allocation, and a free list for reclaiming deleted pages; and a
// transaction engine that implements PageStore on top of the pager,
// giving each transaction a consistent snapshot and copy-on-write isolation.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/MHS-20/ElkDB/btree"
)

const dbSig = "ElkDB"

// KV is the top-level database handle.
// Open it with KV.Open, then create transactions with Begin / BeginRead.
type KV struct {
	Path   string
	NoSync bool // skip fsync (useful in tests; dangerous in production)

	fp   *os.File
	tree struct {
		root uint64
	}
	free btree.FreeListData
	mmap struct {
		file   int      // file size in bytes (can exceed database size)
		total  int      // total mapped bytes (can exceed file size)
		chunks [][]byte // one or more mmap regions
	}
	page struct {
		flushed uint64 // database size in pages
	}

	mu      sync.Mutex
	writer  sync.Mutex // held for the duration of every write transaction
	version uint64
	readers readerList // min-heap tracking the oldest active reader version
}

// Open opens or creates the database file at db.Path.
func (kv *KV) Open() error {
	fp, err := os.OpenFile(kv.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	kv.fp = fp

	sz, chunk, err := mmapInit(kv.fp)
	if err != nil {
		goto fail
	}
	kv.mmap.file = sz
	kv.mmap.total = len(chunk)
	kv.mmap.chunks = [][]byte{chunk}

	err = masterLoad(kv)
	if err != nil {
		goto fail
	}
	return nil

fail:
	kv.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// Close unmaps all pages and closes the file.
func (kv *KV) Close() {
	for _, chunk := range kv.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = kv.fp.Close()
}

// --- mmap helpers ---

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%btree.PageSize != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20
	assert(mmapSize%btree.PageSize == 0)
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

func extendFile(kv *KV, npages int) error {
	filePages := kv.mmap.file / btree.PageSize
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		inc := max(filePages/8, 1)
		filePages += inc
	}
	fileSize := filePages * btree.PageSize
	if err := syscall.Fallocate(int(kv.fp.Fd()), 0, 0, int64(fileSize)); err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	kv.mmap.file = fileSize
	return nil
}

func extendMmap(kv *KV, npages int) error {
	if kv.mmap.total >= npages*btree.PageSize {
		return nil
	}
	chunk, err := syscall.Mmap(
		int(kv.fp.Fd()), int64(kv.mmap.total), kv.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	kv.mmap.total += len(chunk)
	kv.mu.Lock()
	kv.mmap.chunks = append(kv.mmap.chunks, chunk)
	kv.mu.Unlock()
	return nil
}

// --- master page ---
func masterLoad(kv *KV) error {
	if kv.mmap.file == 0 {
		kv.page.flushed = 1
		return nil
	}

	data := kv.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	free := binary.LittleEndian.Uint64(data[32:])
	version := binary.LittleEndian.Uint64(data[40:])

	if !bytes.Equal([]byte(dbSig), data[:len(dbSig)]) {
		return errors.New("bad signature")
	}
	bad := 1 > used || used > uint64(kv.mmap.file/btree.PageSize)
	bad = bad || root >= used
	bad = bad || free >= used
	if bad {
		return errors.New("bad master page")
	}

	kv.tree.root = root
	kv.free.Head = free
	kv.page.flushed = used
	kv.version = version
	return nil
}

func masterStore(kv *KV) error {
	var data [48]byte
	copy(data[:16], []byte(dbSig))
	binary.LittleEndian.PutUint64(data[16:], kv.tree.root)
	binary.LittleEndian.PutUint64(data[24:], kv.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], kv.free.Head)
	binary.LittleEndian.PutUint64(data[40:], kv.version)
	_, err := kv.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// --- reader heap ---

// readerList is a min-heap of active read transactions ordered by version.
type readerList []*KVReader

func (h readerList) Len() int           { return len(h) }
func (h readerList) Less(i, j int) bool { return h[i].version < h[j].version }
func (h readerList) Swap(i, j int)      { h[i].index, h[j].index = j, i; h[i], h[j] = h[j], h[i] }
func (h *readerList) Push(x any)        { r := x.(*KVReader); r.index = len(*h); *h = append(*h, r) }
func (h *readerList) Pop() any          { x := (*h)[len(*h)-1]; *h = (*h)[:len(*h)-1]; return x }

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}
