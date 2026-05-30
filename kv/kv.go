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
func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// Close unmaps all pages and closes the file.
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = db.fp.Close()
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

func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / btree.PageSize
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * btree.PageSize
	if err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize)); err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	db.mmap.file = fileSize
	return nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*btree.PageSize {
		return nil
	}
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += db.mmap.total
	db.mu.Lock()
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	db.mu.Unlock()
	return nil
}

// --- master page ---

func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		db.page.flushed = 1 // page 0 is reserved for the master page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	free := binary.LittleEndian.Uint64(data[32:])
	version := binary.LittleEndian.Uint64(data[40:])

	if !bytes.Equal([]byte(dbSig), data[:len(dbSig)]) {
		return errors.New("bad signature")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/btree.PageSize))
	bad = bad || root >= used
	bad = bad || free >= used
	if bad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.free.Head = free
	db.page.flushed = used
	db.version = version
	return nil
}

func masterStore(db *KV) error {
	var data [48]byte
	copy(data[:16], []byte(dbSig))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.free.Head)
	binary.LittleEndian.PutUint64(data[40:], db.version)
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// --- reader heap ---

// readerList is a min-heap of active read transactions ordered by version.
type readerList []*KVReader

func (h readerList) Len() int            { return len(h) }
func (h readerList) Less(i, j int) bool  { return h[i].version < h[j].version }
func (h readerList) Swap(i, j int)       { h[i].index, h[j].index = j, i; h[i], h[j] = h[j], h[i] }
func (h *readerList) Push(x interface{}) { r := x.(*KVReader); r.index = len(*h); *h = append(*h, r) }
func (h *readerList) Pop() interface{}   { x := (*h)[len(*h)-1]; *h = (*h)[:len(*h)-1]; return x }

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}
