package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const DB_SIG = "ELKDB"
const DB_SIG_SIZE = 16
const METAPAGE_SIZE = 40
const INITIAL_MMAP_SIZE = 64 << 20 // 64MB

// bnode = page
// chuck = collection of pages
// chunk = portion of the db mapped in memory

type KV struct {
	Path string
	fp   *os.File
	tree BTree
	free FreeList

	page struct {
		flushed  uint64 // db size in pages
		n_free   int    // pages in freelist
		n_append int    // pages to  appended
		updates  map[uint64][]byte
	}

	mmap struct {
		file_size int
		mmap_size int
		chunks    [][]byte // multiple mmaps, can be non-continuous
	}
}

/*----- PAGER API -----*/
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error) {
	req := &InsertReq{Key: key, Val: val, Mode: mode}
	db.tree.InsertImpl(req)
	return req.Added, flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil, " ")
	}
	_ = db.fp.Close()
}

func (db *KV) Open() error {
	db.page.updates = map[uint64][]byte{}

	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// free list callbacks
	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

	// open DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the initial mmap
	size, chunk, err := mmapInit(db.fp)
	if err != nil {
		db.Close()
		return fmt.Errorf("KV.Open: %w", err)
	}

	db.mmap.file_size = size
	db.mmap.mmap_size = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	err = loadMetapage(db)
	if err != nil {
		db.Close()
		return fmt.Errorf("KV.Open: %w", err)
	}

	return nil
}

/*----- BTREE PERSISTANCE -----*/
func (db *KV) pageDel(pointer uint64) {
	db.page.updates[pointer] = nil
}

func (db *KV) pageUse(pointer uint64, node BNode) {
	db.page.updates[pointer] = node
}

func (db *KV) pageGet(pointer uint64) BNode {
	if page, ok := db.page.updates[pointer]; ok {
		assert(page != nil, "page is null")
		return BNode(page) // new pages
	}
	return db.pageGetMapped(pointer) // retrive pages
}

func (db *KV) pageGetMapped(pointer uint64) BNode {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_MAX_NODE_SIZE
		if pointer < end {
			offset := BTREE_MAX_NODE_SIZE * (pointer - start)
			return BNode(chunk[offset : offset+BTREE_MAX_NODE_SIZE])
		}
		start = end
	}
	panic("bad pointer")
}

func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node) <= BTREE_MAX_NODE_SIZE, "node too large")
	pointer := uint64(0)

	if db.page.n_free < db.free.ListLen() {
		// reuse a page
		pointer = db.free.Get(db.page.n_free)
		db.page.n_free++
	} else {
		// new page
		pointer = db.page.flushed + uint64(db.page.n_append)
		db.page.n_append++
	}

	db.page.updates[pointer] = node
	return pointer
}

func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node) <= BTREE_MAX_NODE_SIZE, "node too large")
	pointer := db.page.flushed + uint64(db.page.n_append)
	db.page.n_append++
	db.page.updates[pointer] = node
	return pointer
}

// initial mmap covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_MAX_NODE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of node(page) size")
	}

	mmapSize := INITIAL_MMAP_SIZE
	assert(mmapSize%BTREE_MAX_NODE_SIZE == 0, "")
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

/*----- METAPAGE MANAGEMENT ------*/
func loadMetapage(db *KV) error {
	if db.mmap.file_size == 0 {
		db.page.flushed = 1 // metapage reserved
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[DB_SIG_SIZE:])
	used := binary.LittleEndian.Uint64(data[DB_SIG_SIZE+POINTER_SIZE:])
	free := binary.LittleEndian.Uint64(data[DB_SIG_SIZE+POINTER_SIZE+POINTER_SIZE:])

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:DB_SIG_SIZE]) {
		return errors.New("bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file_size/BTREE_MAX_NODE_SIZE))
	bad = bad || !(root < used)
	bad = bad || !(free < used)

	if bad {
		return errors.New("bad meta page")
	}

	db.tree.root = root
	db.free.head = free
	db.page.flushed = used
	return nil
}

// atomic metapage update
func storeMetapage(db *KV) error {
	var data [METAPAGE_SIZE]byte
	copy(data[:DB_SIG_SIZE], []byte(DB_SIG))

	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE:], db.tree.root)
	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE+POINTER_SIZE:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE+POINTER_SIZE+POINTER_SIZE:], db.free.head)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}

	return nil
}

/*------- EXTENSION MANAGEMENT -----*/
// extend the file to at least npages
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file_size / BTREE_MAX_NODE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		inc := max(filePages/8, 1)
		filePages += inc
	}

	fileSize := filePages * BTREE_MAX_NODE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file_size = fileSize
	return nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.mmap_size >= npages*BTREE_MAX_NODE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.mmap_size), db.mmap.mmap_size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.mmap_size += db.mmap.mmap_size
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

/*------ PAGE PERSISTANCE ----*/
// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for pointer, page := range db.page.updates {
		if page == nil {
			freed = append(freed, pointer)
		}
	}
	db.free.Update(db.page.n_free, freed)

	// extend file & mmap
	npages := int(db.page.flushed) + db.page.n_append
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy pages to file
	for pointer, page := range db.page.updates {
		if page != nil {
			copy(db.pageGetMapped(pointer), page)
		}
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(db.page.n_append)
	db.page.n_free = 0
	db.page.n_append = 0
	db.page.updates = map[uint64][]byte{}

	if err := storeMetapage(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}
