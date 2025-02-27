package btree

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
const METAPAGE_SIZE = 32
const INITIAL_MMAP_SIZE = 64 << 20 // 64MB

// bnode = page
// chuck = collection of pages
// chunk = portion of the db mapped in memory

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree

	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}

	mmap struct {
		file_size int      // file size, can be larger than the database size
		mmap_size int      // mmap size, can be larger than the file size
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
	// open or create the DB file
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

	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	err = loadMetapage(db)
	if err != nil {
		db.Close()
		return fmt.Errorf("KV.Open: %w", err)
	}

	return nil
}

/*----- BTREE PERSISTANCE -----*/
func (db *KV) pageGet(ptr uint64) BNode {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_MAX_NODE_SIZE
		if ptr < end {
			offset := BTREE_MAX_NODE_SIZE * (ptr - start)
			return BNode(chunk[offset : offset+BTREE_MAX_NODE_SIZE])
		}
		start = end
	}

	panic("bad ptr")
}

func (db *KV) pageNew(node BNode) uint64 {
	// TODO: reuse deallocated pages
	assert(len(node) <= BTREE_MAX_NODE_SIZE, " ")
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func (db *KV) pageDel(uint64) {
	// TODO: implement this
}

// initial mmap covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_MAX_NODE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
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

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:DB_SIG_SIZE]) {
		return errors.New("bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file_size/BTREE_MAX_NODE_SIZE))
	bad = bad || !(root < used)

	if bad {
		return errors.New("bad meta page")
	}

	db.tree.root = root
	db.page.flushed = used
	return nil
}

// atomic metapage update
func storeMetapage(db *KV) error {
	var data [METAPAGE_SIZE]byte
	copy(data[:DB_SIG_SIZE], []byte(DB_SIG))

	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE:], db.tree.root)
	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE+POINTER_SIZE:], db.page.flushed)

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
	// extend the file & mmap if needed
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr), page)
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	if err := storeMetapage(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}
