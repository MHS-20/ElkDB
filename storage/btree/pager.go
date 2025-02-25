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

// chunk = portion of the db mapped in memory
// chuck = collection of pages
// bnode = page

type KV struct {
	Path string
	fp   *os.File
	tree BTree

	page struct {
		flushed uint64   // database size in pages
		temp    [][]byte // newly allocated pages
	}

	mmap struct {
		file   int      // file size
		total  int      // mmap size
		chunks [][]byte // multiple mmaps (non-continuous)
	}
}

/*------- PAGER API ------*/
// read key from DB
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update DB key-val
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

// delete key from DB
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// DELETE DB
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil, "err "+err.Error())
	}
	_ = db.fp.Close()
}

// CREATE DB
func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
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

	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// read the master page
	err = metapageLoad(db)
	if err != nil {
		goto fail
	}

	// done
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

/*------- PAGER to BTREE Interface  ------*/
// retrive page (Bnode) from pointer
func (db *KV) pageGet(pointer uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_MAX_NODE_SIZE
		if pointer < end { // pointer falls in current chunck
			offset := BTREE_MAX_NODE_SIZE * (pointer - start)
			return BNode(chunk[offset : offset+BTREE_MAX_NODE_SIZE]) // ? correct syntax
		}
		start = end
	}
	panic("bad pointer")
}

// Allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
	// TODO: reuse deallocated pages
	assert(node.nbytes() <= BTREE_MAX_NODE_SIZE, "node size exceeds the limit")
	pointer := db.page.flushed + uint64(len(db.page.temp)) //current db size
	db.page.temp = append(db.page.temp, node)
	return pointer
}

// callback for BTree, deallocate a page.
func (db *KV) pageDel(uint64) {
	// TODO: implement deallocation
}

// Initial mmap (covers the whole file)
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_MAX_NODE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := INITIAL_MMAP_SIZE
	assert(mmapSize%BTREE_MAX_NODE_SIZE == 0, "mmap size must be a multiple of page size")
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2 // larger than file
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

/*------- META-PAGE MANAGEMENT --------*/
func metapageLoad(db *KV) error {
	// empty file, first write will create the metapage
	if db.mmap.file == 0 {
		db.page.flushed = 1 // reserved for the metapage
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[DB_SIG_SIZE:])
	used := binary.LittleEndian.Uint64(data[DB_SIG_SIZE+POINTER_SIZE:])

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:DB_SIG_SIZE]) {
		return errors.New("bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_MAX_NODE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("bad meta page")
	}

	db.tree.root = root
	db.page.flushed = used
	return nil
}

// Update the meta-page (atomically)
func metapageStore(db *KV) error {
	var data [METAPAGE_SIZE]byte
	copy(data[:DB_SIG_SIZE], []byte(DB_SIG))

	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE:], db.tree.root)
	binary.LittleEndian.PutUint64(data[DB_SIG_SIZE+POINTER_SIZE:], db.page.flushed)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// Extend file exponentially
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_MAX_NODE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		filePages *= 2

		// inc := filePages / 8
		// if inc < 1 {
		// 	inc = 1
		// }
		// filePages += inc
	}

	fileSize := filePages * BTREE_MAX_NODE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

// Add new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_MAX_NODE_SIZE {
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
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// Persist the newly allocated pages
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

// write & extend
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
		pointer := db.page.flushed + uint64(i)
		copy(db.pageGet(pointer), page)
	}
	return nil
}

// Flush data to disk
func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	// update & flush the meta page
	if err := metapageStore(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}
