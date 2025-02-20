package kvstore

import (
	. "elkdb/storage/btree"
	"os"
	"syscall"
)

// Assert panics with the provided message if the condition is false.
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

const DB_SIG = "ELKDB"

type KV struct {
	Path string
	fp   *os.File
	tree BTree

	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}

	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil, "err "+err.Error())
	}
	_ = db.fp.Close()
}
