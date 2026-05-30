package kv

import "github.com/MHS-20/ElkDB/btree"

// Reader is the read-only surface of a KV transaction.
// Both *KVReader and *KVTX satisfy this interface.
// The table package depends only on this interface, never on the concrete types.
type Reader interface {
	// Get returns the value stored under key, or (nil, false) if absent.
	Get(key []byte) ([]byte, bool)
	// Seek positions a B-tree iterator at the key nearest to key satisfying cmp.
	// cmp must be one of btree.CmpGE, CmpGT, CmpLT, CmpLE.
	Seek(key []byte, cmp int) *btree.BIter
}

// Writer is the read-write surface of a KV transaction.
// Only *KVTX satisfies this interface.
type Writer interface {
	Reader
	// Update inserts, updates, or conditionally inserts a key depending on
	// req.Mode. Returns true if a new key was created.
	Update(req *btree.InsertReq) bool
	// Del deletes a key. Returns true if the key existed.
	Del(req *btree.DeleteReq) bool
}
