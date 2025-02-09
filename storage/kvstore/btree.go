package kvstore

import (
	"bytes"
	"encoding/binary"
)

type BTree struct {
	root uint64
	// managing on-disk pages
	get func(uint64) []byte
	new func([]byte) uint64
	del func(uint64)
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BTREE_LEAF, old.nkeys()+1) // setup the header
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// skip the first key since it's a copy from the parent
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// copy a KV into its position
func nodeAppendKV(newNode BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	newNode.setPointer(idx, ptr)
	loc := newNode.kvLocation(idx)

	// write values' lentghts
	binary.LittleEndian.PutUint16(newNode[loc:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newNode[loc+KEY_LENGTH_SIZE:], uint16(len(val)))

	// write actual values
	key_value_offset := uint16(KEY_LENGTH_SIZE + VAL_LENGTH_SIZE)
	val_value_offset := uint16(KEY_LENGTH_SIZE + VAL_LENGTH_SIZE + len(key))

	copy(newNode[loc+key_value_offset:], key)
	copy(newNode[loc+val_value_offset:], val)
	newNode.setOffset(idx+1, newNode.getOffset(idx)+KEY_LENGTH_SIZE+VAL_LENGTH_SIZE+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position from the old node
func nodeAppendRange(newNode BNode, oldNode BNode, dstNew uint16, srcOld uint16, n uint16) {
	assert(srcOld+n <= oldNode.nkeys(), "srcOld+n out of range")
	assert(dstNew+n <= newNode.nkeys(), "dstNew+n out of range")

	if n == 0 {
		return
	}

	// update all pointers
	for i := uint16(0); i < n; i++ {
		newNode.setPointer(dstNew+i, oldNode.getPointer(srcOld+i))
	}

	// update all offsets
	dstBegin := newNode.getOffset(dstNew)
	srcBegin := oldNode.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // ! NOTE: the range is [1, n]
		offset := dstBegin + oldNode.getOffset(srcOld+i) - srcBegin
		newNode.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := oldNode.kvLocation(srcOld)
	end := oldNode.kvLocation(srcOld + n)
	copy(newNode[newNode.kvLocation(dstNew):], oldNode[begin:end])
}
