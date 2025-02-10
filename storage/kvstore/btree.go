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

// search key
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

/*---- BTREE UPDATES -----*/

// Update a Leaf Node (add a new key to a leaf node)
func leafInsert(newNode BNode, oldNode BNode, idx uint16, key []byte, val []byte) {
	newNode.setHeader(BTREE_LEAF, oldNode.nkeys()+1) // setup the header

	// copy existing node keys (oldNode[0:idx] -> newNode[0:idx]
	nodeAppendRange(newNode, oldNode, 0, 0, idx)

	// insert the new key
	nodeAppendKV(newNode, idx, 0, key, val)

	// copy the rest of the keys (oldNode[idx:end] -> newNode[idx+1:end+1])
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nkeys()-idx)
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

	// shift all pointers
	for i := uint16(0); i < n; i++ {
		newNode.setPointer(dstNew+i, oldNode.getPointer(srcOld+i))
	}

	// update all offsets
	dstBegin := newNode.getOffset(dstNew)
	srcBegin := oldNode.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // the range is [1, n]
		// relative distance in the old node
		offset := dstBegin + (oldNode.getOffset(srcOld+i) - srcBegin)
		newNode.setOffset(dstNew+i, offset)
	}

	// copy KVs in interval [srcOld, srcOld+n)
	begin := oldNode.kvLocation(srcOld)
	end := oldNode.kvLocation(srcOld + n)
	copy(newNode[newNode.kvLocation(dstNew):], oldNode[begin:end])
}

// Update an Internal Node (replace a link with multiple links)
func nodeReplaceNchild(tree *BTree, newNode BNode, oldNode BNode, idx uint16, childs ...BNode) {
	inc := uint16(len(childs))
	newNode.setHeader(BTREE_NODE, oldNode.nkeys()+inc-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)

	// create&add nodes for each child
	for i, node := range childs {
		nodeAppendKV(newNode, idx+uint16(i), tree.new(node), node.getKey(0), nil)
		// first key is used as separator
	}

	// copy the rest of the keys (oldNode[idx:end] -> newNode[idx+inc:end+inc])
	nodeAppendRange(newNode, oldNode, idx+inc, idx+1, oldNode.nkeys()-(idx+1))
}

/*---- BTREE SPLITS -----*/
// ! just in memory split, save to disk
func nodeSplit2(left BNode, right BNode, old BNode) {
	mid := old.nkeys() / 2

	left.setHeader(old.btype(), mid)
	nodeAppendRange(left, old, 0, 0, mid)

	right.setHeader(old.btype(), old.nkeys()-mid)
	nodeAppendRange(right, old, 0, mid, old.nkeys()-mid)
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_MAX_NODE_SIZE {
		old = old[:BTREE_MAX_NODE_SIZE]
		return 1, [3]BNode{old}
	}

	left := make(BNode, 2*BTREE_MAX_NODE_SIZE) // might be split later
	right := make(BNode, BTREE_MAX_NODE_SIZE)
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_MAX_NODE_SIZE {
		left = left[:BTREE_MAX_NODE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large
	leftleft := make(BNode, BTREE_MAX_NODE_SIZE)
	middle := make(BNode, BTREE_MAX_NODE_SIZE)
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_MAX_NODE_SIZE, "leftleft is too large")
	return 3, [3]BNode{leftleft, middle, right}
}

/*---- BTREE INSERTION ----*/
