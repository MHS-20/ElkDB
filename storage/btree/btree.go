package btree

import (
	"bytes"
	"encoding/binary"
)

type BTree struct {
	root uint64 // pointer
	// managing on-disk pages
	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}

	return nodeGetKey(tree, tree.get(tree.root), key)
}

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BTREE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BTREE_NODE:
		return nodeGetKey(tree, tree.get(node.getPointer(idx)), key)
	default:
		panic("bad node!")
	}
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
// Insert a new key into a Leaf Node
func leafInsert(newNode BNode, oldNode BNode, idx uint16, key []byte, val []byte) {
	newNode.setHeader(BTREE_LEAF, oldNode.nkeys()+1) // setup the header
	// copy existing node keys (oldNode[0:idx] -> newNode[0:idx]
	nodeAppendRange(newNode, oldNode, 0, 0, idx)

	// insert the new key
	nodeAppendKV(newNode, idx, 0, key, val)

	// copy the rest of the keys (oldNode[idx:end] -> newNode[idx+1:end+1])
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nkeys()-idx)
}

// Update KV in a Leaf Node
func leafUpdate(newNode BNode, oldNode BNode, idx uint16, key []byte, val []byte) {
	newNode.setHeader(BTREE_LEAF, oldNode.nkeys())
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nkeys()-(idx+1))
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

	for i := uint16(1); i <= n; i++ {
		// relative distance in the old node
		offset := dstBegin + oldNode.getOffset(srcOld+i) - srcBegin
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

	// optimizing common case
	if inc == 1 && bytes.Equal(childs[0].getKey(0), oldNode.getKey(idx)) {
		copy(newNode, oldNode[:oldNode.nbytes()])
		newNode.setPointer(idx, tree.new(childs[0]))
		return
	}

	// general case, less efficient
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
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2, "nodeSplit2: nkeys < 2")

	// initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_MAX_NODE_SIZE {
		nleft--
	}

	assert(nleft >= 1, "nodeSplit2: nleft < 1")

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}

	for right_bytes() > BTREE_MAX_NODE_SIZE {
		nleft++
	}

	assert(nleft < old.nkeys(), "nodeSplit2: nleft >= nkeys")
	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// the left half may be still too big
	assert(right.nbytes() <= BTREE_MAX_NODE_SIZE, "nodeSplit2: right node is too large")
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
	} else {
		// the left node is still too large
		leftleft := make(BNode, BTREE_MAX_NODE_SIZE)
		middle := make(BNode, BTREE_MAX_NODE_SIZE)
		nodeSplit2(leftleft, middle, left)
		assert(leftleft.nbytes() <= BTREE_MAX_NODE_SIZE, "leftleft is too large")
		return 3, [3]BNode{leftleft, middle, right}
	}
}

/*---- BTREE INSERTION ----*/
// modes of the updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type InsertReq struct {
	tree *BTree
	// out
	Added bool // added a new key
	// in
	Key  []byte
	Val  []byte
	Mode int
}

// insert a KV into a node,the caller is responsible for:
// deallocating the input node, splitting and allocating result nodes.
func treeInsert(req *InsertReq, node BNode) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := make(BNode, 2*BTREE_MAX_NODE_SIZE)
	// where to insert the key?
	idx := nodeLookupLE(node, req.Key)
	// act depending on the node type
	switch node.btype() {
	case BTREE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(req.Key, node.getKey(idx)) {
			// found the key, update it.
			if req.Mode == MODE_INSERT_ONLY {
				return BNode{}
			}
			if bytes.Equal(req.Val, node.getVal(idx)) {
				return BNode{}
			}
			leafUpdate(new, node, idx, req.Key, req.Val)
		} else {
			// insert it after the position.
			if req.Mode == MODE_UPDATE_ONLY {
				return BNode{}
			}
			leafInsert(new, node, idx+1, req.Key, req.Val)
			req.Added = true
		}
		return new
	case BTREE_NODE:
		// internal node, insert it to a kid node.
		return nodeInsert(req, new, node, idx)
	default:
		panic("bad node!")
	}
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(req *InsertReq, new BNode, node BNode, idx uint16) BNode {
	// recursive insertion to the kid node
	kptr := node.getPointer(idx)
	updated := treeInsert(req, req.tree.get(kptr))
	if len(updated) == 0 {
		return BNode{}
	}
	// deallocate the kid node
	req.tree.del(kptr)
	// split the result
	nsplit, splited := nodeSplit3(updated)
	// update the kid links
	nodeReplaceNchild(req.tree, new, node, idx, splited[:nsplit]...)
	return new
}

/*--- BTREE KV-STORE INTERFACE ---*/
func (tree *BTree) Insert(key []byte, val []byte) bool {
	req := &InsertReq{Key: key, Val: val}
	tree.InsertImpl(req)
	return req.Added
}

func (tree *BTree) InsertImpl(req *InsertReq) {
	assert(len(req.Key) != 0, "inserting empty key")
	assert(len(req.Key) <= BTREE_MAX_KEY_SIZE, "key size exceeds BTREE_MAX_KEY_SIZE")
	assert(len(req.Val) <= BTREE_MAX_VAL_SIZE, "val size exceeds BTREE_MAX_VAL_SIZE")

	if tree.root == 0 {
		// create the first node
		root := make(BNode, BTREE_MAX_NODE_SIZE)
		root.setHeader(BTREE_LEAF, n_keys)

		// dummy key ensure the tree has always a key
		nodeAppendKV(root, 0, 0, nil, nil)

		// add actual key-value
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		return
	}

	req.tree = tree
	node := treeInsert(req, tree.get(tree.root))

	if len(node) == 0 {
		return
	}

	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)

	if nsplit > 1 {
		// the root has been splitted, add a new level
		root := make(BNode, BTREE_MAX_NODE_SIZE)
		root.setHeader(BTREE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0) // add first key to maintain search structure
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

/*--- BTREE MERGING & DELETION ---*/
// should the updated child be merged with a sibling?
// left (-1)
// right (+1)
// 0 no merge required
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_MAX_NODE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 { // a left sibling exists
		sibling := BNode(tree.get(node.getPointer(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_MAX_NODE_SIZE {
			return -1, sibling // merge with left sibling
		}
	}

	if idx+1 < node.nkeys() { // a right sibling exists
		sibling := BNode(tree.get(node.getPointer(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_MAX_NODE_SIZE {
			return +1, sibling // merge with right sibling
		}
	}
	return 0, BNode{}
}

// merge 2 nodes into 1
func nodeMerge(newNode BNode, left BNode, right BNode) {
	newNode.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(newNode, left, 0, 0, left.nkeys())
	nodeAppendRange(newNode, right, left.nkeys(), 0, right.nkeys())
	assert(newNode.nbytes() <= BTREE_MAX_NODE_SIZE, "nodeMerge: merged node is too large")

}

// remove a key from a leaf node
func leafDelete(newNode BNode, oldNode BNode, idx uint16) {
	newNode.setHeader(BTREE_LEAF, oldNode.nkeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendRange(newNode, oldNode, idx, idx+1, oldNode.nkeys()-(idx+1))
}

// replace 2 adjacent links with 1
func nodeReplace2Child(newNode BNode, oldNode BNode, idx uint16, ptr uint64, key []byte) {
	newNode.setHeader(oldNode.btype(), oldNode.nkeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, ptr, key, nil)
	nodeAppendRange(newNode, oldNode, idx+1, idx+2, oldNode.nkeys()-(idx+2))
	// idx+2 to skip the key we want to remove
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BTREE_LEAF:
		if idx < node.nkeys() && bytes.Equal(key, node.getKey(idx)) {
			newNode := make(BNode, BTREE_MAX_NODE_SIZE)
			leafDelete(newNode, node, idx)
			return newNode
		}
		return BNode{} // not found
	case BTREE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

// delete a key from an internal node
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse to the child node
	child_pointer := node.getPointer(idx)
	updated := treeDelete(tree, tree.get(child_pointer), key)

	if len(updated) == 0 {
		return BNode{} // not found
	}

	tree.del(child_pointer)
	new := make(BNode, BTREE_MAX_NODE_SIZE)

	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := make(BNode, BTREE_MAX_NODE_SIZE)
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPointer(idx - 1))
		nodeReplace2Child(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := make(BNode, BTREE_MAX_NODE_SIZE)
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPointer(idx + 1))
		nodeReplace2Child(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0, "one empty child but no sibling")
		new.setHeader(BTREE_NODE, 0) // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceNchild(tree, new, node, idx, updated)
	}
	return new
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0, "deleting empty key")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, "deleting overflowing key")
	if tree.root == 0 {
		return false
	}

	node := treeDelete(tree, tree.get(tree.root), key)
	if len(node) == 0 {
		return false
	}

	tree.del(tree.root)
	if node.btype() == BTREE_NODE && node.nkeys() == 1 {
		// remove a level
		tree.root = node.getPointer(0)
	} else {
		tree.root = tree.new(node)
	}
	return true

	// if node.nkeys() == 0 {
	// 	tree.root = 0
	// } else {
	// 	tree.root = tree.new(node)
	// }
	// return true
}
