package btree

import "bytes"

// BTree is a copy-on-write B-tree.
// All page I/O is delegated to a PageStore, so this type contains no disk
// or mmap logic.
type BTree struct {
	Root  uint64    // page number of the root node (0 = empty tree)
	Store PageStore // injected by kv when a transaction begins
}

// --- internal node helpers ---

func nodeReplaceKid1ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	copy(new.Data, old.Data[:old.nbytes()])
	new.setPtr(idx, ptr)
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	if inc == 1 && bytes.Equal(kids[0].getKey(0), old.getKey(idx)) {
		nodeReplaceKid1ptr(new, old, idx, tree.Store.PageNew(kids[0]))
		return
	}

	new.setHeader(BNodeInternal, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.Store.PageNew(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNodeInternal, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// --- insert ---

// Modes for Insert / InsertEx.
const (
	ModeUpsert     = 0 // insert or replace
	ModeUpdateOnly = 1 // update existing keys only
	ModeInsertOnly = 2 // insert new keys only
)

// InsertReq carries both the input parameters and the output results of an
// insert operation, mirroring the original design so callers in the kv/table
// layers do not need to change their call sites.
type InsertReq struct {
	// inputs
	Key  []byte
	Val  []byte
	Mode int
	// outputs
	Added   bool   // true if a new key was created
	Updated bool   // true if the tree was modified (insert or value change)
	Old     []byte // previous value, set when an existing key was replaced
}

func treeInsert(tree *BTree, req *InsertReq, node BNode) BNode {
	new := BNode{Data: make([]byte, 2*PageSize)}

	idx := nodeLookupLE(node, req.Key)
	switch node.btype() {
	case BNodeLeaf:
		if bytes.Equal(req.Key, node.getKey(idx)) {
			if req.Mode == ModeInsertOnly {
				return BNode{}
			}
			old := node.getVal(idx)
			if bytes.Equal(req.Val, old) {
				return BNode{}
			}
			leafUpdate(new, node, idx, req.Key, req.Val)
			req.Updated = true
			req.Old = old
		} else {
			if req.Mode == ModeUpdateOnly {
				return BNode{}
			}
			leafInsert(new, node, idx+1, req.Key, req.Val)
			req.Updated = true
			req.Added = true
		}
		return new
	case BNodeInternal:
		return nodeInsert(tree, req, new, node, idx)
	default:
		panic("bad node!")
	}
}

func nodeInsert(tree *BTree, req *InsertReq, new BNode, node BNode, idx uint16) BNode {
	kptr := node.getPtr(idx)
	updated := treeInsert(tree, req, tree.Store.PageGet(kptr))
	if len(updated.Data) == 0 {
		return BNode{}
	}
	tree.Store.PageDel(kptr)
	nsplit, split := nodeSplit3(updated)
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	return new
}

// Insert inserts or replaces key/val. Returns true if a new key was added.
func (tree *BTree) Insert(key []byte, val []byte) bool {
	req := &InsertReq{Key: key, Val: val}
	tree.InsertEx(req)
	return req.Added
}

// InsertEx is the full insert path, writing results into req.
func (tree *BTree) InsertEx(req *InsertReq) {
	assert(len(req.Key) != 0)
	assert(len(req.Key) <= MaxKeySize)
	assert(len(req.Val) <= MaxValSize)

	if tree.Root == 0 {
		root := BNode{Data: make([]byte, PageSize)}
		root.setHeader(BNodeLeaf, 2)
		nodeAppendKV(root, 0, 0, nil, nil) // dummy sentinel key
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.Root = tree.Store.PageNew(root)
		req.Added = true
		return
	}

	updated := treeInsert(tree, req, tree.Store.PageGet(tree.Root))
	if len(updated.Data) == 0 {
		return
	}

	tree.Store.PageDel(tree.Root)
	nsplit, split := nodeSplit3(updated)
	if nsplit > 1 {
		root := BNode{Data: make([]byte, PageSize)}
		root.setHeader(BNodeInternal, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.Store.PageNew(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.Root = tree.Store.PageNew(root)
	} else {
		tree.Root = tree.Store.PageNew(split[0])
	}
}

// --- delete ---

// DeleteReq carries both the input key and the output old value.
type DeleteReq struct {
	// input
	Key []byte
	// output
	Old []byte // value that was deleted
}

func treeDelete(tree *BTree, req *DeleteReq, node BNode) BNode {
	idx := nodeLookupLE(node, req.Key)
	switch node.btype() {
	case BNodeLeaf:
		if !bytes.Equal(req.Key, node.getKey(idx)) {
			return BNode{}
		}
		req.Old = node.getVal(idx)
		new := BNode{Data: make([]byte, PageSize)}
		leafDelete(new, node, idx)
		return new
	case BNodeInternal:
		return nodeDelete(tree, req, node, idx)
	default:
		panic("bad node!")
	}
}

func nodeDelete(tree *BTree, req *DeleteReq, node BNode, idx uint16) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, req, tree.Store.PageGet(kptr))
	if len(updated.Data) == 0 {
		return BNode{}
	}
	tree.Store.PageDel(kptr)

	new := BNode{Data: make([]byte, PageSize)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // merge with left sibling
		merged := BNode{Data: make([]byte, PageSize)}
		nodeMerge(merged, sibling, updated)
		tree.Store.PageDel(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.Store.PageNew(merged), merged.getKey(0))
	case mergeDir > 0: // merge with right sibling
		merged := BNode{Data: make([]byte, PageSize)}
		nodeMerge(merged, updated, sibling)
		tree.Store.PageDel(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.Store.PageNew(merged), merged.getKey(0))
	case mergeDir == 0:
		assert(updated.nkeys() > 0)
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > PageSize/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.Store.PageGet(node.getPtr(idx - 1))
		if sibling.nbytes()+updated.nbytes()-headerSize <= PageSize {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.Store.PageGet(node.getPtr(idx + 1))
		if sibling.nbytes()+updated.nbytes()-headerSize <= PageSize {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

// Delete removes key from the tree. Returns true if the key existed.
func (tree *BTree) Delete(key []byte) bool {
	return tree.DeleteEx(&DeleteReq{Key: key})
}

// DeleteEx is the full delete path, writing the old value into req.
func (tree *BTree) DeleteEx(req *DeleteReq) bool {
	assert(len(req.Key) != 0)
	assert(len(req.Key) <= MaxKeySize)
	if tree.Root == 0 {
		return false
	}

	updated := treeDelete(tree, req, tree.Store.PageGet(tree.Root))
	if len(updated.Data) == 0 {
		return false
	}

	tree.Store.PageDel(tree.Root)
	if updated.btype() == BNodeInternal && updated.nkeys() == 1 {
		tree.Root = updated.getPtr(0) // collapse one level
	} else {
		tree.Root = tree.Store.PageNew(updated)
	}
	return true
}

// --- get ---

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		}
		return nil, false
	case BNodeInternal:
		return nodeGetKey(tree, tree.Store.PageGet(node.getPtr(idx)), key)
	default:
		panic("bad node!")
	}
}

// Get returns the value for key, or (nil, false) if not found.
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.Root == 0 {
		return nil, false
	}
	return nodeGetKey(tree, tree.Store.PageGet(tree.Root), key)
}
