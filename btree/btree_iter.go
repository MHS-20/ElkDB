package btree

import "bytes"

// BIter is a cursor over a BTree.
// It holds a path from the root down to a leaf, plus an index at each level.
type BIter struct {
	tree *BTree
	path []BNode  // nodes from root to current leaf
	pos  []uint16 // index into each node along the path
}

// Comparison modes for Seek.
const (
	CmpGE = +3 // >=
	CmpGT = +2 // >
	CmpLT = -2 // <
	CmpLE = -3 // <=
)

// Clone returns a deep copy of the iterator.
func (iter *BIter) Clone() *BIter {
	return &BIter{
		tree: iter.tree,
		path: append([]BNode(nil), iter.path...),
		pos:  append([]uint16(nil), iter.pos...),
	}
}

// Deref returns the key and value at the current position.
func (iter *BIter) Deref() ([]byte, []byte) {
	assert(iter.Valid())
	return iterDeref(iter)
}

func iterDeref(iter *BIter) ([]byte, []byte) {
	last := len(iter.path) - 1
	node := iter.path[last]
	pos := iter.pos[last]
	return node.getKey(pos), node.getVal(pos)
}

// Valid reports whether the iterator points to a real (non-dummy) key.
func (iter *BIter) Valid() bool {
	return !iterDummy(iter) && iterInRange(iter)
}

// iterDummy returns true when every position is zero, which means the iterator
// is sitting on the synthetic sentinel key at the start of the tree.
func iterDummy(iter *BIter) bool {
	for _, pos := range iter.pos {
		if pos != 0 {
			return false
		}
	}
	return true
}

func iterInRange(iter *BIter) bool {
	last := len(iter.path) - 1
	node := iter.path[last]
	return iter.pos[last] < node.nkeys()
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]--
	} else if level > 0 {
		iterPrev(iter, level-1) // move to a sibling node
	} else {
		return // already at the dummy key
	}

	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := iter.tree.Store.PageGet(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++
	} else if level > 0 {
		iterNext(iter, level-1)
	} else {
		iter.pos[len(iter.pos)-1]++ // step past the last key
		return
	}

	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := iter.tree.Store.PageGet(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

// Prev moves the iterator one step backward.
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}

// Next moves the iterator one step forward.
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

// SeekLE positions the iterator at the largest key <= the given key.
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.Root; ptr != 0; {
		node := tree.Store.PageGet(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.btype() == BNodeInternal {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// CmpOK reports whether the comparison "key cmp ref" holds.
func CmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CmpGE:
		return r >= 0
	case CmpGT:
		return r > 0
	case CmpLT:
		return r < 0
	case CmpLE:
		return r <= 0
	default:
		panic("unknown cmp value")
	}
}

// Seek positions the iterator at the key nearest to key satisfying cmp.
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := &BIter{tree: tree}
	if tree.Root == 0 {
		return iter
	}

	iter = tree.SeekLE(key)
	if cmp != CmpLE && iterInRange(iter) {
		cur, _ := iterDeref(iter)
		if !CmpOK(cur, cmp, key) {
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	if iter.Valid() {
		cur, _ := iter.Deref()
		assert(CmpOK(cur, cmp, key))
	}
	return iter
}
