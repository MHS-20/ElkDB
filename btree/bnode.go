package btree

import (
	"bytes"
	"encoding/binary"
)

const headerSize = 4

const (
	PageSize   = 4096
	MaxKeySize = 1000
	MaxValSize = 3000
)

func init() {
	node1max := headerSize + 8 + 2 + 4 + MaxKeySize + MaxValSize
	assert(node1max <= PageSize)
}

const (
	BNodeInternal = 1 // internal nodes without values
	BNodeLeaf     = 2 // leaf nodes with values
)

// BNode is a B-tree page stored as a flat byte slice.
// The same slice is what gets written to / read from disk.
type BNode struct {
	Data []byte // exported so kv can copy it into mmap pages
}

// --- header ---

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.Data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.Data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.Data[0:2], btype)
	binary.LittleEndian.PutUint16(node.Data[2:4], nkeys)
}

// --- pointers ---

func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := headerSize + 8*idx
	return binary.LittleEndian.Uint64(node.Data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := headerSize + 8*idx
	binary.LittleEndian.PutUint64(node.Data[pos:], val)
}

// --- offset list ---

func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return headerSize + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.Data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.Data[offsetPos(node, idx):], offset)
}

// --- key-values ---

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return headerSize + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos:])
	return node.Data[pos+4:][:klen:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.Data[pos+2:])
	return node.Data[pos+4+klen:][:vlen:vlen]
}

// nbytes is the used size of the node in bytes.
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// --- lookup ---

// nodeLookupLE returns the last index i where node.getKey(i) <= key.
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	lo, hi := uint16(1), nkeys-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		cmp := bytes.Compare(node.getKey(mid), key)
		if cmp <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo < nkeys && bytes.Compare(node.getKey(lo), key) <= 0 {
		return lo
	}
	return 0
}

// --- mutation helpers ---

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)
	pos := new.kvPos(idx)

	binary.LittleEndian.PutUint16(new.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.Data[pos+2:], uint16(len(val)))

	copy(new.Data[pos+4:], key)
	copy(new.Data[pos+4+uint16(len(key)):], val)

	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	assert(srcOld+n <= old.nkeys())
	assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}

	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.Data[new.kvPos(dstNew):], old.Data[begin:end])
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNodeLeaf, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNodeLeaf, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNodeLeaf, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
	assert(new.nbytes() <= PageSize)
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	nleft := old.nkeys() / 2

	leftBytes := func() uint16 {
		return headerSize + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for leftBytes() > PageSize {
		nleft--
	}
	assert(nleft >= 1)

	rightBytes := func() uint16 {
		return old.nbytes() - leftBytes() + headerSize
	}
	for rightBytes() > PageSize {
		nleft++
	}

	assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)

	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	assert(right.nbytes() <= PageSize)
}

// nodeSplit3 splits a node into 1-3 nodes if it exceeds PageSize.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= PageSize {
		old.Data = old.Data[:PageSize]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*PageSize)}
	right := BNode{make([]byte, PageSize)}

	nodeSplit2(left, right, old)
	if left.nbytes() <= PageSize {
		left.Data = left.Data[:PageSize]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode{make([]byte, PageSize)}
	middle := BNode{make([]byte, PageSize)}

	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= PageSize)

	return 3, [3]BNode{leftleft, middle, right}
}

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}
