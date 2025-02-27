package btree

import "encoding/binary"

type FreeList struct {
	head uint64
	get  func(uint64) BNode  // dereference a pointer
	new  func(BNode) uint64  // append a new page
	use  func(uint64, BNode) // reuse a page
}

const BNODE_FREE_LIST = 3
const TYPE_LENGTH = 2
const SIZE_LENGTH = 2
const TOTAL_LENGTH = 8
const NEXT_LENGTH = 8
const FREE_LIST_HEADER = TYPE_LENGTH + SIZE_LENGTH + TOTAL_LENGTH + NEXT_LENGTH
const FREE_LIST_CAP = (BTREE_MAX_NODE_SIZE - FREE_LIST_HEADER) / 8

func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}
	node := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(node[HEADER:]))
}

func freeListNodeSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node[TYPE_LENGTH:]))
}

func freeListNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node[FREE_LIST_HEADER:])
}

func freelistNodePointer(node BNode, idx int) uint64 {
	offset := FREE_LIST_HEADER + POINTER_SIZE*idx
	return binary.LittleEndian.Uint64(node[offset:])
}

func freeListNodeSetPointer(node BNode, idx int, pointer uint64) {
	assert(idx < freeListNodeSize(node), "idx out of range")
	offset := FREE_LIST_HEADER + POINTER_SIZE*idx
	binary.LittleEndian.PutUint64(node[offset:], pointer)
}

func freeListNodeSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node[0:], BNODE_FREE_LIST)
	binary.LittleEndian.PutUint16(node[TYPE_LENGTH:], size)
	binary.LittleEndian.PutUint64(node[TYPE_LENGTH+SIZE_LENGTH:], 0)
	binary.LittleEndian.PutUint64(node[FREE_LIST_HEADER:], next)
}

func freeListNodeSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node[TYPE_LENGTH+SIZE_LENGTH:], total)
}
