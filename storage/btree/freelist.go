package btree

import "encoding/binary"

type FreeList struct {
	head uint64
	get  func(uint64) BNode
	new  func(BNode) uint64
	use  func(uint64, BNode)
}

const BNODE_FREE_LIST = 3
const TYPE_LENGTH = 2
const SIZE_LENGTH = 2
const TOTAL_LENGTH = 8
const NEXT_LENGTH = 8
const FREE_LIST_HEADER = TYPE_LENGTH + SIZE_LENGTH + TOTAL_LENGTH + NEXT_LENGTH
const FREE_LIST_CAP = (BTREE_MAX_NODE_SIZE - FREE_LIST_HEADER) / POINTER_SIZE

func (fl *FreeList) ListLen() int {
	if fl.head == 0 {
		return 0
	}
	node := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(node[TYPE_LENGTH+SIZE_LENGTH:]))
}

func freeListNodeSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node[TYPE_LENGTH:]))
}

func freeListNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node[TYPE_LENGTH+SIZE_LENGTH+TOTAL_LENGTH:])
}

func freelistNodeGetPointer(node BNode, idx int) uint64 {
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
	binary.LittleEndian.PutUint64(node[TYPE_LENGTH+SIZE_LENGTH+TOTAL_LENGTH:], next)
}

func freeListNodeSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node[TYPE_LENGTH+SIZE_LENGTH:], total)
}

// get a page from the free list
func (fl *FreeList) Get(topn int) uint64 {
	assert(0 <= topn && topn < fl.ListLen(), "topn out of range")
	node := fl.get(fl.head)

	for freeListNodeSize(node) <= topn {
		topn -= freeListNodeSize(node)
		next := freeListNext(node)
		assert(next != 0, "node is tail")
		node = fl.get(next)
	}
	return freelistNodeGetPointer(node, freeListNodeSize(node)-topn-1)
}

// remove `popn` pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	assert(popn <= fl.ListLen(), "popn out of range")
	if popn == 0 && len(freed) == 0 {
		return
	}

	// construct the new list
	total := fl.ListLen()
	reuse := []uint64{}

	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recyle the node itself

		if popn >= freeListNodeSize(node) {
			// remove all pointers in this node
			popn -= freeListNodeSize(node)
		} else {
			// remove some pointers
			remain := freeListNodeSize(node) - popn
			popn = 0

			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, freelistNodeGetPointer(node, remain))
			}

			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, freelistNodeGetPointer(node, i))
			}
		}

		// discard the node and move to the next node
		total -= freeListNodeSize(node)
		fl.head = freeListNext(node)
	}

	assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0, "no enough free list nodes")
	freeListPush(fl, freed, reuse)
	freeListNodeSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func freeListPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		newNode := make(BNode, BTREE_MAX_NODE_SIZE)
		size := min(len(freed), FREE_LIST_CAP)
		freeListNodeSetHeader(newNode, uint16(size), fl.head)

		for i, pointer := range freed[:size] {
			freeListNodeSetPointer(newNode, i, pointer)
		}

		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse pointer
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, newNode)
		} else {
			// append page for new node
			fl.head = fl.new(newNode)
		}
	}
	assert(len(reuse) == 0, "no enough free list nodes")
}
