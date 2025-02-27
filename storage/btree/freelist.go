package btree

type FreeList struct {
	head uint64
	get  func(uint64) BNode  // dereference a pointer
	new  func(BNode) uint64  // append a new page
	use  func(uint64, BNode) // reuse a page
}

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_MAX_NODE_SIZE - FREE_LIST_HEADER) / 8
