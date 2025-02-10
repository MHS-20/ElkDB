package kvstore

import "unsafe"

type TreeTester struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newTreeTester() *TreeTester {
	pages := map[uint64]BNode{}
	return &TreeTester{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				assert(ok, "page not found")
				return node
			},
			new: func(node []byte) uint64 {
				assert(BNode(node).nbytes() <= BTREE_MAX_NODE_SIZE, "node too large")
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[ptr] == nil, "page already exists")
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				assert(pages[ptr] != nil, "page not found")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (tt *TreeTester) add(key string, val string) {
	tt.tree.Insert([]byte(key), []byte(val))
	tt.ref[key] = val // reference data
}
