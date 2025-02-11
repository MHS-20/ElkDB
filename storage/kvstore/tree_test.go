package kvstore

import (
	"fmt"
	"testing"
	"unsafe"
)

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

// --- TEST HELPERS ---

func (tt *TreeTester) add(key string, val string) {
	tt.tree.Insert([]byte(key), []byte(val))
	tt.ref[key] = val // reference data
}

func (tt *TreeTester) del(key string) {
	tt.tree.Delete([]byte(key))
	delete(tt.ref, key) // reference data
}

func (tt *TreeTester) printTree() {
	for k, v := range tt.ref {
		println("Key:", k, "Value:", v)
	}
}

// --- TESTS ---
func TestBTree(t *testing.T) {
	tt := newTreeTester()
	fmt.Print("Running BTree tests...\n")

	// Add some key-value pairs
	tt.add("a", "1")
	tt.add("b", "2")
	tt.add("c", "3")
	tt.add("d", "4")
	tt.add("e", "5")
	tt.add("f", "6")
	tt.add("g", "7")
	tt.add("h", "8")
	tt.add("i", "9")
	tt.add("j", "10")
	tt.printTree()

	// check keys
	if len(tt.ref) != 10 {
		t.Errorf("Expected 10 keys, got %d", len(tt.ref))
	}

	// Delete some key-value pairs
	tt.del("a")
	tt.del("b")
	tt.del("c")
	tt.del("d")
	tt.del("e")
	tt.printTree()

	// check keys
	if len(tt.ref) != 5 {
		t.Errorf("Expected 5 keys after deletions, got %d", len(tt.ref))
	}

	// Add some more key-value pairs
	tt.add("k", "11")
	tt.add("l", "12")
	tt.add("m", "13")
	tt.add("n", "14")
	tt.add("o", "15")
	tt.add("p", "16")
	tt.add("q", "17")
	tt.add("r", "18")
	tt.add("s", "19")
	tt.add("t", "20")
	tt.printTree()

	// Delete all key-value pairs
	for key := range tt.ref {
		tt.del(key)
	}
}
