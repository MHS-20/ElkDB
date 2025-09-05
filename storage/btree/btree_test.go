package btree

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"
	"unsafe"

	is "github.com/stretchr/testify/require"
)

type TreeTester struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newTreeTester() *TreeTester {
	pages := map[uint64]BNode{}
	return &TreeTester{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok, "page not found")
				return node
			},
			new: func(node BNode) uint64 {
				// fmt.Println("new: ", node.nbytes())
				assert(node.nbytes() <= BTREE_MAX_NODE_SIZE, "node too large")
				key := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[key] == nil, "page already exists")
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok, "page not found")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (tt *TreeTester) add(key string, val string) {
	tt.tree.Insert([]byte(key), []byte(val))
	tt.ref[key] = val
}

func (tt *TreeTester) del(key string) bool {
	delete(tt.ref, key)
	return tt.tree.Delete([]byte(key))
}

func (tt *TreeTester) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := tt.tree.get(ptr)
		node = BNode(node)
		nkeys := node.nkeys()

		if node.btype() == BTREE_LEAF {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPointer(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(tt.tree.root)
	assert(keys[0] == "", fmt.Sprintf("dummy key: %v", keys[0]))
	assert(vals[0] == "", fmt.Sprintf("dummy val: %v", vals[0]))
	return keys[1:], vals[1:]
}

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (s sortIF) Len() int {
	return s.len
}

func (s sortIF) Less(i, j int) bool {
	return s.less(i, j)
}

func (s sortIF) Swap(i, j int) {
	s.swap(i, j)
}

func (tt *TreeTester) verify(t *testing.T) {
	keys, vals := tt.dump()

	rkeys, rvals := []string{}, []string{}
	for k, v := range tt.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}

	is.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			k, v := rkeys[i], rvals[i]
			rkeys[i], rvals[i] = rkeys[j], rvals[j]
			rkeys[j], rvals[j] = k, v
		},
	})

	is.Equal(t, rkeys, keys)
	is.Equal(t, rvals, vals)

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1, "nkeys")
		if node.btype() == BTREE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := tt.tree.get(node.getPointer(i))
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(tt.tree.get(tt.tree.root))
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// func TestFixed(t *testing.T) {
// 	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
// 	vals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

// 	c := newTreeTester()
// 	for i := range keys {
// 		c.add(keys[i], vals[i])
// 	}
// 	c.verify(t)
// }

func TestBasic(t *testing.T) {
	fmt.Println("TestBasic")
	tt := newTreeTester()
	tt.add("k", "v")
	tt.verify(t)

	// insert
	for i := 0; i < 2500; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		tt.add(key, val)
		if i < 200 {
			tt.verify(t)
		}
	}

	tt.verify(t)

	// del
	for i := 200; i < 2500; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, tt.del(key))
	}

	tt.verify(t)

	// overwrite
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		tt.add(key, val)
		tt.verify(t)
	}

	is.False(t, tt.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, tt.del(key))
		tt.verify(t)
	}

	tt.add("k", "v2")
	tt.verify(t)
	tt.del("k")
	tt.verify(t)

	// the dummy empty key
	is.Equal(t, 1, len(tt.pages))
	is.Equal(t, uint16(1), tt.tree.get(tt.tree.root).nkeys())
}

func TestRandLength(t *testing.T) {
	fmt.Println("TestRandLength")
	tt := newTreeTester()
	for i := 0; i < 200; i++ {
		klen := fmix32(uint32(2*i+0)) % BTREE_MAX_KEY_SIZE
		vlen := fmix32(uint32(2*i+1)) % BTREE_MAX_VAL_SIZE
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		rand.Read(val)
		tt.add(string(key), string(val))
		tt.verify(t)
	}
}

func TestIncLength(t *testing.T) {
	fmt.Println("TestIncLength")
	for l := 1; l < BTREE_MAX_KEY_SIZE+BTREE_MAX_VAL_SIZE; l++ {
		tt := newTreeTester()

		klen := l
		if klen > BTREE_MAX_KEY_SIZE {
			klen = BTREE_MAX_KEY_SIZE
		}

		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := BTREE_MAX_NODE_SIZE / l
		size := factor * factor * 2

		if size > 4000 {
			size = 4000
		}

		if size < 10 {
			size = 10
		}

		for i := 0; i < size; i++ {
			rand.Read(key)
			tt.add(string(key), string(val))
		}

		tt.verify(t)
	}
}

func (tt *TreeTester) update(key string, val string) bool {
	_, exists := tt.ref[key]
	if !exists {
		return false
	}
	req := &InsertReq{Key: []byte(key), Val: []byte(val), Mode: MODE_UPDATE_ONLY}
	tt.tree.InsertImpl(req)
	if !req.Added {
		tt.ref[key] = val
		return true
	}
	return false
}

// Test specifico per la funzionalità di update
func TestUpdate(t *testing.T) {
	fmt.Println("TestUpdate")
	tt := newTreeTester()
	// Inserisci alcune chiavi
	tt.add("a", "1")
	tt.add("b", "2")
	tt.add("c", "3")
	tt.verify(t)

	// Aggiorna una chiave esistente
	is.True(t, tt.update("b", "22"))
	tt.verify(t)
	keys, vals := tt.dump()
	is.Equal(t, []string{"a", "b", "c"}, keys)
	is.Equal(t, []string{"1", "22", "3"}, vals)

	// Aggiorna una chiave non esistente
	is.False(t, tt.update("d", "4"))
	tt.verify(t)
}
