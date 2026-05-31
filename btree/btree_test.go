package btree

import (
	"crypto/rand"
	"fmt"
	"sort"
	"testing"
	"unsafe"

	is "github.com/stretchr/testify/require"
)

type testStore struct {
	pages map[uint64]BNode
}

func (s *testStore) PageGet(ptr uint64) BNode {
	node, ok := s.pages[ptr]
	assert(ok)
	return node
}

func (s *testStore) PageNew(node BNode) uint64 {
	assert(node.nbytes() <= PageSize)
	key := uint64(uintptr(unsafe.Pointer(&node.Data[0])))
	assert(s.pages[key].Data == nil)
	s.pages[key] = node
	return key
}

func (s *testStore) PageDel(ptr uint64) {
	_, ok := s.pages[ptr]
	assert(ok)
	delete(s.pages, ptr)
}

type btreeTester struct {
	tree  BTree
	ref   map[string]string
	store *testStore
}

func newBTreeTester() *btreeTester {
	store := &testStore{pages: map[uint64]BNode{}}
	return &btreeTester{
		tree:  BTree{Store: store},
		ref:   map[string]string{},
		store: store,
	}
}

func (btt *btreeTester) add(key, val string) {
	btt.tree.Insert([]byte(key), []byte(val))
	btt.ref[key] = val
}

func (btt *btreeTester) del(key string) bool {
	delete(btt.ref, key)
	return btt.tree.Delete([]byte(key))
}

func (btt *btreeTester) dump() ([]string, []string) {
	keys, vals := []string{}, []string{}
	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := btt.store.PageGet(ptr)
		nkeys := node.nkeys()
		if node.btype() == BNodeLeaf {
			for i := range nkeys {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := range nkeys {
				nodeDump(node.getPtr(i))
			}
		}
	}
	nodeDump(btt.tree.Root)
	assert(keys[0] == "")
	assert(vals[0] == "")
	return keys[1:], vals[1:]
}

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (s sortIF) Len() int           { return s.len }
func (s sortIF) Less(i, j int) bool { return s.less(i, j) }
func (s sortIF) Swap(i, j int)      { s.swap(i, j) }

func (btt *btreeTester) verify(t *testing.T) {
	keys, vals := btt.dump()
	rkeys, rvals := []string{}, []string{}
	for k, v := range btt.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}
	is.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			rkeys[i], rvals[i], rkeys[j], rvals[j] = rkeys[j], rvals[j], rkeys[i], rvals[i]
		},
	})
	is.Equal(t, rkeys, keys)
	is.Equal(t, rvals, vals)

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1)
		if node.btype() == BNodeLeaf {
			return
		}
		for i := range nkeys {
			kid := btt.store.PageGet(node.getPtr(i))
			is.Equal(t, node.getKey(i), kid.getKey(0))
			nodeVerify(kid)
		}
	}
	nodeVerify(btt.store.PageGet(btt.tree.Root))
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func TestBTreeBasic(t *testing.T) {
	btt := newBTreeTester()
	btt.add("k", "v")
	btt.verify(t)

	for i := range 250000 {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		btt.add(key, val)
		if i < 2000 {
			btt.verify(t)
		}
	}
	btt.verify(t)

	for i := 2000; i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, btt.del(key))
	}
	btt.verify(t)

	for i := range 2000 {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		btt.add(key, val)
		btt.verify(t)
	}
	for range 2000 {
		root := btt.tree.Root
		btt.add("k", "v")
		is.Equal(t, root, btt.tree.Root)
	}

	is.False(t, btt.del("kk"))

	for i := range 2000 {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, btt.del(key))
		btt.verify(t)
	}

	btt.add("k", "v2")
	btt.verify(t)
	btt.del("k")
	btt.verify(t)

	is.Equal(t, 1, len(btt.store.pages))
	is.Equal(t, uint16(1), btt.store.PageGet(btt.tree.Root).nkeys())
}

func TestBTreeRandLength(t *testing.T) {
	btt := newBTreeTester()
	for i := range 2000 {
		klen := fmix32(uint32(2*i+0)) % MaxKeySize
		vlen := fmix32(uint32(2*i+1)) % MaxValSize
		if klen == 0 {
			continue
		}
		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		btt.add(string(key), string(val))
		btt.verify(t)
	}
}

func TestBTreeIncLength(t *testing.T) {
	for l := 1; l < MaxKeySize+MaxValSize; l++ {
		btt := newBTreeTester()
		klen := min(l, MaxKeySize)
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)
		factor := PageSize / l
		size := max(min(factor*factor*2, 4000), 10)
		for range size {
			rand.Read(key)
			btt.add(string(key), string(val))
		}
		btt.verify(t)
	}
}
