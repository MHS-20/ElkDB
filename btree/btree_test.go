package btree

import (
	"fmt"
	"math/rand"
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

type C struct {
	tree  BTree
	ref   map[string]string
	store *testStore
}

func newC() *C {
	store := &testStore{pages: map[uint64]BNode{}}
	return &C{
		tree:  BTree{Store: store},
		ref:   map[string]string{},
		store: store,
	}
}

func (c *C) add(key, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) dump() ([]string, []string) {
	keys, vals := []string{}, []string{}
	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := c.store.PageGet(ptr)
		nkeys := node.nkeys()
		if node.btype() == BNodeLeaf {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				nodeDump(node.getPtr(i))
			}
		}
	}
	nodeDump(c.tree.Root)
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

func (c *C) verify(t *testing.T) {
	keys, vals := c.dump()
	rkeys, rvals := []string{}, []string{}
	for k, v := range c.ref {
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
		for i := uint16(0); i < nkeys; i++ {
			kid := c.store.PageGet(node.getPtr(i))
			is.Equal(t, node.getKey(i), kid.getKey(0))
			nodeVerify(kid)
		}
	}
	nodeVerify(c.store.PageGet(c.tree.Root))
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
	c := newC()
	c.add("k", "v")
	c.verify(t)

	for i := 0; i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)

	for i := 2000; i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
	}
	c.verify(t)

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		c.add(key, val)
		c.verify(t)
	}
	for i := 0; i < 2000; i++ {
		root := c.tree.Root
		c.add("k", "v")
		is.Equal(t, root, c.tree.Root)
	}

	is.False(t, c.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
		c.verify(t)
	}

	c.add("k", "v2")
	c.verify(t)
	c.del("k")
	c.verify(t)

	is.Equal(t, 1, len(c.store.pages))
	is.Equal(t, uint16(1), c.store.PageGet(c.tree.Root).nkeys())
}

func TestBTreeRandLength(t *testing.T) {
	c := newC()
	for i := 0; i < 2000; i++ {
		klen := fmix32(uint32(2*i+0)) % MaxKeySize
		vlen := fmix32(uint32(2*i+1)) % MaxValSize
		if klen == 0 {
			continue
		}
		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		c.add(string(key), string(val))
		c.verify(t)
	}
}

func TestBTreeIncLength(t *testing.T) {
	for l := 1; l < MaxKeySize+MaxValSize; l++ {
		c := newC()
		klen := l
		if klen > MaxKeySize {
			klen = MaxKeySize
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)
		factor := PageSize / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			rand.Read(key)
			c.add(string(key), string(val))
		}
		c.verify(t)
	}
}
