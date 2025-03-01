package btree

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	is "github.com/stretchr/testify/require"
)

type PagerTester struct {
	db  KV
	ref map[string]string
}

func newPagerTester() *PagerTester {
	os.Remove("test.db")

	pt := &PagerTester{}
	pt.ref = map[string]string{}
	pt.db.Path = "test.db"
	err := pt.db.Open()
	assert(err == nil, "open failed")
	return pt
}

func (pt *PagerTester) dispose() {
	pt.db.Close()
	os.Remove("test.db")
}

func (pt *PagerTester) add(key string, val string) {
	pt.db.Set([]byte(key), []byte(val))
	pt.ref[key] = val
}

func (pt *PagerTester) del(key string) bool {
	delete(pt.ref, key)
	deleted, err := pt.db.Del([]byte(key))
	assert(err == nil, "delete failed")
	return deleted
}

func (pt *PagerTester) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := pt.db.tree.get(ptr)
		nkeys := node.nkeys()
		if node.btype() == BTREE_LEAF {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			assert(node.btype() == BTREE_NODE, "invalid node type")
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPointer(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(pt.db.tree.root)
	assert(keys[0] == "", "invalid root key")
	assert(vals[0] == "", "invalid root val")
	return keys[1:], vals[1:]
}

func flDump(fl *FreeList) []uint64 {
	ptrs := []uint64{}
	head := fl.head
	for head != 0 {
		node := fl.get(head)
		size := freeListNodeSize(node)
		for i := 0; i < size; i++ {
			ptrs = append(ptrs, freelistNodeGetPointer(node, i))
		}
		head = freeListNext(node)
	}
	return ptrs
}

func (pt *PagerTester) verify(t *testing.T) {
	keys, vals := pt.dump()
	rkeys, rvals := []string{}, []string{}

	for k, v := range pt.ref {
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

	// node structures
	pages := make([]uint8, pt.db.page.flushed)
	pages[0] = 1
	pages[pt.db.tree.root] = 1
	var nodeVerify func(BNode)

	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1, "invalid number of keys")
		if node.btype() == BTREE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			ptr := node.getPointer(i)
			is.Zero(t, pages[ptr])
			pages[ptr] = 1
			key := node.getKey(i)
			kid := pt.db.tree.get(ptr)
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(pt.db.tree.get(pt.db.tree.root))

	// free list
	for head := pt.db.free.head; head != 0; {
		is.Zero(t, pages[head])
		pages[head] = 2
		head = freeListNext(pt.db.pageGet(head))
	}
	for _, ptr := range flDump(&pt.db.free) {
		is.Zero(t, pages[ptr])
		pages[ptr] = 3
	}
	for _, flag := range pages {
		is.NotZero(t, flag)
	}
}

func TestPagerBasic(t *testing.T) {
	fmt.Println("TestPagerBasic")
	pt := newPagerTester()
	defer pt.dispose()

	pt.add("k", "v")
	pt.verify(t)

	// insert
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		pt.add(key, val)
		if i < 2000 {
			pt.verify(t)
		}
	}

	pt.verify(t)
	t.Log("insertion done")

	// del
	for i := 20; i < 25; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, pt.del(key))
	}

	pt.verify(t)
	t.Log("deletion done")

	// overwrite
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		pt.add(key, val)
		pt.verify(t)
	}

	is.False(t, pt.del("kk"))

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, pt.del(key))
		pt.verify(t)
	}

	pt.add("k", "v2")
	pt.verify(t)
	pt.del("k")
	pt.verify(t)
}

func TestPagerRandLength(t *testing.T) {
	fmt.Println("TestPagerRandLength")
	pt := newPagerTester()
	defer pt.dispose()

	for i := 0; i < 20; i++ {
		klen := fmix32(uint32(2*i+0)) % BTREE_MAX_KEY_SIZE
		vlen := fmix32(uint32(2*i+1)) % BTREE_MAX_VAL_SIZE
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		// rand.Read(val)
		pt.add(string(key), string(val))
		pt.verify(t)
	}
}

func TestPagerIncLength(t *testing.T) {
	fmt.Println("TestPagerIncLength")
	for l := 1; l < BTREE_MAX_KEY_SIZE+BTREE_MAX_VAL_SIZE; l++ {
		pt := newPagerTester()

		klen := l
		if klen > BTREE_MAX_KEY_SIZE {
			klen = BTREE_MAX_KEY_SIZE
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := BTREE_MAX_NODE_SIZE / l
		size := max(min(factor*factor*2, 40), 10)
		for range size {
			rand.Read(key)
			pt.add(string(key), string(val))
		}

		pt.verify(t)
		pt.dispose()
	}
}
