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

	d := &PagerTester{}
	d.ref = map[string]string{}
	d.db.Path = "test.db"
	err := d.db.Open()
	assert(err == nil, "open failed")
	return d
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
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPointer(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(pt.db.tree.root)
	assert(keys[0] == "", "root is not leaf")
	assert(vals[0] == "", "root is not leaf")
	return keys[1:], vals[1:]
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

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1, "nkeys < 1")

		if node.btype() == BTREE_LEAF {
			return
		}

		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := pt.db.tree.get(node.getPointer(i))
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}
	nodeVerify(pt.db.tree.get(pt.db.tree.root))
}

func TestKVBasic(t *testing.T) {
	fmt.Println("TestKVBasic")
	pt := newPagerTester()
	defer pt.dispose()

	pt.add("k", "v")
	pt.verify(t)

	// insert
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		pt.add(key, val)
		pt.verify(t)

		if i < 20 {
			pt.verify(t)
		}
	}

	pt.verify(t)
	t.Log("insertion done")

	// deletion
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

func TestKVRandLength(t *testing.T) {
	fmt.Println("TestKVRandLength")
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

func TestKVIncLength(t *testing.T) {
	fmt.Println("TestKVIncLength")
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
		size := min(factor*factor*2, 40)
		size = max(size, 10)

		for i := 0; i < size; i++ {
			rand.Read(key)
			pt.add(string(key), string(val))
		}
		pt.verify(t)
		pt.dispose()
	}
}
