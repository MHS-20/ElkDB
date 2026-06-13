package kv

import (
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/MHS-20/ElkDB/btree"
	is "github.com/stretchr/testify/require"
)

type kvTester struct {
	db  KV
	ref map[string]string
}

func newKVTester() *kvTester {
	os.Remove("test.db")
	os.Remove("test.db.wal")
	kvt := &kvTester{ref: map[string]string{}}
	kvt.db.Path = "test.db"
	kvt.db.NoSync = true
	err := kvt.db.Open()
	assert(err == nil)
	return kvt
}

func (kvt *kvTester) reopen() {
	kvt.db.Close()
	kvt.db = KV{Path: kvt.db.Path}
	err := kvt.db.Open()
	assert(err == nil)
}

func (kvt *kvTester) dispose() {
	kvt.db.Close()
	os.Remove("test.db")
	os.Remove("test.db.wal")
}

func (kvt *kvTester) add(key, val string) {
	tx := KVTX{}
	kvt.db.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte(key), Val: []byte(val)})
	err := kvt.db.Commit(&tx)
	assert(err == nil)
	kvt.ref[key] = val
}

func (kvt *kvTester) del(key string) bool {
	delete(kvt.ref, key)
	tx := KVTX{}
	kvt.db.Begin(&tx)
	deleted := tx.Del(&btree.DeleteReq{Key: []byte(key)})
	err := kvt.db.Commit(&tx)
	assert(err == nil)
	return deleted
}

func (kvt *kvTester) verify(t *testing.T) {
	tx := KVReader{}
	kvt.db.BeginRead(&tx)
	defer kvt.db.EndRead(&tx)

	rkeys := []string{}
	for k := range kvt.ref {
		rkeys = append(rkeys, k)
	}
	sort.Strings(rkeys)

	for k, v := range kvt.ref {
		got, ok := tx.Get([]byte(k))
		is.True(t, ok)
		is.Equal(t, []byte(v), got)
	}

	if len(rkeys) == 0 {
		return
	}
	iter := tx.Seek([]byte(rkeys[0]), btree.CmpGE)
	for _, k := range rkeys {
		is.True(t, iter.Valid())
		gotk, _ := iter.Deref()
		is.Equal(t, []byte(k), gotk)
		iter.Next()
	}
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func TestKVBasic(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	kvt.add("k", "v")
	kvt.verify(t)

	for i := 0; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		kvt.add(key, val)
		if i < 2000 {
			kvt.verify(t)
		}
	}
	kvt.verify(t)
	t.Log("insertion done")

	for i := 2000; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, kvt.del(key))
	}
	kvt.verify(t)
	t.Log("deletion done")

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		kvt.add(key, val)
		kvt.verify(t)
	}

	is.False(t, kvt.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, kvt.del(key))
		kvt.verify(t)
	}

	kvt.add("k", "v2")
	kvt.verify(t)
	kvt.del("k")
	kvt.verify(t)
}

func TestKVRandLength(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	for i := 0; i < 2000; i++ {
		klen := fmix32(uint32(2*i+0)) % btree.MaxKeySize
		vlen := fmix32(uint32(2*i+1)) % btree.MaxValSize
		if klen == 0 {
			continue
		}
		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		kvt.add(string(key), string(val))
		kvt.verify(t)
	}
}

func TestKVIncLength(t *testing.T) {
	for l := 1; l < btree.MaxKeySize+btree.MaxValSize; l++ {
		kvt := newKVTester()
		klen := l
		if klen > btree.MaxKeySize {
			klen = btree.MaxKeySize
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)
		factor := btree.PageSize / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			rand.Read(key)
			kvt.add(string(key), string(val))
		}
		kvt.verify(t)
		kvt.dispose()
	}
}
