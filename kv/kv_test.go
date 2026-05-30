package kv

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/MHS-20/ElkDB/btree"
	is "github.com/stretchr/testify/require"
)

type D struct {
	db  KV
	ref map[string]string
}

func newD() *D {
	os.Remove("test.db")
	d := &D{ref: map[string]string{}}
	d.db.Path = "test.db"
	d.db.NoSync = true
	err := d.db.Open()
	assert(err == nil)
	return d
}

func (d *D) reopen() {
	d.db.Close()
	d.db = KV{Path: d.db.Path}
	err := d.db.Open()
	assert(err == nil)
}

func (d *D) dispose() {
	d.db.Close()
	os.Remove("test.db")
}

func (d *D) add(key, val string) {
	tx := KVTX{}
	d.db.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte(key), Val: []byte(val)})
	err := d.db.Commit(&tx)
	assert(err == nil)
	d.ref[key] = val
}

func (d *D) del(key string) bool {
	delete(d.ref, key)
	tx := KVTX{}
	d.db.Begin(&tx)
	deleted := tx.Del(&btree.DeleteReq{Key: []byte(key)})
	err := d.db.Commit(&tx)
	assert(err == nil)
	return deleted
}

func (d *D) verify(t *testing.T) {
	tx := KVReader{}
	d.db.BeginRead(&tx)
	defer d.db.EndRead(&tx)

	rkeys := []string{}
	for k := range d.ref {
		rkeys = append(rkeys, k)
	}
	sort.Strings(rkeys)

	for k, v := range d.ref {
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
	c := newD()
	defer c.dispose()

	c.add("k", "v")
	c.verify(t)

	for i := 0; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)
	t.Log("insertion done")

	for i := 2000; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
	}
	c.verify(t)
	t.Log("deletion done")

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		c.add(key, val)
		c.verify(t)
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
}

func TestKVRandLength(t *testing.T) {
	c := newD()
	defer c.dispose()

	for i := 0; i < 2000; i++ {
		klen := fmix32(uint32(2*i+0)) % btree.MaxKeySize
		vlen := fmix32(uint32(2*i+1)) % btree.MaxValSize
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

func TestKVIncLength(t *testing.T) {
	for l := 1; l < btree.MaxKeySize+btree.MaxValSize; l++ {
		c := newD()
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
			c.add(string(key), string(val))
		}
		c.verify(t)
		c.dispose()
	}
}
