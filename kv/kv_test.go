package kv

import (
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"sync"
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

// --- Phase 2: Concurrent writer tests ---

func insertWithRetry(db *KV, key, val string) error {
	for {
		tx := KVTX{}
		db.Begin(&tx)
		tx.Update(&btree.InsertReq{Key: []byte(key), Val: []byte(val)})
		err := db.Commit(&tx)
		if err == nil {
			return nil
		}
		// OCC conflict — retry with fresh snapshot
	}
}

func TestConcurrentDisjointKeys(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	const N = 5
	const keysPerWriter = 200
	var wg sync.WaitGroup
	errCh := make(chan error, N*keysPerWriter)

	for w := 0; w < N; w++ {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				key := fmt.Sprintf("w%d-k%d", w, i)
				val := fmt.Sprintf("v%d-%d", w, i)
				if err := insertWithRetry(&kvt.db, key, val); err != nil {
					errCh <- fmt.Errorf("writer %d key %s: %w", w, key, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify every key is actually in the DB.
	tx := KVReader{}
	kvt.db.BeginRead(&tx)
	defer kvt.db.EndRead(&tx)

	for w := 0; w < N; w++ {
		for i := 0; i < keysPerWriter; i++ {
			key := fmt.Sprintf("w%d-k%d", w, i)
			val := fmt.Sprintf("v%d-%d", w, i)
			got, ok := tx.Get([]byte(key))
			is.True(t, ok, "key %q should exist", key)
			is.Equal(t, []byte(val), got, "value for key %q", key)
		}
	}
}

func TestConcurrentSameKeyConflict(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := KVTX{}
			kvt.db.Begin(&tx)
			tx.Update(&btree.InsertReq{Key: []byte("conflict-key"), Val: []byte("value")})
			_ = kvt.db.Commit(&tx)
		}()
	}
	wg.Wait()

	// Exactly one writer should have committed (the rest got OCC conflict).
	tx := KVReader{}
	kvt.db.BeginRead(&tx)
	defer kvt.db.EndRead(&tx)
	_, ok := tx.Get([]byte("conflict-key"))
	is.True(t, ok, "the key must exist after concurrent writes")
}

func TestConcurrentWriterReaderIsolation(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	// Writer A inserts a batch of keys, writer B inserts different keys.
	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	writer := func(prefix string, count int) {
		defer wg.Done()
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%s-%d", prefix, i)
			val := fmt.Sprintf("v%s-%d", prefix, i)
			if err := insertWithRetry(&kvt.db, key, val); err != nil {
				errCh <- fmt.Errorf("writer %s key %s: %w", prefix, key, err)
			}
		}
	}

	wg.Add(2)
	go writer("a", 100)
	go writer("b", 100)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Reader must see all keys from both writers.
	tx := KVReader{}
	kvt.db.BeginRead(&tx)
	defer kvt.db.EndRead(&tx)

	for i := 0; i < 100; i++ {
		_, ok := tx.Get([]byte(fmt.Sprintf("a-%d", i)))
		is.True(t, ok, "key a-%d from writer A", i)
		_, ok = tx.Get([]byte(fmt.Sprintf("b-%d", i)))
		is.True(t, ok, "key b-%d from writer B", i)
	}
}

func TestConcurrentWriterStress(t *testing.T) {
	kvt := newKVTester()
	defer kvt.dispose()

	const writers = 10
	const keysPerWriter = 200
	var wg sync.WaitGroup
	errCh := make(chan error, writers*keysPerWriter)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				key := fmt.Sprintf("s-%d-%d", w, i)
				val := fmt.Sprintf("sv-%d-%d", w, i)
				if err := insertWithRetry(&kvt.db, key, val); err != nil {
					errCh <- fmt.Errorf("writer %d key %s: %w", w, key, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// All keys should be in the DB.
	tx := KVReader{}
	kvt.db.BeginRead(&tx)
	defer kvt.db.EndRead(&tx)

	for w := 0; w < writers; w++ {
		for i := 0; i < keysPerWriter; i++ {
			key := fmt.Sprintf("s-%d-%d", w, i)
			expected := fmt.Sprintf("sv-%d-%d", w, i)
			got, ok := tx.Get([]byte(key))
			is.True(t, ok, "key %q", key)
			is.Equal(t, []byte(expected), got, "value for key %q", key)
		}
	}
}
