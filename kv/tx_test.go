package kv

import (
	"testing"

	"github.com/MHS-20/ElkDB/btree"
	is "github.com/stretchr/testify/require"
)

func TestKVTX(t *testing.T) {
	kvt := newKVTester()

	kvt.add("k1", "v1")

	tx := KVTX{}
	kvt.db.Begin(&tx)

	tx.Update(&btree.InsertReq{Key: []byte("k1"), Val: []byte("xxx")})
	tx.Update(&btree.InsertReq{Key: []byte("k2"), Val: []byte("xxx")})

	val, ok := tx.Get([]byte("k1"))
	is.True(t, ok)
	is.Equal(t, []byte("xxx"), val)
	val, ok = tx.Get([]byte("k2"))
	is.True(t, ok)
	is.Equal(t, []byte("xxx"), val)

	kvt.db.Abort(&tx)
	kvt.verify(t)

	kvt.reopen()
	kvt.verify(t)

	{
		tx := KVTX{}
		kvt.db.Begin(&tx)
		_, ok = tx.Get([]byte("k2"))
		is.False(t, ok)
		kvt.db.Abort(&tx)
	}

	kvt.dispose()
}

func TestKVRW(t *testing.T) {
	kvt := newKVTester()

	{
		r0 := KVReader{}
		kvt.db.BeginRead(&r0)

		kvt.add("k1", "v1")
		{
			r1 := KVReader{}
			kvt.db.BeginRead(&r1)

			kvt.add("k2", "v2")
			_, ok := r1.Get([]byte("k2"))
			assert(!ok)
			val, ok := r1.Get([]byte("k1"))
			assert(ok && string(val) == "v1")

			kvt.db.EndRead(&r1)
		}

		kvt.add("k3", "v3")

		_, ok := r0.Get([]byte("k1"))
		assert(!ok)
		_, ok = r0.Get([]byte("k2"))
		assert(!ok)
		_, ok = r0.Get([]byte("k3"))
		assert(!ok)

		kvt.db.EndRead(&r0)
	}

	{
		r3 := KVReader{}
		kvt.db.BeginRead(&r3)
		val, ok := r3.Get([]byte("k1"))
		assert(ok && string(val) == "v1")
		val, ok = r3.Get([]byte("k2"))
		assert(ok && string(val) == "v2")
		val, ok = r3.Get([]byte("k3"))
		assert(ok && string(val) == "v3")
		kvt.db.EndRead(&r3)
	}

	assert(kvt.db.version == 3)

	kvt.dispose()
}
