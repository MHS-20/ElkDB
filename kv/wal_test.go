package kv

import (
	"os"
	"syscall"
	"testing"

	"github.com/MHS-20/ElkDB/btree"
	is "github.com/stretchr/testify/require"
)

func TestWALOpenClose(t *testing.T) {
	path := tempWAL(t)
	wal, err := OpenWAL(path)
	is.NoError(t, err)
	is.NotNil(t, wal)
	is.NoError(t, wal.Close())

	// Reopen should see header
	wal2, err := OpenWAL(path)
	is.NoError(t, err)
	hasData, err := wal2.HasData()
	is.NoError(t, err)
	is.False(t, hasData)
	is.NoError(t, wal2.Close())
}

func TestWALWriteAndRead(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg := make([]byte, btree.PageSize)
	copy(pg, "hello world")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 42, pg))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 10, FreeHead: 5, PageFlushed: 100}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.NotNil(t, state)
	is.Equal(t, uint64(10), state.Root)
	is.Equal(t, uint64(5), state.FreeHead)
	is.Equal(t, uint64(100), state.PageFlushed)
	is.Len(t, entries, 1)
	is.Equal(t, uint64(42), entries[0].pageNum)
	is.Equal(t, pg, entries[0].data)
}

func TestWALMultipleTransactions(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg1 := make([]byte, btree.PageSize)
	copy(pg1, "page1")
	pg2 := make([]byte, btree.PageSize)
	copy(pg2, "page2")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 10, pg1))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 2, PageFlushed: 50}))

	is.NoError(t, wal.BeginTX(2))
	is.NoError(t, wal.PageData(2, 20, pg2))
	is.NoError(t, wal.CommitTX(2, commitState{Root: 3, PageFlushed: 60}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.NotNil(t, state)
	is.Equal(t, uint64(3), state.Root)
	is.Equal(t, uint64(60), state.PageFlushed)
	is.Len(t, entries, 2)
}

func TestWALUncommittedTransactionIgnored(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg := make([]byte, btree.PageSize)
	copy(pg, "data")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 42, pg))
	// No CommitTX for tx 1 — should be ignored

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.Nil(t, state)
	is.Empty(t, entries)
}

func TestWALCorruptionStopsReplay(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg := make([]byte, btree.PageSize)
	copy(pg, "before")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 10, pg))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 1, PageFlushed: 20}))

	// Write garbage after valid records
	_, err := wal.fp.Write([]byte("GARBAGE"))
	is.NoError(t, err)

	// Write a valid record after garbage (should not be read)
	is.NoError(t, wal.BeginTX(2))
	is.NoError(t, wal.CommitTX(2, commitState{Root: 99, PageFlushed: 99}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	// Should have stopped before garbage; only tx 1 recovered
	is.NotNil(t, state)
	is.Equal(t, uint64(1), state.Root)
	is.Len(t, entries, 1)
}

func TestWALCorruptCRCStopsReplay(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg := make([]byte, btree.PageSize)
	copy(pg, "valid")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 10, pg))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 1, PageFlushed: 20}))

	// Read the WAL and corrupt a CRC
	data, err := os.ReadFile(wal.path)
	is.NoError(t, err)
	// Corrupt byte at position 21 (CRC of first record after header)
	data[21] ^= 0xFF
	is.NoError(t, os.WriteFile(wal.path, data, 0644))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.Nil(t, state)
	is.Empty(t, entries)
}

func TestWALPageDedup(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg1 := make([]byte, btree.PageSize)
	copy(pg1, "first")
	pg2 := make([]byte, btree.PageSize)
	copy(pg2, "second")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 42, pg1))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 1, PageFlushed: 50}))

	// Second transaction overwrites same page
	is.NoError(t, wal.BeginTX(2))
	is.NoError(t, wal.PageData(2, 42, pg2))
	is.NoError(t, wal.CommitTX(2, commitState{Root: 2, PageFlushed: 50}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.NotNil(t, state)
	is.Equal(t, uint64(2), state.Root)
	// Only the latest version of page 42
	is.Len(t, entries, 1)
	is.Equal(t, uint64(42), entries[0].pageNum)
	is.Equal(t, pg2, entries[0].data)
}

func TestWALReset(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 1, PageFlushed: 10}))

	hasData, err := wal.HasData()
	is.NoError(t, err)
	is.True(t, hasData)

	is.NoError(t, wal.reset())

	hasData, err = wal.HasData()
	is.NoError(t, err)
	is.False(t, hasData)

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.Nil(t, state)
	is.Empty(t, entries)
}

func TestWALRecovery(t *testing.T) {
	dbPath := tempDB(t)
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Write some data
	db1 := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db1.Open())

	tx := KVTX{}
	db1.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte("k1"), Val: []byte("v1")})
	is.NoError(t, db1.Commit(&tx))

	tx2 := KVTX{}
	db1.Begin(&tx2)
	tx2.Update(&btree.InsertReq{Key: []byte("k2"), Val: []byte("v2")})
	is.NoError(t, db1.Commit(&tx2))

	// Simulate crash: close without checkpoint (skip the KV.Close that
	// would normally checkpoint). Instead just close the WAL and file.
	is.NoError(t, db1.wal.Close())
	for _, chunk := range db1.mmap.chunks {
		_ = syscall.Munmap(chunk)
	}
	_ = db1.fp.Close()

	// Reopen — WAL should recover the committed data
	db2 := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db2.Open())
	defer db2.Close()

	tx3 := KVReader{}
	db2.BeginRead(&tx3)
	defer db2.EndRead(&tx3)

	v, ok := tx3.Get([]byte("k1"))
	is.True(t, ok)
	is.Equal(t, []byte("v1"), v)

	v, ok = tx3.Get([]byte("k2"))
	is.True(t, ok)
	is.Equal(t, []byte("v2"), v)
}

func TestWALCheckpoint(t *testing.T) {
	dbPath := tempDB(t)
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	db := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db.Open())

	tx := KVTX{}
	db.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte("k"), Val: []byte("v")})
	is.NoError(t, db.Commit(&tx))

	// Checkpoint explicitly
	hasData, err := db.wal.HasData()
	is.NoError(t, err)
	is.True(t, hasData)

	is.NoError(t, db.wal.Checkpoint(db))

	// After checkpoint, WAL should be empty
	hasData, err = db.wal.HasData()
	is.NoError(t, err)
	is.False(t, hasData)

	// Data should still be readable
	tx2 := KVReader{}
	db.BeginRead(&tx2)
	v, ok := tx2.Get([]byte("k"))
	db.EndRead(&tx2)
	is.True(t, ok)
	is.Equal(t, []byte("v"), v)

	db.Close()

	// Reopen: master page should have data (no WAL recovery needed)
	db2 := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db2.Open())
	defer db2.Close()

	tx3 := KVReader{}
	db2.BeginRead(&tx3)
	v, ok = tx3.Get([]byte("k"))
	db2.EndRead(&tx3)
	is.True(t, ok)
	is.Equal(t, []byte("v"), v)
}

func TestWALNoData(t *testing.T) {
	// Empty WAL (just header)
	wal := newTestWAL(t)
	defer wal.Close()

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.Nil(t, state)
	is.Empty(t, entries)
}

func TestWALCheckpointThenRecovery(t *testing.T) {
	dbPath := tempDB(t)
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	db := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db.Open())

	tx := KVTX{}
	db.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte("persist"), Val: []byte("me")})
	is.NoError(t, db.Commit(&tx))

	// Normal close — triggers checkpoint
	db.Close()

	// Reopen — should see data from master page
	db2 := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db2.Open())
	defer db2.Close()

	tx2 := KVReader{}
	db2.BeginRead(&tx2)
	v, ok := tx2.Get([]byte("persist"))
	db2.EndRead(&tx2)
	is.True(t, ok)
	is.Equal(t, []byte("me"), v)
}

func TestWALUncommittedAfterCrash(t *testing.T) {
	dbPath := tempDB(t)
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	db := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db.Open())

	// Commit one value
	tx := KVTX{}
	db.Begin(&tx)
	tx.Update(&btree.InsertReq{Key: []byte("a"), Val: []byte("1")})
	is.NoError(t, db.Commit(&tx))

	// Start but don't commit another transaction
	tx2 := KVTX{}
	db.Begin(&tx2)
	tx2.Update(&btree.InsertReq{Key: []byte("b"), Val: []byte("2")})
	// Crash: abort
	db.Abort(&tx2)

	// Close normally
	db.Close()

	// Reopen — only "a" should exist
	db2 := &KV{Path: dbPath, NoSync: true}
	is.NoError(t, db2.Open())
	defer db2.Close()

	tx3 := KVReader{}
	db2.BeginRead(&tx3)
	_, ok := tx3.Get([]byte("b"))
	db2.EndRead(&tx3)
	is.False(t, ok)

	v, ok := tx3.Get([]byte("a"))
	db2.BeginRead(&tx3)
	v, ok = tx3.Get([]byte("a"))
	db2.EndRead(&tx3)
	is.True(t, ok)
	is.Equal(t, []byte("1"), v)
}

func TestWALBeginTXAndCommitTXOnly(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	// Transaction with no page data (valid but empty)
	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 5, PageFlushed: 10}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.NotNil(t, state)
	is.Equal(t, uint64(5), state.Root)
	is.Empty(t, entries)
}

func TestWALMultiplePageDataPerTX(t *testing.T) {
	wal := newTestWAL(t)
	defer wal.Close()

	pg1 := make([]byte, btree.PageSize)
	copy(pg1, "p1")
	pg2 := make([]byte, btree.PageSize)
	copy(pg2, "p2")
	pg3 := make([]byte, btree.PageSize)
	copy(pg3, "p3")

	is.NoError(t, wal.BeginTX(1))
	is.NoError(t, wal.PageData(1, 10, pg1))
	is.NoError(t, wal.PageData(1, 20, pg2))
	is.NoError(t, wal.PageData(1, 30, pg3))
	is.NoError(t, wal.CommitTX(1, commitState{Root: 1, PageFlushed: 50}))

	entries, state, err := wal.readCommitted()
	is.NoError(t, err)
	is.NotNil(t, state)
	is.Len(t, entries, 3)
}

// --- helpers ---

func newTestWAL(t *testing.T) *WAL {
	t.Helper()
	path := tempWAL(t)
	wal, err := OpenWAL(path)
	is.NoError(t, err)
	t.Cleanup(func() {
		wal.Close()
		os.Remove(path)
	})
	return wal
}

func tempWAL(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "elkdb-wal-test-*.wal")
	is.NoError(t, err)
	f.Close()
	return f.Name()
}

func tempDB(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "elkdb-db-test-*.db")
	is.NoError(t, err)
	f.Close()
	return f.Name()
}
