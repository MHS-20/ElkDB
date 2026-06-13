package kv

import (
	"container/heap"
	"fmt"
	"slices"

	"github.com/MHS-20/ElkDB/btree"
)

// KVReader is a snapshot read transaction.
// It satisfies the kv.Reader interface.
type KVReader struct {
	version uint64
	tree    btree.BTree
	mmap    struct {
		chunks [][]byte // snapshot of db.mmap.chunks at the moment Begin was called
	}
	index int  // position in the KV.readers heap
	done  bool // true after EndRead
}

// BeginRead opens a new read transaction, taking a snapshot of the current
// tree root and mmap chunk list.
func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.tree.Root = kv.tree.root
	tx.tree.Store = tx // KVReader implements btree.PageStore (read-only subset)
	tx.version = kv.version
	heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

// EndRead closes a read transaction and removes it from the reader heap.
func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

// --- btree.PageStore implementation for KVReader (read path only) ---

// PageGet returns the BNode at page ptr by reading from the mmap.
// It satisfies the read-only part of btree.PageStore so BTree.Get and
// BTree.Seek work on a KVReader.
func (tx *KVReader) PageGet(ptr uint64) btree.BNode {
	return pageGetMapped(tx.mmap.chunks, ptr)
}

// pageGetMapped is the shared mmap read logic used by both KVReader and KVTX.
func pageGetMapped(chunks [][]byte, ptr uint64) btree.BNode {
	assert(ptr != 0)
	start := uint64(0)
	for _, chunk := range chunks {
		end := start + uint64(len(chunk))/btree.PageSize
		if ptr < end {
			offset := btree.PageSize * (ptr - start)
			pageEnd := offset + btree.PageSize
			return btree.BNode{Data: chunk[offset:pageEnd:pageEnd]}
		}
		start = end
	}
	panic("bad ptr")
}

// KVReader does not support writes; these methods are not part of its public
// surface, but btree.BTree.Store is typed as btree.PageStore (which includes
// PageNew and PageDel).  We provide panicking stubs so a KVReader can be
// assigned to btree.BTree.Store at all — the B-tree read paths never call them.

func (tx *KVReader) PageNew(_ btree.BNode) uint64 { panic("read-only transaction") }
func (tx *KVReader) PageDel(_ uint64)             { panic("read-only transaction") }

// --- kv.Reader interface ---

// Get returns the value for key in this snapshot, or (nil, false) if absent.
func (tx *KVReader) Get(key []byte) ([]byte, bool) {
	return tx.tree.Get(key)
}

// Seek returns an iterator positioned at the key nearest to key satisfying cmp.
func (tx *KVReader) Seek(key []byte, cmp int) *btree.BIter {
	return tx.tree.Seek(key, cmp)
}

// ---------------------------------------------------------------------------

// KVTX is a read-write transaction.
// It satisfies both kv.Reader and kv.Writer.
type KVTX struct {
	KVReader // embedded snapshot (provides Get, Seek, PageGet for committed pages)
	kv       *KV
	free     *btree.FreeList
	page     struct {
		nappend int               // number of pages to append to the file
		updates map[uint64][]byte // nil value = page is freed; non-nil = new content
	}
}

// --- btree.PageStore implementation for KVTX (read + write path) ---

// PageGet returns the BNode at ptr, preferring in-transaction updates over
// the on-disk mmap copy.
func (tx *KVTX) PageGet(ptr uint64) btree.BNode {
	assert(ptr != 0)
	if page, ok := tx.page.updates[ptr]; ok {
		assert(page != nil)
		return btree.BNode{Data: page}
	}
	return pageGetMapped(tx.mmap.chunks, ptr)
}

// PageNew allocates a page: reuses a free page if available, otherwise appends.
func (tx *KVTX) PageNew(node btree.BNode) uint64 {
	assert(len(node.Data) <= btree.PageSize)
	if ptr := tx.free.Pop(); ptr != 0 {
		tx.page.updates[ptr] = node.Data
		return ptr
	}
	return tx.PageAppend(node)
}

// PageDel marks a page as freed; it will be added to the free list on commit.
func (tx *KVTX) PageDel(ptr uint64) {
	tx.page.updates[ptr] = nil
}

// PageAppend allocates a brand-new page beyond the current file end.
// Used by both PageNew (overflow) and the FreeList (via btree.FreeListStore).
func (tx *KVTX) PageAppend(node btree.BNode) uint64 {
	assert(len(node.Data) <= btree.PageSize)
	ptr := tx.kv.page.flushed + uint64(tx.page.nappend)
	tx.page.nappend++
	tx.page.updates[ptr] = node.Data
	return ptr
}

// PageUse rewrites an existing page in-place (used by FreeList to recycle its
// own nodes without going through the free list again).
func (tx *KVTX) PageUse(ptr uint64, node btree.BNode) {
	tx.page.updates[ptr] = node.Data
}

// --- btree.FreeListStore: KVTX wires itself as the store for FreeList ---
// PageGet is already provided above.
// PageAppend and PageUse are provided above.

// --- kv.Writer interface ---

// Update inserts or updates a key. Returns true if a new key was created.
func (tx *KVTX) Update(req *btree.InsertReq) bool {
	tx.tree.InsertEx(req)
	return req.Added
}

// Del deletes a key. Returns true if the key existed.
func (tx *KVTX) Del(req *btree.DeleteReq) bool {
	return tx.tree.DeleteEx(req)
}

// --- transaction lifecycle ---

// Begin opens a new write transaction.  The writer mutex is held until
// Commit or Abort is called.
func (kv *KV) Begin(tx *KVTX) {
	tx.kv = kv
	tx.page.updates = map[uint64][]byte{}
	tx.mmap.chunks = kv.mmap.chunks

	kv.writer.Lock()
	tx.version = kv.version

	// Wire the B-tree to this transaction's page store.
	tx.tree.Root = kv.tree.root
	tx.tree.Store = tx

	// Determine the oldest active reader so the free list knows which pages
	// are safe to reuse.
	minReader := kv.version
	kv.mu.Lock()
	if len(kv.readers) > 0 {
		minReader = kv.readers[0].version
	}
	kv.mu.Unlock()

	// Wire the free list.
	tx.free = btree.NewFreeList(kv.free, kv.version, minReader, tx)

	assert(tx.page.nappend == 0 && len(tx.page.updates) == 0)
}

// Commit persists the transaction and releases the writer lock.
func (kv *KV) Commit(tx *KVTX) error {
	assert(!tx.done)
	tx.done = true
	defer kv.writer.Unlock()

	if kv.tree.root == tx.tree.Root {
		return nil // nothing changed
	}

	// 1. Collect freed pages and update the freelist.
	freed := make([]uint64, 0, len(tx.page.updates))
	for ptr, page := range tx.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	slices.Sort(freed)
	tx.free.Add(freed)

	// 2. Write modified pages into the mmap so readers can see them.
	newFlushed := kv.page.flushed + uint64(tx.page.nappend)
	db := tx.kv
	npages := int(db.page.flushed) + tx.page.nappend
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}
	tx.mmap.chunks = db.mmap.chunks
	for ptr, page := range tx.page.updates {
		if page != nil {
			src := page
			dst := pageGetMapped(tx.mmap.chunks, ptr).Data
			copy(dst, src)
		}
	}

	// 3. Write the transaction to the WAL for crash recovery.
	if err := kv.wal.BeginTX(kv.version); err != nil {
		return fmt.Errorf("WAL begin: %w", err)
	}
	for ptr, page := range tx.page.updates {
		if page == nil {
			continue
		}
		if err := kv.wal.PageData(kv.version, ptr, page); err != nil {
			return fmt.Errorf("WAL page data: %w", err)
		}
	}
	if err := kv.wal.CommitTX(kv.version, commitState{
		Root:        tx.tree.Root,
		FreeHead:    tx.free.FreeListData.Head,
		PageFlushed: newFlushed,
	}); err != nil {
		return fmt.Errorf("WAL commit: %w", err)
	}

	// 4. fsync the WAL so the commit is durable (main DB fsync deferred to checkpoint).
	if !kv.NoSync {
		if err := kv.wal.Sync(); err != nil {
			return fmt.Errorf("WAL fsync: %w", err)
		}
	}

	// 5. Publish the new in-memory state so subsequent reads see it.
	kv.page.flushed = newFlushed
	kv.free = tx.free.FreeListData
	kv.mu.Lock()
	kv.tree.root = tx.tree.Root
	kv.version++
	kv.mu.Unlock()

	// 6. Write the master page (no fsync) so other sessions can open the DB
	// without needing WAL recovery. On next open, if WAL has data it will be
	// recovered; otherwise the master page is the sole source of truth.
	if err := masterStore(kv); err != nil {
		return fmt.Errorf("commit master store: %w", err)
	}
	return nil
}

// Abort rolls back the transaction and releases the writer lock.
func (kv *KV) Abort(tx *KVTX) {
	assert(!tx.done)
	tx.done = true
	kv.writer.Unlock()
	// In-memory updates are simply abandoned; nothing was written to disk.
}


