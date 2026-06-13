package kv

import (
	"container/heap"
	"fmt"
	"slices"
	"sync"

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
	mmapMu *sync.RWMutex // shared reference to KV.mmapMu
	index  int           // position in the KV.readers heap
	done   bool          // true after EndRead
}

// BeginRead opens a new read transaction, taking a snapshot of the current
// tree root and mmap chunk list.
func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.tree.Root = kv.tree.root
	tx.tree.Store = tx // KVReader implements btree.PageStore (read-only subset)
	tx.version = kv.version
	tx.mmapMu = &kv.mmapMu
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
	tx.mmapMu.RLock()
	defer tx.mmapMu.RUnlock()
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
	readSet  map[uint64]struct{} // pages read from committed state (for OCC conflict detection)
	page     struct {
		nappend int               // number of pages appended by this tx
		updates map[uint64][]byte // nil value = page is freed; non-nil = new content
	}
	// pageCache holds copies of mmap pages read during this transaction.
	// Separate from updates to avoid treating cached reads as writes at commit time.
	pageCache map[uint64][]byte
}

// --- btree.PageStore implementation for KVTX (read + write path) ---

// PageGet returns the BNode at ptr, preferring in-transaction updates over
// the on-disk mmap copy.  Pages read from the mmap are copied into private
// memory (pageCache) so that concurrent commits (which write to the shared
// mmap) do not corrupt this transaction's snapshot.
func (tx *KVTX) PageGet(ptr uint64) btree.BNode {
	assert(ptr != 0)
	tx.readSet[ptr] = struct{}{}
	if page, ok := tx.page.updates[ptr]; ok {
		assert(page != nil)
		return btree.BNode{Data: page}
	}
	if cached, ok := tx.pageCache[ptr]; ok {
		return btree.BNode{Data: cached}
	}
	tx.kv.mmapMu.RLock()
	src := pageGetMapped(tx.mmap.chunks, ptr)
	buf := make([]byte, btree.PageSize)
	copy(buf, src.Data)
	tx.kv.mmapMu.RUnlock()
	tx.pageCache[ptr] = buf
	return btree.BNode{Data: buf}
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
// Page numbers are handed out under pageAllocMu so that concurrent writers
// each get unique page numbers.
func (tx *KVTX) PageAppend(node btree.BNode) uint64 {
	assert(len(node.Data) <= btree.PageSize)
	tx.kv.pageAllocMu.Lock()
	ptr := tx.kv.pageAlloc
	tx.kv.pageAlloc++
	tx.kv.pageAllocMu.Unlock()
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

// Begin opens a new write transaction.
// Unlike the old single-writer model, this is non-blocking — multiple KVTX
// instances may exist simultaneously. The commit phase is serialised via
// commitMu and uses OCC conflict detection.
func (kv *KV) Begin(tx *KVTX) {
	tx.kv = kv
	tx.page.updates = map[uint64][]byte{}
	tx.pageCache = map[uint64][]byte{}
	tx.readSet = map[uint64]struct{}{}
	tx.mmap.chunks = kv.mmap.chunks

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

// Commit persists the transaction using OCC.
// Under commitMu it performs conflict detection, then writes pages to the
// mmap, appends commit records to the WAL, fsyncs the WAL, and publishes
// the new in-memory state.
func (kv *KV) Commit(tx *KVTX) error {
	assert(!tx.done)
	tx.done = true

	kv.commitMu.Lock()
	defer kv.commitMu.Unlock()

	// --- OCC conflict detection ---
	// If another writer committed after this tx began (version advanced),
	// our snapshot is stale. The tree root changed, so the new root we
	// computed does not incorporate the other tx's changes. We must abort
	// to prevent lost updates.
	if tx.version != kv.version {
		return fmt.Errorf("serialisation conflict: retry transaction")
	}

	// Fast path: nothing changed.
	// Checked *after* the version guard (under commitMu) so a concurrent
	// commit that coincidentally produces the same root page number does
	// not trick us into a false match (TOCTOU race).
	if kv.tree.root == tx.tree.Root {
		return nil
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
	newFlushed := kv.page.flushed
	for ptr, page := range tx.page.updates {
		if page != nil && ptr >= newFlushed {
			newFlushed = ptr + 1
		}
	}
	db := tx.kv
	npages := int(newFlushed)
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}
	tx.mmap.chunks = db.mmap.chunks
	kv.mmapMu.Lock()
	for ptr, page := range tx.page.updates {
		if page != nil {
			src := page
			dst := pageGetMapped(tx.mmap.chunks, ptr).Data
			copy(dst, src)
		}
	}
	kv.mmapMu.Unlock()

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
	// without needing WAL recovery.
	if err := masterStore(kv); err != nil {
		return fmt.Errorf("commit master store: %w", err)
	}
	return nil
}

// Abort rolls back the transaction.
func (kv *KV) Abort(tx *KVTX) {
	assert(!tx.done)
	tx.done = true
}
