package btree

// PageStore is the interface BTree requires from its storage backend.
// kv.KVTX implements all three methods.
//
// The btree package has no knowledge of files, mmap, or transactions;
// it only knows how to ask for pages by number and how to allocate/free them.
type PageStore interface {
	// PageGet returns the node stored at the given page number.
	PageGet(ptr uint64) BNode
	// PageNew persists a new node and returns its page number.
	PageNew(node BNode) uint64
	// PageDel marks a page as free (to be reclaimed by the free list).
	PageDel(ptr uint64)
}

// FreeListStore is the interface FreeList requires from its storage backend.
// It extends PageStore with PageUse, which rewrites an existing page in-place
// (used when the free list recycles its own nodes).
// kv.KVTX implements all four methods.
type FreeListStore interface {
	PageGet(ptr uint64) BNode
	// PageAppend allocates a brand-new page beyond the current file end.
	// FreeList uses this instead of PageNew so it does not recurse into itself.
	PageAppend(node BNode) uint64
	// PageUse rewrites an already-allocated page without going through the
	// free list (used to recycle free-list nodes themselves).
	PageUse(ptr uint64, node BNode)
}
