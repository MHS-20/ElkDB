package btree

import "encoding/binary"

// FreeListData is the serialisable, snapshot-able part of the free list.
// kv copies this into each transaction so changes can be committed atomically.
type FreeListData struct {
	Head uint64
	// Cached pointers to list nodes, ordered from tail to head.
	nodes []uint64
	// Cached total number of free pages; also stored in the head node on disk.
	total int
	// Number of already-consumed entries in the current tail node.
	offset int
}

// FreeList manages the on-disk free page list for a single write transaction.
// Read access to pages goes through a FreeListStore; the list itself never
// performs direct mmap or file I/O.
type FreeList struct {
	FreeListData
	version   uint64   // version of the current transaction
	minReader uint64   // oldest reader version (pages freed after this are unsafe to reuse)
	freed     []uint64 // pages queued for release by the current transaction
	store     FreeListStore
}

// NewFreeList wires the store into a FreeList ready for use in a transaction.
func NewFreeList(data FreeListData, version, minReader uint64, store FreeListStore) *FreeList {
	return &FreeList{
		FreeListData: data,
		version:      version,
		minReader:    minReader,
		store:        store,
	}
}

// --- node format ---
// | type | size | total | next |  pointer-version-pairs |
// |  2B  |  2B  |   8B  |  8B  |       size * 16B       |

const (
	BNodeFreeList  = 3
	freeListHeader = 4 + 8 + 8
	FreeListCap    = (PageSize - freeListHeader) / 16
)

func flTotal(fl *FreeList) int {
	if fl.Head == 0 {
		return 0
	}
	node := fl.store.PageGet(fl.Head)
	return int(binary.LittleEndian.Uint64(node.Data[4:]))
}

func flnSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node.Data[2:]))
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.Data[12:])
}

func flnItem(node BNode, idx int) (ptr uint64, ver uint64) {
	offset := freeListHeader + 16*idx
	ptr = binary.LittleEndian.Uint64(node.Data[offset+0:])
	ver = binary.LittleEndian.Uint64(node.Data[offset+8:])
	return
}

func flnSetItem(node BNode, idx int, ptr uint64, ver uint64) {
	assert(idx < flnSize(node))
	offset := freeListHeader + 16*idx
	binary.LittleEndian.PutUint64(node.Data[offset+0:], ptr)
	binary.LittleEndian.PutUint64(node.Data[offset+8:], ver)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.Data[0:], BNodeFreeList)
	binary.LittleEndian.PutUint16(node.Data[2:], size)
	binary.LittleEndian.PutUint64(node.Data[4:], 0)
	binary.LittleEndian.PutUint64(node.Data[12:], next)
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.Data[4:], total)
}

// --- cache loading ---

func (fl *FreeList) loadCache() {
	if fl.Head == 0 || len(fl.nodes) > 0 {
		return
	}

	fl.total = flTotal(fl)

	head := fl.Head
	remain := fl.total
	for remain > 0 {
		node := fl.store.PageGet(head)
		fl.nodes = append(fl.nodes, head)
		remain -= flnSize(node)
		head = flnNext(node)
	}

	// reverse: fl.nodes[0] is the tail, fl.nodes[last] is the head
	for i, j := 0, len(fl.nodes)-1; i < j; i, j = i+1, j-1 {
		fl.nodes[i], fl.nodes[j] = fl.nodes[j], fl.nodes[i]
	}

	fl.offset = -remain
}

// --- public API ---

// Pop removes and returns one page pointer from the tail of the list.
// Returns 0 if no page is available (list empty, or all remaining pages are
// still reachable by a concurrent reader).
func (fl *FreeList) Pop() uint64 {
	fl.loadCache()
	return flPop1(fl)
}

func versionBefore(a, b uint64) bool {
	return int64(a-b) < 0
}

func flPop1(fl *FreeList) uint64 {
	if fl.total == 0 {
		return 0
	}

	assert(fl.offset < flnSize(fl.store.PageGet(fl.nodes[0])))
	ptr, ver := flnItem(fl.store.PageGet(fl.nodes[0]), fl.offset)
	if versionBefore(fl.minReader, ver) {
		return 0 // still reachable by a reader
	}
	fl.offset++
	fl.total--

	for len(fl.nodes) > 0 && fl.offset == flnSize(fl.store.PageGet(fl.nodes[0])) {
		fl.offset = 0
		fl.freed = append(fl.freed, fl.nodes[0]) // recycle the node page itself
		fl.nodes = fl.nodes[1:]
		if len(fl.nodes) == 0 {
			fl.Head = 0
		}
	}
	return ptr
}

func flRemoveHead(fl *FreeList) (prepend []uint64, version []uint64) {
	node := fl.store.PageGet(fl.Head)
	sz := flnSize(node)

	start := 0
	if len(fl.nodes) == 1 {
		start = fl.offset
	}
	fl.total -= sz - start

	for ; start < sz; start++ {
		ptr, ver := flnItem(node, start)
		prepend = append(prepend, ptr)
		version = append(version, ver)
	}

	fl.nodes = fl.nodes[:len(fl.nodes)-1]
	if len(fl.nodes) > 0 {
		fl.Head = fl.nodes[len(fl.nodes)-1]
	} else {
		fl.Head = 0
		fl.offset = 0
	}
	return
}

// Add finalises the transaction's free-list update.
// freed is the list of pages the current transaction is releasing.
// After Add returns, FreeListData contains the updated head pointer that must
// be written to the master page.
func (fl *FreeList) Add(freed []uint64) {
	assert(fl.Head == 0 || len(fl.nodes) > 0)
	fl.freed = append(freed, fl.freed...)

	if len(fl.freed) > 0 {
		// Try to recycle existing free-list node pages rather than appending
		// fresh pages for the new head nodes.
		var reuse []uint64
		for fl.total > 0 {
			remain := flnSize(fl.store.PageGet(fl.Head))
			if len(fl.nodes) == 1 {
				remain -= fl.offset
			}
			if len(reuse)*FreeListCap >= remain+len(fl.freed)+1 {
				break
			}
			ptr := flPop1(fl)
			if ptr == 0 {
				break
			}
			reuse = append(reuse, ptr)
		}

		var version []uint64
		if fl.total > 0 {
			fl.freed = append(fl.freed, fl.Head)
			var prepend []uint64
			prepend, version = flRemoveHead(fl)
			fl.freed = append(prepend, fl.freed...)
		}

		flPush(fl, fl.freed, version, reuse)
	}

	if fl.Head != 0 {
		flnSetTotal(fl.store.PageGet(fl.Head), uint64(fl.total))
	}
}

func flPush(fl *FreeList, freed []uint64, version []uint64, reuse []uint64) {
	fl.total += len(freed)
	for len(freed) > 0 {
		node := BNode{make([]byte, PageSize)}

		size := len(freed)
		if size > FreeListCap {
			size = FreeListCap
		}
		flnSetHeader(node, uint16(size), fl.Head)
		for i, ptr := range freed[:size] {
			ver := fl.version + 1
			if len(version) > 0 {
				ver, version = version[0], version[1:]
			}
			flnSetItem(node, i, ptr, ver)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			fl.Head, reuse = reuse[0], reuse[1:]
			fl.store.PageUse(fl.Head, node)
		} else {
			fl.Head = fl.store.PageAppend(node)
		}
		fl.nodes = append(fl.nodes, fl.Head)
	}

	if len(reuse) > 0 {
		// edge case: one recycled slot left over with nothing to store in it
		assert(len(reuse) == 1)
		node := BNode{make([]byte, PageSize)}
		flnSetHeader(node, 0, fl.Head)
		fl.Head = reuse[0]
		fl.store.PageUse(fl.Head, node)
		fl.nodes = append(fl.nodes, fl.Head)
	}
}
