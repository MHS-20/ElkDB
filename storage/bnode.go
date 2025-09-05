package storage

import (
	"encoding/binary"
	"strconv"
)

// Assert panics with the provided message if the condition is false.
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

/*---- DISK CONSTANTS ---- */
const TYPE = 2
const NKEYS = 2
const HEADER = TYPE + NKEYS
const POINTER_SIZE = 8
const OFFSET_SIZE = 2
const KEY_LENGTH_SIZE = 2
const VAL_LENGTH_SIZE = 2
const BTREE_MAX_NODE_SIZE = 4096 //OS page size
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const n_keys = 2 //! don't chage

func init() {
	nodemax := HEADER +
		(POINTER_SIZE * n_keys) +
		(OFFSET_SIZE * n_keys) +
		(KEY_LENGTH_SIZE+VAL_LENGTH_SIZE)*n_keys +
		BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(nodemax <= BTREE_MAX_NODE_SIZE, "nodemax exceeds BTREE_MAX_NODE_SIZE") // maximum KV
}

/*----------------*/
/*---- B-TREE ----*/
/*----------------*/

type BNode []byte // ? improve

const (
	BTREE_NODE = 1 // internal nodes
	BTREE_LEAF = 2 // leaf nodes
)

/* --- HEADER APIs --- */
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

/* --- POINTERS APIs --- */
func (node BNode) getPointer(idx uint16) uint64 {
	assert(idx < node.nkeys(), "idx out of nkeys range: "+strconv.Itoa(int(idx))+"<"+strconv.Itoa(int(node.nkeys())))
	loc := HEADER + POINTER_SIZE*idx
	return binary.LittleEndian.Uint64(node[loc:])
}

func (node BNode) setPointer(idx uint16, val uint64) {
	assert(idx < node.nkeys(), "idx out of nkeys range: "+strconv.Itoa(int(idx))+"<"+strconv.Itoa(int(node.nkeys())))
	loc := HEADER + POINTER_SIZE*idx
	binary.LittleEndian.PutUint64(node[loc:], val)
}

/* --- OFFSETS APIs --- */
func offsetLocation(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys(), "idx out of nkeys range "+strconv.Itoa(int(idx))+"<"+strconv.Itoa(int(node.nkeys())))
	return HEADER + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetLocation(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetLocation(node, idx):], offset)
}

/* --- KV APIs --- */
func (node BNode) kvLocation(idx uint16) uint16 {
	assert(idx <= node.nkeys(), "idx out of nkeys range: "+strconv.Itoa(int(idx))+"<"+strconv.Itoa(int(node.nkeys())))
	return HEADER + (POINTER_SIZE * node.nkeys()) + (OFFSET_SIZE * node.nkeys()) + node.getOffset(idx)

}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys(), "idx out of nkeys range: "+strconv.Itoa(int(idx))+" "+strconv.Itoa(int(node.nkeys())))
	loc := node.kvLocation(idx)
	klen := binary.LittleEndian.Uint16(node[loc:])
	return node[loc+(KEY_LENGTH_SIZE+VAL_LENGTH_SIZE):][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys(), "idx out of nkeys range: "+strconv.Itoa(int(idx))+" "+strconv.Itoa(int(node.nkeys())))
	loc := node.kvLocation(idx)
	klen := binary.LittleEndian.Uint16(node[loc:])
	vlen := binary.LittleEndian.Uint16(node[loc+KEY_LENGTH_SIZE:])
	return node[loc+KEY_LENGTH_SIZE+VAL_LENGTH_SIZE+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvLocation(node.nkeys())
}
