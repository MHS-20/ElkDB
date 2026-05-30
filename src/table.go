package elk

import (
	"sync"
)

type DB struct {
	Path string
	// internals
	kv     KV
	mu     sync.Mutex
	tables map[string]*TableDef // cached table definition
}

// read-only DB transactions
type DBReader struct {
	kv KVTX // contains the KVReader
	db *DB
}

func (db *DB) BeginRead(tx *DBReader) {
	tx.db = db
	db.kv.BeginRead(&tx.kv.KVReader)
}

func (db *DB) EndRead(tx *DBReader) {
	db.kv.EndRead(&tx.kv.KVReader)
}

// DB transactions
type DBTX struct {
	DBReader
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.kv.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(&tx.kv)
}

// table definition
type TableDef struct {
	// user defined
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the primary key
	Indexes [][]string
	// auto-assigned B-tree key prefixes for different tables/indexes
	Prefix        uint32
	IndexPrefixes []uint32
}

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}
