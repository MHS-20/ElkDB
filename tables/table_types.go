// Package tables builds a relational layer on top of the kv store.
// It manages table schemas, encodes rows as ordered byte keys, and maintains
// secondary indexes. Each operation runs inside a transaction,
// so that schema changes and data mutations are always atomic.
package tables

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/MHS-20/ElkDB/kv"
)

// ---------------------------------------------------------------------------
// DB handle
// ---------------------------------------------------------------------------

// DB is the top-level relational database handle.
// Open it with DB.Open, then create transactions with Begin / BeginRead.
type DB struct {
	Path string
	// internals
	kv     kv.KV
	mu     sync.Mutex
	tables map[string]*TableDef // cache of table definitions loaded from disk
}

func (db *DB) Open() error {
	db.kv.Path = db.Path
	return db.kv.Open()
}

func (db *DB) Close() {
	db.kv.Close()
}

// ---------------------------------------------------------------------------
// Transaction types
// ---------------------------------------------------------------------------

// DBReader is a read-only snapshot transaction.
// It satisfies the table.Reader interface.
type DBReader struct {
	db   *DB
	kvr  kv.Reader    // snapshot; either a *kv.KVReader or the read face of a *kv.KVTX
	kvtx *kv.KVReader // non-nil only for stand-alone read transactions (BeginRead)
}

// BeginRead opens a read-only transaction.
func (db *DB) BeginRead(tx *DBReader) {
	tx.db = db
	r := &kv.KVReader{}
	db.kv.BeginRead(r)
	tx.kvtx = r
	tx.kvr = r
}

// EndRead closes a read-only transaction.
func (db *DB) EndRead(tx *DBReader) {
	db.kv.EndRead(tx.kvtx)
}

// DBTX is a read-write transaction.
// It satisfies both table.Reader and table.Writer interfaces.
type DBTX struct {
	db       *DB
	kvw      kv.Writer // the underlying kv write transaction
	DBReader           // embedded for the Reader methods; kvr is wired to kvw
}

// Begin opens a read-write transaction.
func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	w := &kv.KVTX{}
	db.kv.Begin(w)
	tx.kvw = w
	// Wire the embedded DBReader so that read methods (Get, Scan, TableDef)
	// see in-transaction writes via the same kv.Writer.
	tx.DBReader.db = db
	tx.kvr = w
	tx.kvtx = nil // not a standalone read tx; EndRead must not be called
}

// Commit persists the transaction.
func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(tx.kvw.(*kv.KVTX))
}

// Abort rolls back the transaction.
func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(tx.kvw.(*kv.KVTX))
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

// TableDef describes the structure of a table.
type TableDef struct {
	// user-defined
	Name    string
	Types   []uint32   // column types (one entry per column)
	Cols    []string   // column names
	PKeys   int        // the first PKeys columns form the primary key
	Indexes [][]string // each entry is an ordered list of column names
	// auto-assigned by TableNew
	Prefix        uint32   // B-tree key prefix for the primary key
	IndexPrefixes []uint32 // B-tree key prefixes for each secondary index
}

// Column type constants.
const (
	TypeUnknown = uint32(0)
	TypeBytes   = uint32(1)
	TypeInt64   = uint32(2)
)

// Value is a single typed column value.
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Record is a table row: a parallel list of column names and values.
type Record struct {
	Cols []string
	Vals []Value
}

// AddStr appends a bytes-typed column to the record and returns the record for
// chaining.
func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TypeBytes, Str: val})
	return rec
}

// AddInt64 appends an int64-typed column to the record and returns the record
// for chaining.
func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TypeInt64, I64: val})
	return rec
}

// Get returns a pointer to the value for the named column, or nil if not
// present.
func (rec *Record) Get(col string) *Value {
	for i, c := range rec.Cols {
		if c == col {
			return &rec.Vals[i]
		}
	}
	return nil
}

// ColIndex returns the position of col in tdef.Cols, or -1 if not found.
// Exported so the ql package can use it without reaching into table internals.
func ColIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Internal (system) table definitions
// ---------------------------------------------------------------------------

// tdefMeta stores arbitrary key-value metadata (e.g. the next_prefix counter).
var tdefMeta = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// tdefTable stores the JSON-serialised definition of every user table.
var tdefTable = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var internalTables = map[string]*TableDef{
	"@meta":  tdefMeta,
	"@table": tdefTable,
}

// ---------------------------------------------------------------------------
// Schema validation
// ---------------------------------------------------------------------------

const tablePrefixMin = uint32(100)

func tableDefCheck(tdef *TableDef) error {
	bad := tdef.Name == "" || len(tdef.Cols) == 0
	bad = bad || len(tdef.Cols) != len(tdef.Types)
	bad = bad || (1 > tdef.PKeys || tdef.PKeys > len(tdef.Cols))
	if bad {
		return fmt.Errorf("bad table definition: %s", tdef.Name)
	}
	for i, index := range tdef.Indexes {
		index, err := checkIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	seen := map[string]bool{}
	for _, c := range index {
		if ColIndex(tdef, c) < 0 {
			return nil, fmt.Errorf("unknown index column: %s", c)
		}
		if seen[c] {
			return nil, fmt.Errorf("duplicated column in index: %s", c)
		}
		seen[c] = true
	}
	// append any primary-key columns not already in the index
	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !seen[c] {
			index = append(index, c)
		}
	}
	assert(len(index) < len(tdef.Cols))
	return index, nil
}

// ---------------------------------------------------------------------------
// Record validation helpers
// ---------------------------------------------------------------------------

// reorderRecord rearranges rec's values into the canonical column order
// defined by tdef, returning a slice parallel to tdef.Cols.
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	assert(len(rec.Cols) == len(rec.Vals))
	out := make([]Value, len(tdef.Cols))
	for i, c := range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue // column absent; caller decides if that's an error
		}
		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] = *v
	}
	return out, nil
}

// valuesComplete checks that the first n columns in vals are non-zero (present)
// and the remaining columns are zero (absent).
func valuesComplete(tdef *TableDef, vals []Value, n int) error {
	for i, v := range vals {
		if i < n && v.Type == TypeUnknown {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != TypeUnknown {
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}
	return nil
}

// checkRecord reorders rec and verifies that exactly the first n columns are
// present. n == tdef.PKeys means "primary key only"; n == len(tdef.Cols) means
// "all columns".
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	vals, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}
	if err := valuesComplete(tdef, vals, n); err != nil {
		return nil, err
	}
	return vals, nil
}

// checkRecordTypes verifies that every column in rec matches the declared type.
func checkRecordTypes(tdef *TableDef, rec Record) error {
	for i, c := range rec.Cols {
		j := ColIndex(tdef, c)
		if j < 0 || tdef.Types[j] != rec.Vals[i].Type {
			return fmt.Errorf("bad column: %s", c)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Key encoding / decoding
// ---------------------------------------------------------------------------

// escapeString makes a byte slice safe for use as a null-terminated key
// component:
//  1. Null bytes are escaped so the encoded string contains no null bytes.
//  2. A leading 0xff or 0xfe byte is escaped so that lexicographic ordering is
//     preserved (0xff is reserved as the "maximum" sentinel).
func escapeString(in []byte) []byte {
	first := len(in) > 0 && in[0] >= 0xfe
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if !first && zeros+ones == 0 {
		return in
	}

	nescape := zeros + ones
	if first {
		nescape++
	}
	out := make([]byte, len(in)+nescape)

	pos := 0
	if first {
		out[0] = 0xfe
		out[1] = in[0]
		pos += 2
		in = in[1:]
	}
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos++
		}
	}
	return out
}

func unescapeString(in []byte) []byte {
	assert(len(in) == 0 || in[0] != 0xff)
	first := len(in) > 0 && in[0] == 0xfe
	if !first && bytes.Count(in, []byte{1}) == 0 {
		return in
	}

	out := make([]byte, len(in))
	pos := 0
	if first {
		out[0] = in[1]
		pos++
		in = in[2:]
	}
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			i++
			assert(in[i] >= 1)
			out[pos] = in[i] - 1
		} else {
			out[pos] = in[i]
		}
		pos++
	}
	return out[:pos]
}

// encodeValues appends the order-preserving encoding of vals to out.
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var buf [8]byte
			// Bias by 1<<63 so that the unsigned encoding is order-preserving
			// for signed integers.
			binary.BigEndian.PutUint64(buf[:], uint64(v.I64)+(1<<63))
			out = append(out, buf[:]...)
		case TypeBytes:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null terminator
		default:
			panic("encodeValues: unknown type")
		}
	}
	return out
}

// encodeKey prepends a 4-byte big-endian prefix to the encoded values.
// Used for both primary keys and index keys.
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	return encodeValues(out, vals)
}

// decodeValues decodes a sequence of encoded values in-place into out.
// out[i].Type must be pre-set to the expected type before calling.
func decodeValues(in []byte, out []Value) {
	for i := range out {
		switch out[i].Type {
		case TypeInt64:
			u := binary.BigEndian.Uint64(in[:8])
			out[i].I64 = int64(u - (1 << 63))
			in = in[8:]
		case TypeBytes:
			idx := bytes.IndexByte(in, 0)
			assert(idx >= 0)
			out[i].Str = unescapeString(in[:idx:idx])
			in = in[idx+1:]
		default:
			panic("decodeValues: unknown type")
		}
	}
	assert(len(in) == 0)
}

// ---------------------------------------------------------------------------
// Table definition cache
// ---------------------------------------------------------------------------

// TableDef retrieves the definition of the named table, consulting the
// in-memory cache first, then the on-disk @table system table.
// Returns nil if the table does not exist.
// This method is exported so the ql package can call it via the Reader
// interface without accessing unexported internals.
func (tx *DBReader) TableDef(name string) *TableDef {
	return getTableDef(tx, name)
}

func getTableDef(tx *DBReader, name string) *TableDef {
	if tdef, ok := internalTables[name]; ok {
		return tdef
	}

	db := tx.db
	db.mu.Lock()
	tdef, ok := db.tables[name]
	db.mu.Unlock()

	if !ok {
		tdef = getTableDefFromDisk(tx, name)
		db.mu.Lock()
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		if tdef != nil {
			db.tables[name] = tdef
		}
		db.mu.Unlock()
	}
	return tdef
}

func getTableDefFromDisk(tx *DBReader, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(tx, tdefTable, rec)
	assert(err == nil)
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil)
	return tdef
}

// ---------------------------------------------------------------------------
// Shared assert
// ---------------------------------------------------------------------------

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}
