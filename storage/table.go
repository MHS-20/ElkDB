package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type DB struct {
	Path   string
	kv     KV
	tables map[string]*TableDef // cached table definition
}

type TableDef struct {
	Name   string
	Types  []uint32 // column types
	Cols   []string // column names
	PKeys  int      // the first PKeys columns are the primary key
	Prefix uint32   // key prefixes for different tables in the same B-tree
}

// Table cells supports only int64 and strings
const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

/*--------- INTERNAL TABLES ---------*/
// metadata internal table
// used to store the next available table prefix
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// table schemas internal table
// serialized table definitions
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var INTERNAL_TABLES map[string]*TableDef = map[string]*TableDef{
	"@meta":  TDEF_META,
	"@table": TDEF_TABLE,
}

const TABLE_PREFIX_MIN = 100

/*--------- RECORD ---------*/
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

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, c := range rec.Cols {
		if c == key {
			return &rec.Vals[i]
		}
	}
	return nil
}

// rearrange a record to match the column order defined in the table definition
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	assert(len(rec.Cols) == len(rec.Vals), "cols number doesn't match values number")
	out := make([]Value, len(tdef.Cols))
	for i, c := range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue // leave column uninitialized
		}
		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] = *v
	}
	return out, nil
}

// check that values match the expected number of columns
func valuesComplete(tdef *TableDef, vals []Value, n int) error {
	for i, v := range vals {
		if i < n && v.Type == 0 {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != 0 {
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}
	return nil
}

// reorder a record and check number of columns
// n == tdef.PKeys: record is a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	vals, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}
	err = valuesComplete(tdef, vals, n)
	if err != nil {
		return nil, err
	}
	return vals, nil
}

/*--------- ENCODING ---------*/
// Strings are encoded as null-terminated strings,
// therefore is needed to escape the null byte
// 0x00 → 0x01 0x01
// 0x01 → 0x01 0x02
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 { // no bytes to escape
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, currentByte := range in {
		if currentByte <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = currentByte + 1
			pos += 2
		} else {
			out[pos] = currentByte
			pos += 1
		}
	}
	return out
}

// decodes a string previously encoded with escapeString
func unescapeString(in []byte) []byte {
	if bytes.Count(in, []byte{1}) == 0 {
		return in
	}
	out := make([]byte, len(in))
	pos := 0
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			i++
			assert(in[i] >= 1, "bad escape sequence")
			out[pos] = in[i] - 1
		} else {
			out[pos] = in[i]
		}
		pos++
	}
	return out[:pos]
}

// order-preserving encoding
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("unknown type during encoding")
		}
	}
	return out
}

// primary keys encoding: prefix + encoded values
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

// already know the types of the values
func decodeValues(in []byte, out []Value) {
	for i := range out {
		switch out[i].Type {
		case TYPE_INT64:
			u := binary.BigEndian.Uint64(in[:8])
			out[i].I64 = int64(u - (1 << 63))
			in = in[8:]
		case TYPE_BYTES:
			idx := bytes.IndexByte(in, 0)
			assert(idx >= 0, "bad string encoding")
			out[i].Str = unescapeString(in[:idx])
			in = in[idx+1:]
		default:
			panic("what?")
		}
	}
	assert(len(in) == 0, "extra bytes after decoding")
}

/*--------- DB OPERATIONS ---------*/
// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)
	return true, nil
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.Update(key, val, mode)
}

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}

// verify validity of table definition
func tableDefCheck(tdef *TableDef) error {
	bad := tdef.Name == "" || len(tdef.Cols) == 0
	bad = bad || len(tdef.Cols) != len(tdef.Types)
	bad = bad || !(1 <= tdef.PKeys && int(tdef.PKeys) <= len(tdef.Cols))
	if bad {
		return fmt.Errorf("bad table definition: %s", tdef.Name)
	}
	return nil
}

/*--------- INTERNAL TABLES OPERATIONS ---------*/
// get the table definition by name from the internal table
func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil, "meta get failed")
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil, "json unmarshal failed")
	return tdef
}

// get the table definition by name
func getTableDef(db *DB, name string) *TableDef {
	if tdef, ok := INTERNAL_TABLES[name]; ok {
		return tdef // expose internal tables
	}
	// check the cached definitions
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		// if not in cache, load the definition from the internal table
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef // update cache
		}
	}
	return tdef
}

/*------------ PUBLIC DB INTERFACE ----------*/
func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check if it already exists in the internal table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	assert(err == nil, "meta get failed")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	assert(tdef.Prefix == 0, "table prefix must be 0")
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	assert(err == nil, "meta get failed")
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN, "bad next_prefix value")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	assert(err == nil, "json marshal failed")
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *DB) Open() error {
	db.kv.Path = db.Path
	return db.kv.Open()
}

func (db *DB) Close() {
	db.kv.Close()
}
