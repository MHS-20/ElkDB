package tables

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/MHS-20/ElkDB/btree"
)

// ---------------------------------------------------------------------------
// Single-row read
// ---------------------------------------------------------------------------

// dbGet fetches one row by its primary key using a point-scan.
// On success it fills rec with the full row; on a miss it returns (false, nil).
func dbGet(tx *DBReader, tdef *TableDef, rec *Record) (bool, error) {
	sc := Scanner{
		Cmp1: btree.CmpGE,
		Cmp2: btree.CmpLE,
		Key1: *rec,
		Key2: *rec,
	}
	if err := dbScan(tx, tdef, &sc); err != nil {
		return false, err
	}
	if sc.Valid() {
		sc.Deref(rec)
		sc.Next()
		assert(!sc.Valid()) // a complete primary key must match at most one row
		return true, nil
	}
	return false, nil
}

// Get fetches one row from table by its primary key.
// rec must contain at least the primary-key columns on entry; on success it
// is rewritten with the full row.
func (tx *DBReader) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	rec.Cols = tdef.Cols[:tdef.PKeys]
	rec.Vals = values[:tdef.PKeys]
	return dbGet(tx, tdef, rec)
}

// ---------------------------------------------------------------------------
// Write helpers
// ---------------------------------------------------------------------------

// DBSetReq carries the inputs and outputs of a Set operation.
type DBSetReq struct {
	Record  Record
	Mode    int // btree.ModeUpsert / ModeUpdateOnly / ModeInsertOnly
	Updated bool
	Added   bool
}

const (
	indexAdd = 1
	indexDel = 2
)

// indexOp adds or removes a secondary index entry for rec.
func indexOp(tx *DBTX, tdef *TableDef, rec Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Value, len(tdef.Cols))
	for i, index := range tdef.Indexes {
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}
		key = encodeKey(key[:0], tdef.IndexPrefixes[i], irec[:len(index)])
		assert(len(key) <= btree.MaxKeySize)
		var done bool
		switch op {
		case indexAdd:
			done = tx.kvw.Update(&btree.InsertReq{Key: key, Mode: btree.ModeUpsert})
		case indexDel:
			done = tx.kvw.Del(&btree.DeleteReq{Key: key})
		default:
			panic("indexOp: unknown op")
		}
		assert(done)
	}
}

// dbUpdate writes one row to tdef, maintaining secondary indexes.
func dbUpdate(tx *DBTX, tdef *TableDef, dbreq *DBSetReq) error {
	values, err := checkRecord(tdef, dbreq.Record, len(tdef.Cols))
	if err != nil {
		return err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])

	if len(key) > btree.MaxKeySize {
		return fmt.Errorf("primary key too large: %d bytes (max %d)", len(key), btree.MaxKeySize)
	}
	if len(val) > btree.MaxValSize {
		return fmt.Errorf("value too large: %d bytes (max %d)", len(val), btree.MaxValSize)
	}

	req := btree.InsertReq{Key: key, Val: val, Mode: dbreq.Mode}
	tx.kvw.Update(&req)
	dbreq.Added, dbreq.Updated = req.Added, req.Updated
	if !req.Updated || len(tdef.Indexes) == 0 {
		return nil
	}

	// Update secondary indexes.
	if req.Updated && !req.Added {
		// The row already existed: remove the old index entries first.
		decodeValues(req.Old, values[tdef.PKeys:])
		indexOp(tx, tdef, Record{tdef.Cols, values}, indexDel)
	}
	if req.Updated {
		indexOp(tx, tdef, dbreq.Record, indexAdd)
	}
	return nil
}

// dbDelete removes one row from tdef, maintaining secondary indexes.
func dbDelete(tx *DBTX, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	if len(key) > btree.MaxKeySize {
		return false, fmt.Errorf("primary key too large: %d bytes (max %d)", len(key), btree.MaxKeySize)
	}

	req := btree.DeleteReq{Key: key}
	deleted := tx.kvw.Del(&req)
	if !deleted || len(tdef.Indexes) == 0 {
		return deleted, nil
	}

	// Recover the non-key column types so decodeValues knows how to decode.
	for i := tdef.PKeys; i < len(tdef.Types); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(req.Old, values[tdef.PKeys:])
	indexOp(tx, tdef, Record{tdef.Cols, values}, indexDel)
	return true, nil
}

// ---------------------------------------------------------------------------
// Public write API on DBTX
// ---------------------------------------------------------------------------

// Set writes a row to table, using the mode specified in req.
func (tx *DBTX) Set(table string, req *DBSetReq) error {
	tdef := getTableDef(&tx.DBReader, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(tx, tdef, req)
}

// Insert adds a new row. Returns (true, nil) if the row was inserted,
// (false, nil) if the primary key already existed.
func (tx *DBTX) Insert(table string, rec Record) (bool, error) {
	req := DBSetReq{Record: rec, Mode: btree.ModeInsertOnly}
	err := tx.Set(table, &req)
	return req.Added, err
}

// Update modifies an existing row. Returns (true, nil) if the row existed and
// was updated, (false, nil) if the primary key was not found.
func (tx *DBTX) Update(table string, rec Record) (bool, error) {
	req := DBSetReq{Record: rec, Mode: btree.ModeUpdateOnly}
	err := tx.Set(table, &req)
	return req.Updated, err
}

// Upsert inserts or replaces a row. Returns (true, nil) if a new row was
// created, (false, nil) if an existing row was replaced.
func (tx *DBTX) Upsert(table string, rec Record) (bool, error) {
	req := DBSetReq{Record: rec, Mode: btree.ModeUpsert}
	err := tx.Set(table, &req)
	return req.Added, err
}

// Delete removes a row by its primary key. Returns (true, nil) if the row was
// found and deleted.
func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(&tx.DBReader, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(tx, tdef, rec)
}

// ---------------------------------------------------------------------------
// Table creation
// ---------------------------------------------------------------------------

// TableNew creates a new user table. Returns an error if a table with the same
// name already exists or if the definition is invalid.
func (tx *DBTX) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// Reject duplicates.
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(&tx.DBReader, tdefTable, table)
	assert(err == nil)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// Assign a fresh prefix for the primary key tree.
	assert(tdef.Prefix == 0)
	tdef.Prefix = tablePrefixMin
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(&tx.DBReader, tdefMeta, meta)
	assert(err == nil)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > tablePrefixMin)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// Assign a prefix for each secondary index.
	for i := range tdef.Indexes {
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, tdef.Prefix+1+uint32(i))
	}

	// Advance the next-prefix counter.
	ntree := 1 + uint32(len(tdef.Indexes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+ntree)
	if err := dbUpdate(tx, tdefMeta, &DBSetReq{Record: *meta}); err != nil {
		return err
	}

	// Persist the definition.
	if tdef.Indexes == nil {
		tdef.Indexes = [][]string{}
	}
	if tdef.IndexPrefixes == nil {
		tdef.IndexPrefixes = []uint32{}
	}
	val, err := json.Marshal(tdef)
	assert(err == nil)
	table.AddStr("def", val)
	return dbUpdate(tx, tdefTable, &DBSetReq{Record: *table})
}
