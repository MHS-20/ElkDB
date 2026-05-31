package tables

import (
	"fmt"
	"reflect"

	"github.com/MHS-20/ElkDB/btree"
)

// ---------------------------------------------------------------------------
// Scanner — cursor for range queries
// ---------------------------------------------------------------------------

// Scanner holds the state for an ongoing range query.
// Callers initialise Cmp1, Cmp2, Key1, and Key2, then pass the Scanner to
// DBReader.Scan.  After Scan returns they iterate with Valid / Next / Deref.
//
// Scanning can be in ascending or descending order:

type Scanner struct {
	// Bounds configured by the caller.
	Cmp1 int // starting comparison: btree.CmpGE or btree.CmpLE
	Cmp2 int // stopping comparison: btree.CmpLE / btree.CmpGE (or 0 for prefix scan)
	Key1 Record
	Key2 Record // required when Cmp2 != 0

	// Fields filled by dbScan; not touched by the caller.
	tx      *DBReader
	tdef    *TableDef
	indexNo int          // -1: primary key; >= 0: secondary index
	iter    *btree.BIter // underlying B-tree iterator
	keyEnd  []byte       // encoded Key2 (the stopping sentinel)
}

// Valid reports whether the scanner is positioned on a row that lies within
// the requested range.
func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return btree.CmpOK(key, sc.Cmp2, sc.keyEnd)
}

// Next advances the scanner by one row.
// Must only be called when Valid() returns true.
func (sc *Scanner) Next() {
	assert(sc.Valid())
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// Deref fills rec with the row at the current scanner position.
// Must only be called when Valid() returns true.
func (sc *Scanner) Deref(rec *Record) {
	assert(sc.Valid())

	tdef := sc.tdef
	rec.Cols = tdef.Cols
	rec.Vals = rec.Vals[:0]
	key, val := sc.iter.Deref()

	if sc.indexNo < 0 {
		// Primary-key scan: the KV pair encodes the full row.
		for _, typ := range tdef.Types {
			rec.Vals = append(rec.Vals, Value{Type: typ})
		}
		decodeValues(key[4:], rec.Vals[:tdef.PKeys])
		decodeValues(val, rec.Vals[tdef.PKeys:])
	} else {
		// Secondary-index scan: decode the index key to get the primary key,
		// then fetch the full row from the primary tree.
		assert(len(val) == 0)

		index := tdef.Indexes[sc.indexNo]
		ival := make([]Value, len(index))
		for i, c := range index {
			ival[i].Type = tdef.Types[ColIndex(tdef, c)]
		}
		decodeValues(key[4:], ival)
		icol := Record{index, ival}

		// Reconstruct the primary key from the decoded index entry.
		rec.Cols = tdef.Cols[:tdef.PKeys]
		for _, c := range rec.Cols {
			rec.Vals = append(rec.Vals, *icol.Get(c))
		}
		// Fetch the complete row by primary key.
		// TODO: skip the round-trip when the index covers all columns.
		ok, err := dbGet(sc.tx, tdef, rec)
		assert(ok && err == nil)
	}
}

// ---------------------------------------------------------------------------
// Index selection helpers
// ---------------------------------------------------------------------------

func isPrefix(long []string, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i, c := range short {
		if long[i] != c {
			return false
		}
	}
	return true
}

// findIndex selects the best index (or primary key) for the given key columns.
// Returns -1 for the primary key, >= 0 for a secondary index.
func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.PKeys]
	if isPrefix(pk, keys) {
		// Primary key (also covers full-table scans with no key columns).
		return -1, nil
	}

	winner := -2
	for i, index := range tdef.Indexes {
		if !isPrefix(index, keys) {
			continue
		}
		if winner == -2 || len(index) < len(tdef.Indexes[winner]) {
			winner = i
		}
	}
	if winner == -2 {
		return -2, fmt.Errorf("no index found for columns: %v", keys)
	}
	return winner, nil
}

// ---------------------------------------------------------------------------
// Partial key encoding
// ---------------------------------------------------------------------------

// encodeKeyPartial encodes values as a (possibly incomplete) index key.
// For missing trailing columns it appends minimum or maximum sentinels
// depending on cmp so that prefix range queries work correctly:
//
//   - CmpLT and CmpGE → nothing appended (empty byte string is the minimum)
//   - CmpGT and CmpLE → 0xff… bytes appended (the maximum sentinel)
func encodeKeyPartial(
	out []byte, prefix uint32, values []Value,
	tdef *TableDef, keys []string, cmp int,
) []byte {
	out = encodeKey(out, prefix, values)

	max := cmp == btree.CmpGT || cmp == btree.CmpLE
loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[ColIndex(tdef, keys[i])] {
		case TypeBytes:
			out = append(out, 0xff)
			break loop // 0xff terminates any string encoding
		case TypeInt64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("encodeKeyPartial: unknown type")
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Core scan implementation
// ---------------------------------------------------------------------------

// dbScan initialises sc for the given table and positions the iterator.
// After dbScan returns, callers use sc.Valid / sc.Next / sc.Deref.
func dbScan(tx *DBReader, tdef *TableDef, req *Scanner) error {
	// Validate the cmp combination.
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0: // forward range:  Cmp1=GE/GT, Cmp2=LE/LT
	case req.Cmp2 > 0 && req.Cmp1 < 0: // backward range: Cmp1=LE/LT, Cmp2=GE/GT
	case req.Cmp1 != 0 && req.Cmp2 == 0 && len(req.Key2.Cols) == 0: // prefix scan
	default:
		return fmt.Errorf("bad range: invalid Cmp1/Cmp2 combination")
	}
	if req.Cmp2 != 0 && !reflect.DeepEqual(req.Key1.Cols, req.Key2.Cols) {
		return fmt.Errorf("bad range key: Key1 and Key2 must have the same columns")
	}
	if err := checkRecordTypes(tdef, req.Key1); err != nil {
		return err
	}
	if req.Cmp2 != 0 {
		if err := checkRecordTypes(tdef, req.Key2); err != nil {
			return err
		}
	}

	// Choose the index.
	indexNo, err := findIndex(tdef, req.Key1.Cols)
	if err != nil {
		return err
	}
	index, prefix := tdef.Cols[:tdef.PKeys], tdef.Prefix
	if indexNo >= 0 {
		index, prefix = tdef.Indexes[indexNo], tdef.IndexPrefixes[indexNo]
	}

	req.tx = tx
	req.tdef = tdef
	req.indexNo = indexNo

	// Seek to Key1.
	keyStart := encodeKeyPartial(nil, prefix, req.Key1.Vals, tdef, index, req.Cmp1)
	req.iter = tx.kvr.Seek(keyStart, req.Cmp1)

	// Compute the stopping key (Key2 / prefix sentinel).
	if req.Cmp2 == 0 {
		// Prefix scan: bound by the adjacent prefix.
		switch req.Cmp1 {
		case btree.CmpGE, btree.CmpGT:
			req.Cmp2 = btree.CmpLT
			req.keyEnd = encodeKey(nil, prefix+1, nil)
		case btree.CmpLE, btree.CmpLT:
			req.Cmp2 = btree.CmpGT
			req.keyEnd = encodeKey(nil, prefix, nil)
		default:
			panic("unreachable")
		}
	} else {
		req.keyEnd = encodeKeyPartial(nil, prefix, req.Key2.Vals, tdef, index, req.Cmp2)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Public Scan method on DBReader
// ---------------------------------------------------------------------------

// Scan initialises req for a range query over table and positions the
// iterator at the first matching row.  After Scan returns, use
// req.Valid / req.Next / req.Deref to iterate.
func (tx *DBReader) Scan(table string, req *Scanner) error {
	tdef := getTableDef(tx, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbScan(tx, tdef, req)
}
