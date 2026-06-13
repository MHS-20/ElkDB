package tables

import (
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/MHS-20/ElkDB/btree"
	is "github.com/stretchr/testify/require"
)

type tableTester struct {
	db  DB
	ref map[string][]Record
}

func newTableTester() *tableTester {
	os.Remove("r.db")
	os.Remove("r.db.wal")
	tt := &tableTester{
		db:  DB{Path: "r.db"},
		ref: map[string][]Record{},
	}
	err := tt.db.Open()
	assert(err == nil)
	return tt
}

func (tt *tableTester) dispose() {
	tt.db.Close()
	os.Remove("r.db")
	os.Remove("r.db.wal")
}

func (tt *tableTester) create(tdef *TableDef) {
	tx := DBTX{}
	tt.db.Begin(&tx)
	err := tx.TableNew(tdef)
	assert(err == nil)
	err = tt.db.Commit(&tx)
	assert(err == nil)
}

func (tt *tableTester) findRef(tx *DBTX, table string, rec Record) int {
	tdef := tx.TableDef(table)

	pkeys := tdef.PKeys
	records := tt.ref[table]
	found := -1
	for i, old := range records {
		if reflect.DeepEqual(old.Vals[:pkeys], rec.Vals[:pkeys]) {
			assert(found == -1)
			found = i
		}
	}
	return found
}

func (tt *tableTester) add(table string, rec Record) bool {
	tx := DBTX{}
	tt.db.Begin(&tx)
	added, err := tx.Upsert(table, rec)
	assert(err == nil)

	records := tt.ref[table]
	idx := tt.findRef(&tx, table, rec)
	if !added {
		assert(idx >= 0)
		records[idx] = rec
	} else {
		assert(idx == -1)
		tt.ref[table] = append(records, rec)
	}

	err = tt.db.Commit(&tx)
	assert(err == nil)
	return added
}

func (tt *tableTester) del(table string, rec Record) bool {
	tx := DBTX{}
	tt.db.Begin(&tx)
	deleted, err := tx.Delete(table, rec)
	assert(err == nil)

	idx := tt.findRef(&tx, table, rec)
	if deleted {
		assert(idx >= 0)
		records := tt.ref[table]
		copy(records[idx:], records[idx+1:])
		tt.ref[table] = records[:len(records)-1]
	} else {
		assert(idx == -1)
	}

	err = tt.db.Commit(&tx)
	assert(err == nil)
	return deleted
}

func (tt *tableTester) get(table string, rec *Record) bool {
	tx := DBTX{}
	tt.db.Begin(&tx)
	ok, err := tx.Get(table, rec)
	assert(err == nil)

	idx := tt.findRef(&tx, table, *rec)
	if ok {
		assert(idx >= 0)
		records := tt.ref[table]
		assert(reflect.DeepEqual(records[idx], *rec))
	} else {
		assert(idx < 0)
	}
	tt.db.Abort(&tx)
	return ok
}

func TestTableCreate(t *testing.T) {
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
	}
	tt.create(tdef)

	tdef = &TableDef{
		Name:  "tbl_test2",
		Cols:  []string{"ki1", "ks2"},
		Types: []uint32{TypeInt64, TypeBytes},
		PKeys: 2,
	}
	tt.create(tdef)

	tx := DBTX{}
	tt.db.Begin(&tx)
	{
		rec := (&Record{}).AddStr("key", []byte("next_prefix"))
		ok, err := tx.Get("@meta", rec)
		assert(ok && err == nil)
		is.Equal(t, []byte{102, 0, 0, 0}, rec.Get("val").Str)
	}
	{
		rec := (&Record{}).AddStr("name", []byte("tbl_test"))
		ok, err := tx.Get("@table", rec)
		assert(ok && err == nil)
		expected := (`{"Name":"tbl_test","Types":[2,1,1,2],"Cols":["ki1","ks2","s1","i2"],` +
			`"PKeys":2,"Indexes":[],"Prefix":100,"IndexPrefixes":[]}`)
		is.Equal(t, expected, string(rec.Get("def").Str))
	}
	tt.db.Abort(&tx)
	tt.dispose()
}

func TestTableBasic(t *testing.T) {
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
	}
	tt.create(tdef)

	rec := Record{}
	rec.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
	rec.AddStr("s1", []byte("world")).AddInt64("i2", 2)
	added := tt.add("tbl_test", rec)
	is.True(t, added)

	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
		ok := tt.get("tbl_test", &got)
		is.True(t, ok)
	}
	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello2"))
		ok := tt.get("tbl_test", &got)
		is.False(t, ok)
	}

	rec.Get("s1").Str = []byte("www")
	added = tt.add("tbl_test", rec)
	is.False(t, added)

	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
		ok := tt.get("tbl_test", &got)
		is.True(t, ok)
	}

	{
		key := Record{}
		key.AddInt64("ki1", 1).AddStr("ks2", []byte("hello2"))
		deleted := tt.del("tbl_test", key)
		is.False(t, deleted)

		key.Get("ks2").Str = []byte("hello")
		deleted = tt.del("tbl_test", key)
		is.True(t, deleted)
	}

	tt.dispose()
}

func TestStringEscape(t *testing.T) {
	in := [][]byte{
		{},
		{0},
		{1},
		{0xfe, 2},
		{0xff, 0},
	}
	out := [][]byte{
		{},
		{1, 1},
		{1, 2},
		{0xfe, 0xfe, 2},
		{0xfe, 0xff, 1, 1},
	}
	for i, s := range in {
		b := escapeString(s)
		is.Equal(t, out[i], b)
		s2 := unescapeString(b)
		is.Equal(t, s, s2)
	}
}

func TestTableEncoding(t *testing.T) {
	input := []int{-1, 0, +1, math.MinInt64, math.MaxInt64}
	sort.Ints(input)

	encoded := []string{}
	for _, i := range input {
		v := Value{Type: TypeInt64, I64: int64(i)}
		b := encodeValues(nil, []Value{v})
		out := []Value{v}
		decodeValues(b, out)
		assert(out[0].I64 == int64(i))
		encoded = append(encoded, string(b))
	}
	is.True(t, sort.StringsAreSorted(encoded))
}

func TestTableScan(t *testing.T) {
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
		Indexes: [][]string{
			{"ks2", "ki1"},
			{"i2"},
			{"ki1", "i2"},
		},
	}
	tt.create(tdef)

	size := 100
	for i := 0; i < size; i += 2 {
		rec := Record{}
		rec.AddInt64("ki1", int64(i)).AddStr("ks2", []byte("hello"))
		rec.AddStr("s1", []byte("world")).AddInt64("i2", int64(i/2))
		added := tt.add("tbl_test", rec)
		assert(added)
	}

	tx := DBTX{}
	tt.db.Begin(&tx)

	{
		rec := Record{}
		req := Scanner{
			Cmp1: btree.CmpGE, Cmp2: btree.CmpLE,
			Key1: rec, Key2: rec,
		}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)

		got := []Record{}
		for req.Valid() {
			rec := Record{}
			req.Deref(&rec)
			got = append(got, rec)
			req.Next()
		}
		is.Equal(t, tt.ref["tbl_test"], got)
	}

	tmpkey := func(n int) Record {
		rec := Record{}
		rec.AddInt64("ki1", int64(n))
		return rec
	}
	i2key := func(n int) Record {
		rec := Record{}
		rec.AddInt64("i2", int64(n)/2)
		return rec
	}

	for i := 0; i < size; i += 2 {
		ref := []int64{}
		for j := i; j < size; j += 2 {
			ref = append(ref, int64(j))

			scanners := []Scanner{
				{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: tmpkey(i), Key2: tmpkey(j)},
				{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: tmpkey(i - 1), Key2: tmpkey(j + 1)},
				{Cmp1: btree.CmpGT, Cmp2: btree.CmpLT, Key1: tmpkey(i - 1), Key2: tmpkey(j + 1)},
				{Cmp1: btree.CmpGT, Cmp2: btree.CmpLT, Key1: tmpkey(i - 2), Key2: tmpkey(j + 2)},
				{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: i2key(i), Key2: i2key(j)},
				{Cmp1: btree.CmpGT, Cmp2: btree.CmpLT, Key1: i2key(i - 2), Key2: i2key(j + 2)},
			}
			for _, tmp := range scanners {
				tmp.Cmp1, tmp.Cmp2 = tmp.Cmp2, tmp.Cmp1
				tmp.Key1, tmp.Key2 = tmp.Key2, tmp.Key1
				scanners = append(scanners, tmp)
			}

			for i := range scanners {
				sc := &scanners[i]
				err := tx.Scan("tbl_test", sc)
				assert(err == nil)

				keys := []int64{}
				got := Record{}
				for sc.Valid() {
					sc.Deref(&got)
					keys = append(keys, got.Get("ki1").I64)
					sc.Next()
				}
				if sc.Cmp1 < sc.Cmp2 {
					for a, b := 0, len(keys)-1; a < b; a, b = a+1, b-1 {
						keys[a], keys[b] = keys[b], keys[a]
					}
				}
				is.Equal(t, ref, keys)
			}
		}
	}

	tt.db.Abort(&tx)
	tt.dispose()
}

func TestTableIndex(t *testing.T) {
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
		Indexes: [][]string{
			{"ks2", "ki1"},
			{"i2"},
			{"ki1", "i2"},
		},
	}
	tt.create(tdef)

	record := func(ki1 int64, ks2 string, s1 string, i2 int64) Record {
		rec := Record{}
		rec.AddInt64("ki1", ki1).AddStr("ks2", []byte(ks2))
		rec.AddStr("s1", []byte(s1)).AddInt64("i2", i2)
		return rec
	}

	r1 := record(1, "a1", "v1", 2)
	r2 := record(2, "a2", "v2", -2)
	tt.add("tbl_test", r1)
	tt.add("tbl_test", r2)

	{
		tx := DBTX{}
		tt.db.Begin(&tx)
		rec := Record{}
		rec.AddInt64("i2", 2)
		req := Scanner{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: rec, Key2: rec}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)
		is.True(t, req.Valid())
		out := Record{}
		req.Deref(&out)
		is.Equal(t, r1, out)
		req.Next()
		is.False(t, req.Valid())
		tt.db.Abort(&tx)
	}

	{
		tx := DBTX{}
		tt.db.Begin(&tx)
		rec1 := Record{}
		rec1.AddInt64("i2", 2)
		rec2 := Record{}
		rec2.AddInt64("i2", 4)
		req := Scanner{Cmp1: btree.CmpGT, Cmp2: btree.CmpLE, Key1: rec1, Key2: rec2}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)
		is.False(t, req.Valid())
		tt.db.Abort(&tx)
	}

	{
		tt.add("tbl_test", record(1, "a1", "v1", 1))
		tx := DBTX{}
		tt.db.Begin(&tx)
		rec := Record{}
		rec.AddInt64("i2", 2)
		req := Scanner{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: rec, Key2: rec}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)
		is.False(t, req.Valid())
		tt.db.Abort(&tx)
	}

	{
		tx := DBTX{}
		tt.db.Begin(&tx)
		rec := Record{}
		rec.AddInt64("i2", 1)
		req := Scanner{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: rec, Key2: rec}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)
		is.True(t, req.Valid())
		tt.db.Abort(&tx)
	}

	{
		rec := Record{}
		rec.AddInt64("ki1", 1).AddStr("ks2", []byte("a1"))
		ok := tt.del("tbl_test", rec)
		assert(ok)
	}

	{
		tx := DBTX{}
		tt.db.Begin(&tx)
		rec := Record{}
		rec.AddInt64("i2", 1)
		req := Scanner{Cmp1: btree.CmpGE, Cmp2: btree.CmpLE, Key1: rec, Key2: rec}
		err := tx.Scan("tbl_test", &req)
		assert(err == nil)
		is.False(t, req.Valid())
		tt.db.Abort(&tx)
	}

	tt.dispose()
}
