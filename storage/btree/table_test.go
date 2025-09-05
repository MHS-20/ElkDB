package btree

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	is "github.com/stretchr/testify/require"
)

type TableTester struct {
	db  DB
	ref map[string][]Record
}

func newTableTester() *TableTester {
	os.Remove("r.db")
	tt := &TableTester{
		db:  DB{Path: "r.db"},
		ref: map[string][]Record{},
	}
	err := tt.db.Open()
	assert(err == nil, "open failed")
	return tt
}

func (tt *TableTester) dispose() {
	tt.db.Close()
	os.Remove("r.db")
}

func (tt *TableTester) create(tdef *TableDef) {
	err := tt.db.TableNew(tdef)
	assert(err == nil, "table create failed")
}

func (tt *TableTester) findRef(table string, rec Record) int {
	pkeys := tt.db.tables[table].PKeys
	records := tt.ref[table]
	found := -1
	for i, old := range records {
		if reflect.DeepEqual(old.Vals[:pkeys], rec.Vals[:pkeys]) {
			assert(found == -1, "duplicate primary key found")
			found = i
		}
	}
	return found
}

func (tt *TableTester) add(table string, rec Record) bool {
	added, err := tt.db.Upsert(table, rec)
	assert(err == nil, "upsert failed")

	records := tt.ref[table]
	idx := tt.findRef(table, rec)
	if !added {
		assert(idx >= 0, "record not found")
		records[idx] = rec
	} else {
		assert(idx == -1, "duplicate primary key found")
		tt.ref[table] = append(records, rec)
	}

	return added
}

func (tt *TableTester) del(table string, rec Record) bool {
	deleted, err := tt.db.Delete(table, rec)
	assert(err == nil, "delete failed")

	idx := tt.findRef(table, rec)
	if deleted {
		assert(idx >= 0, "record not found")
		records := tt.ref[table]
		copy(records[idx:], records[idx+1:])
		tt.ref[table] = records[:len(records)-1]
	} else {
		assert(idx == -1, "duplicate primary key found")
	}

	return deleted
}

func (tt *TableTester) get(table string, rec *Record) bool {
	ok, err := tt.db.Get(table, rec)
	assert(err == nil, "get failed")
	idx := tt.findRef(table, *rec)
	if ok {
		assert(idx >= 0, "record not found")
		records := tt.ref[table]
		assert(reflect.DeepEqual(records[idx], *rec), "record mismatch")
	} else {
		assert(idx < 0, "record found")
	}
	return ok
}

func TestTableCreate(t *testing.T) {
	fmt.Println("Table creation")
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_BYTES, TYPE_INT64},
		PKeys: 2,
	}
	tt.create(tdef)

	tdef = &TableDef{
		Name:  "tbl_test2",
		Cols:  []string{"ki1", "ks2"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES},
		PKeys: 2,
	}
	tt.create(tdef)

	{
		rec := (&Record{}).AddStr("key", []byte("next_prefix"))
		ok, err := tt.db.Get("@meta", rec)
		assert(ok && err == nil, "meta get failed")
		is.Equal(t, []byte{102, 0, 0, 0}, rec.Get("val").Str)
	}
	{
		rec := (&Record{}).AddStr("name", []byte("tbl_test"))
		ok, err := tt.db.Get("@table", rec)
		assert(ok && err == nil, "table get failed")
		expected := `{"Name":"tbl_test","Types":[2,1,1,2],"Cols":["ki1","ks2","s1","i2"],"PKeys":2,"Prefix":100}`
		is.Equal(t, expected, string(rec.Get("def").Str))
	}

	tt.dispose()
}

func TestTableBasic(t *testing.T) {
	fmt.Println("Table basic operations")
	tt := newTableTester()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_BYTES, TYPE_INT64},
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

func TestTableEncoding(t *testing.T) {
	fmt.Println("Table encoding")
	input := []int{-1, 0, +1, math.MinInt64, math.MaxInt64}
	sort.Ints(input)

	encoded := []string{}
	for _, i := range input {
		v := Value{Type: TYPE_INT64, I64: int64(i)}
		b := encodeValues(nil, []Value{v})
		out := []Value{v}
		decodeValues(b, out)
		assert(out[0].I64 == int64(i), "decoded value mismatch")
		encoded = append(encoded, string(b))
	}

	is.True(t, sort.StringsAreSorted(encoded))
}
