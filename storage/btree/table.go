package btree

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
