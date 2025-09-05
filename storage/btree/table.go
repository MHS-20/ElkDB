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
 
