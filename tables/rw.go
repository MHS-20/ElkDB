package tables

// Reader is the read-only surface of a table transaction.
// Both *DBReader and *DBTX (which embeds DBReader) satisfy this interface.
// The ql package depends only on this interface, never on the concrete types.
type Reader interface {
	// Get fetches one row by its primary key.
	// rec must contain the primary-key columns on entry; on success it is
	// rewritten with the full row.
	Get(tableName string, rec *Record) (bool, error)

	// Scan initialises req for a range query and positions the iterator at
	// the first matching row.
	Scan(tableName string, req *Scanner) error

	// TableDef returns the definition of the named table, or nil if it does
	// not exist.  Exposes the internal getTableDef lookup so the ql package
	// can inspect schemas without reaching into unexported table internals.
	TableDef(tableName string) *TableDef
}

// Writer is the read-write surface of a table transaction.
// Only *DBTX satisfies this interface.
type Writer interface {
	Reader

	// TableNew creates a new user table. Returns an error if a table with
	// the same name already exists or the definition is invalid.
	TableNew(tdef *TableDef) error

	// Insert adds a new row. Returns (true, nil) if inserted, (false, nil)
	// if the primary key already existed.
	Insert(tableName string, rec Record) (bool, error)

	// Update modifies an existing row. Returns (true, nil) if found and
	// updated, (false, nil) if the primary key was not found.
	Update(tableName string, rec Record) (bool, error)

	// Upsert inserts or replaces a row. Returns (true, nil) if a new row
	// was created, (false, nil) if an existing row was replaced.
	Upsert(tableName string, rec Record) (bool, error)

	// Delete removes a row by its primary key. Returns (true, nil) if the
	// row was found and deleted.
	Delete(tableName string, rec Record) (bool, error)
}
