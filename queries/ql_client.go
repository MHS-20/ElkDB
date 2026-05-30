package queries

import (
	table "github.com/MHS-20/ElkDB/tables"
)

// WriterExecString parses and executes a single SQL-like statement against a
// read-write transaction.  Use this for INSERT, UPSERT, UPDATE, DELETE, and
// CREATE TABLE statements.
//
// The tx parameter is table.Writer — any *table.DBTX satisfies it, as do
// test doubles.
func WriterExecString(tx table.Writer, query string) (Result, error) {
	stmt, err := ParseStatement(query)
	if err != nil {
		return Result{}, err
	}
	return qlExec(tx, tx, stmt)
}

// ReaderExecString parses and executes a single SQL-like statement against a
// read-only transaction.  Only SELECT statements are permitted; write
// statements will fail because a table.Reader does not satisfy table.Writer.
//
// The tx parameter is table.Reader — both *table.DBReader and *table.DBTX
// satisfy it, as do test doubles.
func ReaderExecString(tx table.Reader, query string) (Result, error) {
	stmt, err := ParseStatement(query)
	if err != nil {
		return Result{}, err
	}
	// A Reader cannot satisfy Writer; pass nil for the write half.
	// qlExec only calls write paths for write statements, so passing nil is
	// safe here as long as the query is a SELECT.
	return qlExec(nil, tx, stmt)
}
