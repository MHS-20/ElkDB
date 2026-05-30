package queries

import (
	"fmt"

	"github.com/MHS-20/ElkDB/btree"
	table "github.com/MHS-20/ElkDB/tables"
)

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

// Result is the output of a successfully executed statement.
type Result struct {
	// Rows holds the records returned by a SELECT.
	Rows []table.Record
	// Affected counts rows inserted / updated / deleted.
	Affected int
}

// ---------------------------------------------------------------------------
// Top-level executor
// ---------------------------------------------------------------------------

// qlExec executes stmt against the supplied transactions.
// w is required for write statements; r is used for read-only statements.
// For write transactions, pass the same *DBTX for both w and r (DBTX
// satisfies both interfaces).
func qlExec(w table.Writer, r table.Reader, stmt Statement) (Result, error) {
	switch stmt.Kind {
	case StmtSelect:
		return qlSelect(r, stmt)
	case StmtInsert:
		return qlInsert(w, stmt)
	case StmtUpdate:
		return qlUpdate(w, stmt)
	case StmtDelete:
		return qlDelete(w, stmt)
	case StmtCreateTable:
		return qlCreateTable(w, stmt)
	}
	return Result{}, fmt.Errorf("unknown statement kind")
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

func qlCreateTable(tx table.Writer, stmt Statement) (Result, error) {
	tdef := &table.TableDef{
		Name:  stmt.Table,
		PKeys: stmt.PKeys,
	}
	for _, cd := range stmt.ColDefs {
		tdef.Cols = append(tdef.Cols, cd.Name)
		tdef.Types = append(tdef.Types, cd.Type)
	}
	for _, idx := range stmt.Indexes {
		tdef.Indexes = append(tdef.Indexes, idx.Cols)
	}
	if err := tx.TableNew(tdef); err != nil {
		return Result{}, err
	}
	return Result{}, nil
}

// ---------------------------------------------------------------------------
// INSERT / UPSERT
// ---------------------------------------------------------------------------

func qlInsert(tx table.Writer, stmt Statement) (Result, error) {
	tdef := tx.TableDef(stmt.Table)
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table)
	}

	rec, err := buildRecord(tdef, stmt.Assigns, nil)
	if err != nil {
		return Result{}, err
	}

	var affected bool
	var execErr error
	switch stmt.Mode {
	case btree.ModeInsertOnly:
		affected, execErr = tx.Insert(stmt.Table, rec)
	case btree.ModeUpsert:
		affected, execErr = tx.Upsert(stmt.Table, rec)
	case btree.ModeUpdateOnly:
		affected, execErr = tx.Update(stmt.Table, rec)
	default:
		return Result{}, fmt.Errorf("unknown insert mode: %d", stmt.Mode)
	}
	if execErr != nil {
		return Result{}, execErr
	}
	n := 0
	if affected {
		n = 1
	}
	return Result{Affected: n}, nil
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

// qlExpandStar replaces ["*"] with the full column list from tdef.
func qlExpandStar(tx table.Reader, tdef *table.TableDef, cols []string) []string {
	if len(cols) == 1 && cols[0] == "*" {
		return tdef.Cols
	}
	return cols
}

func qlSelect(tx table.Reader, stmt Statement) (Result, error) {
	tdef := tx.TableDef(stmt.Table)
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table)
	}

	outputCols := qlExpandStar(tx, tdef, stmt.Cols)

	// Validate requested columns.
	for _, c := range outputCols {
		if table.ColIndex(tdef, c) < 0 {
			return Result{}, fmt.Errorf("unknown column: %s", c)
		}
	}

	// Build the scanner.
	sc, err := qlScan(tx, tdef, stmt)
	if err != nil {
		return Result{}, err
	}

	var rows []table.Record
	for sc.Valid() {
		var full table.Record
		sc.Deref(&full)
		sc.Next()

		// Apply the WHERE filter (post-scan, for expressions that aren't
		// simple primary-key range bounds).
		if stmt.Where != nil {
			row := recordToMap(full)
			v, err := evalExpr(*stmt.Where, row)
			if err != nil {
				return Result{}, err
			}
			if v.Type != table.TypeInt64 || v.I64 == 0 {
				continue
			}
		}

		// Project down to requested columns.
		rows = append(rows, projectRecord(full, outputCols))
	}

	return Result{Rows: rows}, nil
}

// qlScan builds and initialises a Scanner for the statement's WHERE clause.
// If Where describes a simple primary-key comparison we use it as a range
// bound; otherwise we do a full scan and let qlSelect filter in memory.
func qlScan(tx table.Reader, tdef *table.TableDef, stmt Statement) (*table.Scanner, error) {
	sc := &table.Scanner{}

	if stmt.Where != nil {
		// Try to extract a primary-key range from the WHERE expression.
		cmp, key, ok := extractPKRange(tdef, stmt.Where)
		if ok {
			sc.Cmp1 = cmp
			sc.Key1 = key
			sc.Cmp2 = 0 // prefix scan: bounded by the next prefix
		} else {
			// Full scan; WHERE is evaluated post-scan.
			sc.Cmp1 = btree.CmpGE
		}
	} else {
		sc.Cmp1 = btree.CmpGE
	}

	if err := tx.Scan(stmt.Table, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// extractPKRange tries to decompose a WHERE expr of the form
//
//	pkCol cmp literal
//
// into a scanner bound.  Returns (cmp, key, true) on success.
func extractPKRange(tdef *table.TableDef, expr *Expr) (int, table.Record, bool) {
	if expr == nil || expr.Kind != ExprBinop {
		return 0, table.Record{}, false
	}
	// We only handle "col op literal" for now.
	if expr.Left == nil || expr.Right == nil {
		return 0, table.Record{}, false
	}
	if expr.Left.Kind != ExprCol {
		return 0, table.Record{}, false
	}
	col := expr.Left.Col
	// Must be the first primary-key column.
	if tdef.PKeys == 0 || tdef.Cols[0] != col {
		return 0, table.Record{}, false
	}
	cmp, err := cmpFromStr(expr.Op)
	if err != nil {
		return 0, table.Record{}, false
	}
	val, ok := literalValue(tdef, col, expr.Right)
	if !ok {
		return 0, table.Record{}, false
	}
	rec := table.Record{}
	rec.Cols = []string{col}
	rec.Vals = []table.Value{val}
	return cmp, rec, true
}

// literalValue converts a literal Expr to a table.Value typed according to tdef.
func literalValue(tdef *table.TableDef, col string, expr *Expr) (table.Value, bool) {
	idx := table.ColIndex(tdef, col)
	if idx < 0 {
		return table.Value{}, false
	}
	typ := tdef.Types[idx]
	switch expr.Kind {
	case ExprNum:
		if typ != table.TypeInt64 {
			return table.Value{}, false
		}
		return table.Value{Type: table.TypeInt64, I64: expr.Num}, true
	case ExprStr:
		if typ != table.TypeBytes {
			return table.Value{}, false
		}
		return table.Value{Type: table.TypeBytes, Str: expr.Str}, true
	}
	return table.Value{}, false
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

func qlUpdate(tx table.Writer, stmt Statement) (Result, error) {
	tdef := tx.TableDef(stmt.Table)
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Scan for matching rows (same logic as SELECT).
	sc := &table.Scanner{Cmp1: btree.CmpGE}
	if stmt.Where != nil {
		cmp, key, ok := extractPKRange(tdef, stmt.Where)
		if ok {
			sc.Cmp1 = cmp
			sc.Key1 = key
		}
	}
	if err := tx.Scan(stmt.Table, sc); err != nil {
		return Result{}, err
	}

	affected := 0
	for sc.Valid() {
		var full table.Record
		sc.Deref(&full)
		sc.Next()

		// Post-scan filter.
		if stmt.Where != nil {
			row := recordToMap(full)
			v, err := evalExpr(*stmt.Where, row)
			if err != nil {
				return Result{}, err
			}
			if v.Type != table.TypeInt64 || v.I64 == 0 {
				continue
			}
		}

		// Apply SET assignments on top of the current row values.
		updated, err := buildRecord(tdef, stmt.Assigns, &full)
		if err != nil {
			return Result{}, err
		}
		ok, err := tx.Update(stmt.Table, updated)
		if err != nil {
			return Result{}, err
		}
		if ok {
			affected++
		}
	}

	return Result{Affected: affected}, nil
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

func qlDelete(tx table.Writer, stmt Statement) (Result, error) {
	tdef := tx.TableDef(stmt.Table)
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table)
	}

	sc := &table.Scanner{Cmp1: btree.CmpGE}
	if stmt.Where != nil {
		cmp, key, ok := extractPKRange(tdef, stmt.Where)
		if ok {
			sc.Cmp1 = cmp
			sc.Key1 = key
		}
	}
	if err := tx.Scan(stmt.Table, sc); err != nil {
		return Result{}, err
	}

	// Collect primary keys first to avoid mutating while iterating.
	var toDelete []table.Record
	for sc.Valid() {
		var full table.Record
		sc.Deref(&full)
		sc.Next()

		if stmt.Where != nil {
			row := recordToMap(full)
			v, err := evalExpr(*stmt.Where, row)
			if err != nil {
				return Result{}, err
			}
			if v.Type != table.TypeInt64 || v.I64 == 0 {
				continue
			}
		}
		// Keep only the primary-key columns for the delete call.
		toDelete = append(toDelete, pkRecord(tdef, full))
	}

	affected := 0
	for _, pk := range toDelete {
		ok, err := tx.Delete(stmt.Table, pk)
		if err != nil {
			return Result{}, err
		}
		if ok {
			affected++
		}
	}
	return Result{Affected: affected}, nil
}

// ---------------------------------------------------------------------------
// Record helpers
// ---------------------------------------------------------------------------

// buildRecord constructs a Record for INSERT / UPDATE.
// If base is non-nil the current row values are used as defaults and the
// assigns overlay them (UPDATE behaviour).  Otherwise all columns must be
// provided by the assigns (INSERT behaviour).
func buildRecord(tdef *table.TableDef, assigns []Assign, base *table.Record) (table.Record, error) {
	vals := map[string]table.Value{}

	// Seed with base values (UPDATE).
	if base != nil {
		for i, c := range base.Cols {
			vals[c] = base.Vals[i]
		}
	}

	// Evaluate and apply each assignment.
	for _, a := range assigns {
		v, err := evalExpr(a.Expr, vals)
		if err != nil {
			return table.Record{}, err
		}
		// Type-check against the schema.
		idx := table.ColIndex(tdef, a.Col)
		if idx < 0 {
			return table.Record{}, fmt.Errorf("unknown column: %s", a.Col)
		}
		if v.Type != tdef.Types[idx] {
			return table.Record{}, fmt.Errorf("type mismatch for column %s", a.Col)
		}
		vals[a.Col] = v
	}

	rec := table.Record{}
	for _, c := range tdef.Cols {
		v, ok := vals[c]
		if !ok {
			// Leave zero value; table layer will catch missing required columns.
			v = table.Value{}
		}
		rec.Cols = append(rec.Cols, c)
		rec.Vals = append(rec.Vals, v)
	}
	return rec, nil
}

// recordToMap converts a Record to a map for expression evaluation.
func recordToMap(rec table.Record) map[string]table.Value {
	m := make(map[string]table.Value, len(rec.Cols))
	for i, c := range rec.Cols {
		m[c] = rec.Vals[i]
	}
	return m
}

// projectRecord returns a new Record containing only the named columns.
func projectRecord(rec table.Record, cols []string) table.Record {
	m := recordToMap(rec)
	out := table.Record{}
	for _, c := range cols {
		out.Cols = append(out.Cols, c)
		out.Vals = append(out.Vals, m[c])
	}
	return out
}

// pkRecord returns a Record containing only the primary-key columns of rec.
func pkRecord(tdef *table.TableDef, rec table.Record) table.Record {
	m := recordToMap(rec)
	out := table.Record{}
	for _, c := range tdef.Cols[:tdef.PKeys] {
		out.Cols = append(out.Cols, c)
		out.Vals = append(out.Vals, m[c])
	}
	return out
}
