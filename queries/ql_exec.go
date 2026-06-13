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
		Name:  stmt.Table(),
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
	tdef := tx.TableDef(stmt.Table())
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table())
	}

	rec, err := buildRecord(tdef, stmt.Assigns, nil)
	if err != nil {
		return Result{}, err
	}

	var affected bool
	var execErr error
	switch stmt.Mode {
	case btree.ModeInsertOnly:
		affected, execErr = tx.Insert(stmt.Table(), rec)
	case btree.ModeUpsert:
		affected, execErr = tx.Upsert(stmt.Table(), rec)
	case btree.ModeUpdateOnly:
		affected, execErr = tx.Update(stmt.Table(), rec)
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
func qlExpandStar(_ table.Reader, tdef *table.TableDef, cols []string) []string {
	if len(cols) == 1 && cols[0] == "*" {
		return tdef.Cols
	}
	return cols
}

// ---------------------------------------------------------------------------
// JOIN executor
// ---------------------------------------------------------------------------

// qlSelectJoin implements nested-loop INNER/LEFT JOIN.
func qlSelectJoin(tx table.Reader, stmt Statement) (Result, error) {
	// Collect table definitions.
	tdefs := make([]*table.TableDef, len(stmt.Tables))
	for i, ref := range stmt.Tables {
		tdefs[i] = tx.TableDef(ref.Name)
		if tdefs[i] == nil {
			return Result{}, fmt.Errorf("table not found: %s", ref.Name)
		}
	}

	// Resolve output columns.
	outputCols, err := resolveJoinCols(tdefs, stmt.Tables, stmt.Cols)
	if err != nil {
		return Result{}, err
	}

	// Resolve ON and WHERE expressions: replace bare column names with
	// qualified names where unambiguous.
	onExprs := make([]*Expr, len(stmt.Tables))
	for i, ref := range stmt.Tables {
		if ref.OnExpr != nil {
			resolved := resolveExprCols(*ref.OnExpr, tdefs, stmt.Tables)
			onExprs[i] = &resolved
		}
	}
	var where *Expr
	if stmt.Where != nil {
		resolved := resolveExprCols(*stmt.Where, tdefs, stmt.Tables)
		where = &resolved
	}

	// Nested-loop join: scan leftmost table, for each row scan all right tables.
	var rows []table.Record

	// Build scanner for leftmost table.
	leftSc := &table.Scanner{Cmp1: btree.CmpGE}
	if err := tx.Scan(tdefs[0].Name, leftSc); err != nil {
		return Result{}, err
	}

	for leftSc.Valid() {
		var leftRec table.Record
		leftSc.Deref(&leftRec)
		leftSc.Next()

		leftAlias := tableAlias(stmt.Tables[0], 0)
		leftQualified := qualifyRecord(leftRec, leftAlias)

		leftRows := []table.Record{leftQualified}
		compatible := true

		for rightIdx := 1; rightIdx < len(stmt.Tables) && compatible; rightIdx++ {
			rightSc := &table.Scanner{Cmp1: btree.CmpGE}
			if err := tx.Scan(tdefs[rightIdx].Name, rightSc); err != nil {
				return Result{}, err
			}

			rightAlias := tableAlias(stmt.Tables[rightIdx], rightIdx)
			var nextLeft []table.Record

			for rightSc.Valid() {
				var rightRec table.Record
				rightSc.Deref(&rightRec)
				rightSc.Next()
				rightQualified := qualifyRecord(rightRec, rightAlias)

				for _, lr := range leftRows {
					combined := combineRows(lr, rightQualified)

					// Evaluate ON clause (if any).
					if onExprs[rightIdx] != nil {
						rowMap := joinRecordToMap(combined, stmt.Tables[:rightIdx+1])
						v, err := evalExpr(*onExprs[rightIdx], rowMap)
						if err != nil {
							return Result{}, err
						}
						if v.Type != table.TypeInt64 || v.I64 == 0 {
							continue
						}
					}

					// Evaluate WHERE on final combined row.
					if rightIdx == len(stmt.Tables)-1 && where != nil {
						rowMap := joinRecordToMap(combined, stmt.Tables)
						v, err := evalExpr(*where, rowMap)
						if err != nil {
							return Result{}, err
						}
						if v.Type != table.TypeInt64 || v.I64 == 0 {
							continue
						}
					}

					nextLeft = append(nextLeft, combined)
				}
			}

			// LEFT JOIN: if no right rows matched, emit left rows with NULLs.
			if len(nextLeft) == 0 && stmt.Tables[rightIdx].JoinType == JoinLeft {
				for _, lr := range leftRows {
					nulls := makeNullRecord(tdefs[rightIdx], stmt.Tables[rightIdx])
					combined := combineRows(lr, nulls)
					if rightIdx == len(stmt.Tables)-1 && where != nil {
						rowMap := joinRecordToMap(combined, stmt.Tables)
						v, err := evalExpr(*where, rowMap)
						if err != nil {
							return Result{}, err
						}
						if v.Type != table.TypeInt64 || v.I64 == 0 {
							continue
						}
					}
					nextLeft = append(nextLeft, combined)
				}
			}

			leftRows = nextLeft
			if len(leftRows) == 0 {
				compatible = false
			}
		}

		// Project and append results.
		for _, lr := range leftRows {
			rowMap := joinRecordToMap(lr, stmt.Tables)
			projected := projectJoinRecord(rowMap, outputCols)
			rows = append(rows, projected)
		}
	}

	return Result{Rows: rows}, nil
}

// qualifyRecord prefixes each column in a record with "alias.".
func qualifyRecord(rec table.Record, alias string) table.Record {
	out := table.Record{
		Cols: make([]string, len(rec.Cols)),
		Vals: make([]table.Value, len(rec.Vals)),
	}
	for i, c := range rec.Cols {
		out.Cols[i] = alias + "." + c
		out.Vals[i] = rec.Vals[i]
	}
	return out
}

// combineRows concatenates two rows into one combined record.
func combineRows(left, right table.Record) table.Record {
	var rec table.Record
	rec.Cols = append(rec.Cols, left.Cols...)
	rec.Vals = append(rec.Vals, left.Vals...)
	rec.Cols = append(rec.Cols, right.Cols...)
	rec.Vals = append(rec.Vals, right.Vals...)
	return rec
}

// tableAlias returns the effective alias for a table reference.
func tableAlias(ref TableRef, idx int) string {
	if ref.Alias != "" {
		return ref.Alias
	}
	return ref.Name
}

// makeNullRecord creates a record where all values are zero-valued (NULL).
func makeNullRecord(tdef *table.TableDef, ref TableRef) table.Record {
	alias := tableAlias(ref, 0)
	var rec table.Record
	for _, c := range tdef.Cols {
		rec.Cols = append(rec.Cols, alias+"."+c)
		rec.Vals = append(rec.Vals, table.Value{Type: table.TypeUnknown})
	}
	return rec
}

// joinRecordToMap converts a combined join record to a map keyed by qualified
// column names ("table.col").
func joinRecordToMap(rec table.Record, refs []TableRef) map[string]table.Value {
	m := make(map[string]table.Value, len(rec.Cols))
	for i, c := range rec.Cols {
		m[c] = rec.Vals[i]
	}
	// Also add bare column names so that unresolved (unambiguous)
	// references still work at eval time.
	for i, c := range rec.Cols {
		if !containsDot(c) {
			// Only add if not already present (prefer first occurrence).
			if _, exists := m[c]; !exists {
				m[c] = rec.Vals[i]
			}
		}
	}
	return m
}

// projectJoinRecord projects a join row map onto the requested columns.
func projectJoinRecord(row map[string]table.Value, cols []string) table.Record {
	var rec table.Record
	for _, c := range cols {
		v := row[c]
		rec.Cols = append(rec.Cols, c)
		rec.Vals = append(rec.Vals, v)
	}
	return rec
}

// resolveJoinCols resolves the SELECT column list for a join query.
func resolveJoinCols(tdefs []*table.TableDef, refs []TableRef, cols []string) ([]string, error) {
	if len(cols) == 1 && cols[0] == "*" {
		var out []string
		for i, tdef := range tdefs {
			alias := tableAlias(refs[i], i)
			for _, c := range tdef.Cols {
				out = append(out, alias+"."+c)
			}
		}
		return out, nil
	}
	// Validate and qualify each column reference.
	var out []string
	for _, c := range cols {
		if containsDot(c) {
			// Already qualified: "table.col"
			tableName, colName := splitQualified(c)
			// Look up the table.
			idx := findTableIndex(refs, tableName)
			if idx < 0 {
				return nil, fmt.Errorf("unknown table: %s", tableName)
			}
			if table.ColIndex(tdefs[idx], colName) < 0 {
				return nil, fmt.Errorf("unknown column: %s", c)
			}
			out = append(out, c)
		} else {
			// Bare column name — must be unambiguous across all tables.
			matches := findColInAll(tdefs, c)
			if len(matches) == 0 {
				return nil, fmt.Errorf("unknown column: %s", c)
			}
			if len(matches) > 1 {
				return nil, fmt.Errorf("ambiguous column: %s (found in %s)", c, joinStrings(matches))
			}
			// Qualify it with the matching table.
			out = append(out, matches[0]+"."+c)
		}
	}
	return out, nil
}

// resolveExprCols resolves column references in an expression using table
// qualification. Bare column names are qualified when unambiguous.
func resolveExprCols(expr Expr, tdefs []*table.TableDef, refs []TableRef) Expr {
	switch expr.Kind {
	case ExprCol:
		if containsDot(expr.Col) {
			return expr
		}
		// Bare column name — try to qualify.
		matches := findColInAll(tdefs, expr.Col)
		if len(matches) == 1 {
			expr.Col = matches[0] + "." + expr.Col
		}
		// If 0 or >1, leave bare — error will be reported at eval time.
		return expr
	case ExprBinop:
		left := resolveExprCols(*expr.Left, tdefs, refs)
		right := resolveExprCols(*expr.Right, tdefs, refs)
		return Expr{Kind: ExprBinop, Op: expr.Op, Left: &left, Right: &right}
	default:
		return expr
	}
}

// findTableIndex finds the index of a table reference by name or alias.
func findTableIndex(refs []TableRef, name string) int {
	for i, ref := range refs {
		if ref.Alias == name || ref.Name == name {
			return i
		}
	}
	return -1
}

// findColInAll returns qualified column names ("table.col") for all tables
// that have a column named col.
func findColInAll(tdefs []*table.TableDef, col string) []string {
	var out []string
	for _, tdef := range tdefs {
		if table.ColIndex(tdef, col) >= 0 {
			out = append(out, tdef.Name)
		}
	}
	return out
}

func containsDot(s string) bool {
	for i := range s {
		if s[i] == '.' {
			return true
		}
	}
	return false
}

func splitQualified(s string) (string, string) {
	for i := range s {
		if s[i] == '.' {
			return s[:i], s[i+1:]
		}
	}
	return "", s
}

func joinStrings(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	s := ss[0]
	for _, v := range ss[1:] {
		s += ", " + v
	}
	return s
}

func qlSelect(tx table.Reader, stmt Statement) (Result, error) {
	if len(stmt.Tables) > 1 {
		return qlSelectJoin(tx, stmt)
	}

	tdef := tx.TableDef(stmt.Table())
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table())
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

	if err := tx.Scan(stmt.Table(), sc); err != nil {
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
	tdef := tx.TableDef(stmt.Table())
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table())
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
	if err := tx.Scan(stmt.Table(), sc); err != nil {
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
		ok, err := tx.Update(stmt.Table(), updated)
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
	tdef := tx.TableDef(stmt.Table())
	if tdef == nil {
		return Result{}, fmt.Errorf("table not found: %s", stmt.Table())
	}

	sc := &table.Scanner{Cmp1: btree.CmpGE}
	if stmt.Where != nil {
		cmp, key, ok := extractPKRange(tdef, stmt.Where)
		if ok {
			sc.Cmp1 = cmp
			sc.Key1 = key
		}
	}
	if err := tx.Scan(stmt.Table(), sc); err != nil {
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
		ok, err := tx.Delete(stmt.Table(), pk)
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
