package queries

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/MHS-20/ElkDB/btree"
	table "github.com/MHS-20/ElkDB/tables"
)

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

type lexer struct {
	input string
	pos   int
}

func newLexer(input string) *lexer { return &lexer{input: input} }

func (l *lexer) skipWS() {
	for l.pos < len(l.input) && (l.input[l.pos] == ' ' || l.input[l.pos] == '\t' ||
		l.input[l.pos] == '\r' || l.input[l.pos] == '\n') {
		l.pos++
	}
}

//
// func (l *lexer) peek() byte {
// 	if l.pos >= len(l.input) {
// 		return 0
// 	}
// 	return l.input[l.pos]
// }

func (l *lexer) next() Token {
	l.skipWS()
	if l.pos >= len(l.input) {
		return Token{Kind: TokenEOF}
	}

	ch := l.input[l.pos]

	// Integer literal
	if ch >= '0' && ch <= '9' || ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9' {
		start := l.pos
		if ch == '-' {
			l.pos++
		}
		for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			l.pos++
		}
		return Token{Kind: TokenInt, Text: l.input[start:l.pos]}
	}

	// String literal (single-quoted)
	if ch == '\'' {
		l.pos++
		start := l.pos
		for l.pos < len(l.input) && l.input[l.pos] != '\'' {
			l.pos++
		}
		if l.pos >= len(l.input) {
			return Token{Kind: TokenError, Text: "unterminated string"}
		}
		s := l.input[start:l.pos]
		l.pos++ // consume closing quote
		return Token{Kind: TokenStr, Text: s}
	}

	// Identifier or keyword
	if isAlpha(ch) || ch == '_' {
		start := l.pos
		for l.pos < len(l.input) && (isAlpha(l.input[l.pos]) || isDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
			l.pos++
		}
		return Token{Kind: TokenIdent, Text: l.input[start:l.pos]}
	}

	// Two-character comparison operators
	if l.pos+1 < len(l.input) {
		two := l.input[l.pos : l.pos+2]
		if two == "==" || two == "!=" || two == "<=" || two == ">=" {
			l.pos += 2
			return Token{Kind: TokenCmp, Text: two}
		}
	}

	// Single-character operators that are also comparison operators
	if ch == '<' || ch == '>' {
		l.pos++
		return Token{Kind: TokenCmp, Text: string(ch)}
	}

	// Single-char punctuation: ( ) , = ; *
	l.pos++
	return Token{Kind: TokenSym, Text: string(ch)}
}

func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// ---------------------------------------------------------------------------
// Parser state
// ---------------------------------------------------------------------------

type parser struct {
	lex     *lexer
	peeked  *Token // one-token lookahead
	lastErr error
}

func newParser(input string) *parser {
	return &parser{lex: newLexer(input)}
}

func (p *parser) peek() Token {
	if p.peeked == nil {
		t := p.lex.next()
		p.peeked = &t
	}
	return *p.peeked
}

func (p *parser) consume() Token {
	t := p.peek()
	p.peeked = nil
	return t
}

func (p *parser) expect(kind TokenKind, text string) (Token, error) {
	t := p.consume()
	if t.Kind != kind || (text != "" && !strings.EqualFold(t.Text, text)) {
		return t, fmt.Errorf("expected %q, got %q", text, t.Text)
	}
	return t, nil
}

func (p *parser) expectIdent() (string, error) {
	t := p.consume()
	if t.Kind != TokenIdent {
		return "", fmt.Errorf("expected identifier, got %q", t.Text)
	}
	return t.Text, nil
}

func (p *parser) keyword(kw string) bool {
	t := p.peek()
	if t.Kind == TokenIdent && strings.EqualFold(t.Text, kw) {
		p.consume()
		return true
	}
	return false
}

func (p *parser) sym(s string) bool {
	t := p.peek()
	if t.Kind == TokenSym && t.Text == s {
		p.consume()
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Expression parser (recursive descent, two precedence levels)
// ---------------------------------------------------------------------------

// parseExpr parses a comparison or arithmetic expression.
func (p *parser) parseExpr() (Expr, error) {
	return p.parseCmp()
}

func (p *parser) parseCmp() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return Expr{}, err
	}
	t := p.peek()
	if t.Kind == TokenCmp {
		p.consume()
		right, err := p.parseAddSub()
		if err != nil {
			return Expr{}, err
		}
		return Expr{Kind: ExprBinop, Op: t.Text, Left: &left, Right: &right}, nil
	}
	return left, nil
}

func (p *parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return Expr{}, err
	}
	for {
		t := p.peek()
		if t.Kind != TokenSym || (t.Text != "+" && t.Text != "-") {
			break
		}
		p.consume()
		right, err := p.parseMulDiv()
		if err != nil {
			return Expr{}, err
		}
		left = Expr{Kind: ExprBinop, Op: t.Text, Left: &left, Right: &right}
	}
	return left, nil
}

func (p *parser) parseMulDiv() (Expr, error) {
	left, err := p.parseAtom()
	if err != nil {
		return Expr{}, err
	}
	for {
		t := p.peek()
		if t.Kind != TokenSym || (t.Text != "*" && t.Text != "/") {
			break
		}
		p.consume()
		right, err := p.parseAtom()
		if err != nil {
			return Expr{}, err
		}
		left = Expr{Kind: ExprBinop, Op: t.Text, Left: &left, Right: &right}
	}
	return left, nil
}

func (p *parser) parseAtom() (Expr, error) {
	t := p.consume()
	switch t.Kind {
	case TokenInt:
		n, err := strconv.ParseInt(t.Text, 10, 64)
		if err != nil {
			return Expr{}, fmt.Errorf("bad integer: %s", t.Text)
		}
		return Expr{Kind: ExprNum, Num: n}, nil
	case TokenStr:
		return Expr{Kind: ExprStr, Str: []byte(t.Text)}, nil
	case TokenIdent:
		return Expr{Kind: ExprCol, Col: t.Text}, nil
	case TokenSym:
		if t.Text == "(" {
			e, err := p.parseExpr()
			if err != nil {
				return Expr{}, err
			}
			if _, err := p.expect(TokenSym, ")"); err != nil {
				return Expr{}, err
			}
			return e, nil
		}
		return Expr{}, fmt.Errorf("unexpected symbol: %s", t.Text)
	default:
		return Expr{}, fmt.Errorf("unexpected token: %s", t.Text)
	}
}

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

// evalExpr evaluates an Expr against a row.  Returns a Value.
func evalExpr(expr Expr, row map[string]table.Value) (table.Value, error) {
	switch expr.Kind {
	case ExprNum:
		return table.Value{Type: table.TypeInt64, I64: expr.Num}, nil
	case ExprStr:
		return table.Value{Type: table.TypeBytes, Str: expr.Str}, nil
	case ExprCol:
		v, ok := row[expr.Col]
		if !ok {
			return table.Value{}, fmt.Errorf("unknown column: %s", expr.Col)
		}
		return v, nil
	case ExprBinop:
		l, err := evalExpr(*expr.Left, row)
		if err != nil {
			return table.Value{}, err
		}
		r, err := evalExpr(*expr.Right, row)
		if err != nil {
			return table.Value{}, err
		}
		return evalBinop(expr.Op, l, r)
	}
	return table.Value{}, fmt.Errorf("unknown expr kind")
}

func evalBinop(op string, l, r table.Value) (table.Value, error) {
	// Arithmetic (int64 only)
	if l.Type == table.TypeInt64 && r.Type == table.TypeInt64 {
		switch op {
		case "+":
			return table.Value{Type: table.TypeInt64, I64: l.I64 + r.I64}, nil
		case "-":
			return table.Value{Type: table.TypeInt64, I64: l.I64 - r.I64}, nil
		case "*":
			return table.Value{Type: table.TypeInt64, I64: l.I64 * r.I64}, nil
		case "/":
			if r.I64 == 0 {
				return table.Value{}, fmt.Errorf("division by zero")
			}
			return table.Value{Type: table.TypeInt64, I64: l.I64 / r.I64}, nil
		}
	}
	// Comparisons — result is int64 (1 = true, 0 = false)
	cmp, err := compareValues(l, r)
	if err != nil {
		return table.Value{}, err
	}
	var result int64
	switch op {
	case "==":
		if cmp == 0 {
			result = 1
		}
	case "!=":
		if cmp != 0 {
			result = 1
		}
	case "<":
		if cmp < 0 {
			result = 1
		}
	case "<=":
		if cmp <= 0 {
			result = 1
		}
	case ">":
		if cmp > 0 {
			result = 1
		}
	case ">=":
		if cmp >= 0 {
			result = 1
		}
	default:
		return table.Value{}, fmt.Errorf("unknown operator: %s", op)
	}
	return table.Value{Type: table.TypeInt64, I64: result}, nil
}

func compareValues(l, r table.Value) (int, error) {
	if l.Type != r.Type {
		return 0, fmt.Errorf("type mismatch in comparison")
	}
	switch l.Type {
	case table.TypeInt64:
		if l.I64 < r.I64 {
			return -1, nil
		}
		if l.I64 > r.I64 {
			return 1, nil
		}
		return 0, nil
	case table.TypeBytes:
		c := strings.Compare(string(l.Str), string(r.Str))
		return c, nil
	}
	return 0, fmt.Errorf("unknown type in comparison")
}

// ---------------------------------------------------------------------------
// Type keyword helper
// ---------------------------------------------------------------------------

func parseTypeKeyword(p *parser) (uint32, error) {
	name, err := p.expectIdent()
	if err != nil {
		return 0, err
	}
	switch strings.ToUpper(name) {
	case "INT", "INT64", "INTEGER":
		return table.TypeInt64, nil
	case "BYTES", "BLOB", "TEXT", "STRING", "VARCHAR":
		return table.TypeBytes, nil
	}
	return 0, fmt.Errorf("unknown column type: %s", name)
}

// ---------------------------------------------------------------------------
// Statement parsers
// ---------------------------------------------------------------------------

// ParseStatement parses one SQL-like statement from input.
func ParseStatement(input string) (Statement, error) {
	p := newParser(input)
	kw, err := p.expectIdent()
	if err != nil {
		return Statement{}, err
	}
	switch strings.ToUpper(kw) {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert(btree.ModeInsertOnly)
	case "UPSERT":
		return p.parseInsert(btree.ModeUpsert)
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CREATE":
		return p.parseCreateTable()
	}
	return Statement{}, fmt.Errorf("unknown statement keyword: %s", kw)
}

// SELECT col, ... FROM table [WHERE expr] [cmp1 key1 cmp2 key2]
// The WHERE clause here is a simple primary-key range, e.g.:
//
//	WHERE id >= 1 AND id <= 100
//
// Full expression filtering is handled post-scan inside qlSelect.
func (p *parser) parseSelect() (Statement, error) {
	stmt := Statement{Kind: StmtSelect}

	// Column list or *
	if p.peek().Kind == TokenSym && p.peek().Text == "*" {
		p.consume()
		stmt.Cols = []string{"*"}
	} else {
		for {
			col, err := p.expectIdent()
			if err != nil {
				return stmt, err
			}
			stmt.Cols = append(stmt.Cols, col)
			if !p.sym(",") {
				break
			}
		}
	}

	if !p.keyword("FROM") {
		return stmt, fmt.Errorf("expected FROM")
	}
	tbl, err := p.expectIdent()
	if err != nil {
		return stmt, err
	}
	stmt.Table = tbl

	// Optional WHERE
	if p.keyword("WHERE") {
		expr, err := p.parseExpr()
		if err != nil {
			return stmt, err
		}
		stmt.Where = &expr
	}

	return stmt, nil
}

// INSERT INTO table (col, ...) VALUES (expr, ...)
func (p *parser) parseInsert(mode int) (Statement, error) {
	stmt := Statement{Kind: StmtInsert, Mode: mode}

	if !p.keyword("INTO") {
		return stmt, fmt.Errorf("expected INTO")
	}
	tbl, err := p.expectIdent()
	if err != nil {
		return stmt, err
	}
	stmt.Table = tbl

	// Column list
	if _, err := p.expect(TokenSym, "("); err != nil {
		return stmt, err
	}
	var cols []string
	for {
		col, err := p.expectIdent()
		if err != nil {
			return stmt, err
		}
		cols = append(cols, col)
		if !p.sym(",") {
			break
		}
	}
	if _, err := p.expect(TokenSym, ")"); err != nil {
		return stmt, err
	}

	if !p.keyword("VALUES") {
		return stmt, fmt.Errorf("expected VALUES")
	}

	// Value list
	if _, err := p.expect(TokenSym, "("); err != nil {
		return stmt, err
	}
	for i, col := range cols {
		if i > 0 {
			if _, err := p.expect(TokenSym, ","); err != nil {
				return stmt, err
			}
		}
		expr, err := p.parseExpr()
		if err != nil {
			return stmt, err
		}
		stmt.Assigns = append(stmt.Assigns, Assign{Col: col, Expr: expr})
	}
	if _, err := p.expect(TokenSym, ")"); err != nil {
		return stmt, err
	}

	return stmt, nil
}

// UPDATE table SET col = expr, ... WHERE expr
func (p *parser) parseUpdate() (Statement, error) {
	stmt := Statement{Kind: StmtUpdate}
	tbl, err := p.expectIdent()
	if err != nil {
		return stmt, err
	}
	stmt.Table = tbl

	if !p.keyword("SET") {
		return stmt, fmt.Errorf("expected SET")
	}
	for {
		col, err := p.expectIdent()
		if err != nil {
			return stmt, err
		}
		if _, err := p.expect(TokenSym, "="); err != nil {
			return stmt, err
		}
		expr, err := p.parseExpr()
		if err != nil {
			return stmt, err
		}
		stmt.Assigns = append(stmt.Assigns, Assign{Col: col, Expr: expr})
		if !p.sym(",") {
			break
		}
	}

	if p.keyword("WHERE") {
		expr, err := p.parseExpr()
		if err != nil {
			return stmt, err
		}
		stmt.Where = &expr
	}

	return stmt, nil
}

// DELETE FROM table WHERE expr
func (p *parser) parseDelete() (Statement, error) {
	stmt := Statement{Kind: StmtDelete}

	if !p.keyword("FROM") {
		return stmt, fmt.Errorf("expected FROM")
	}
	tbl, err := p.expectIdent()
	if err != nil {
		return stmt, err
	}
	stmt.Table = tbl

	if p.keyword("WHERE") {
		expr, err := p.parseExpr()
		if err != nil {
			return stmt, err
		}
		stmt.Where = &expr
	}

	return stmt, nil
}

// CREATE TABLE name (col type, ..., PRIMARY KEY (col, ...) [, INDEX (col, ...)] ...)
func (p *parser) parseCreateTable() (Statement, error) {
	stmt := Statement{Kind: StmtCreateTable}

	if !p.keyword("TABLE") {
		return stmt, fmt.Errorf("expected TABLE")
	}
	tbl, err := p.expectIdent()
	if err != nil {
		return stmt, err
	}
	stmt.Table = tbl

	if _, err := p.expect(TokenSym, "("); err != nil {
		return stmt, err
	}

	for {
		t := p.peek()
		if t.Kind == TokenIdent && strings.EqualFold(t.Text, "PRIMARY") {
			// PRIMARY KEY (col, ...)
			p.consume()
			if !p.keyword("KEY") {
				return stmt, fmt.Errorf("expected KEY after PRIMARY")
			}
			if _, err := p.expect(TokenSym, "("); err != nil {
				return stmt, err
			}
			pkCols, err := p.parseColList()
			if err != nil {
				return stmt, err
			}
			// reorder ColDefs so primary key columns come first
			stmt.PKeys = len(pkCols)
			stmt.ColDefs, err = reorderColDefs(stmt.ColDefs, pkCols)
			if err != nil {
				return stmt, err
			}
		} else if t.Kind == TokenIdent && strings.EqualFold(t.Text, "INDEX") {
			// INDEX (col, ...)
			p.consume()
			if _, err := p.expect(TokenSym, "("); err != nil {
				return stmt, err
			}
			cols, err := p.parseColList()
			if err != nil {
				return stmt, err
			}
			stmt.Indexes = append(stmt.Indexes, IndexDef{Cols: cols})
		} else if t.Kind == TokenIdent {
			// col type
			col, err := p.expectIdent()
			if err != nil {
				return stmt, err
			}
			typ, err := parseTypeKeyword(p)
			if err != nil {
				return stmt, err
			}
			stmt.ColDefs = append(stmt.ColDefs, ColDef{Name: col, Type: typ})
		} else {
			return stmt, fmt.Errorf("unexpected token in CREATE TABLE: %q", t.Text)
		}

		if !p.sym(",") {
			break
		}
		// allow trailing comma before closing paren
		if p.peek().Kind == TokenSym && p.peek().Text == ")" {
			break
		}
	}

	if _, err := p.expect(TokenSym, ")"); err != nil {
		return stmt, err
	}

	if stmt.PKeys == 0 {
		return stmt, fmt.Errorf("CREATE TABLE %s: missing PRIMARY KEY", stmt.Table)
	}

	return stmt, nil
}

// parseColList parses a comma-separated list of identifiers followed by ")".
func (p *parser) parseColList() ([]string, error) {
	var cols []string
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if !p.sym(",") {
			break
		}
	}
	if _, err := p.expect(TokenSym, ")"); err != nil {
		return nil, err
	}
	return cols, nil
}

// reorderColDefs reorders defs so that the columns named in pkCols come first,
// preserving their relative order within both groups.
func reorderColDefs(defs []ColDef, pkCols []string) ([]ColDef, error) {
	byName := map[string]ColDef{}
	for _, d := range defs {
		byName[d.Name] = d
	}
	out := make([]ColDef, 0, len(defs))
	for _, c := range pkCols {
		d, ok := byName[c]
		if !ok {
			return nil, fmt.Errorf("PRIMARY KEY column %q not defined", c)
		}
		out = append(out, d)
		delete(byName, c)
	}
	// append non-PK columns in original order
	for _, d := range defs {
		if _, skip := byName[d.Name]; skip { // still in map → non-PK
			// byName still contains this key
		} else {
			continue
		}
		out = append(out, d)
		delete(byName, d.Name)
	}
	return out, nil
}

// cmpFromStr converts a comparison operator string to a btree.Cmp* constant.
func cmpFromStr(op string) (int, error) {
	switch op {
	case ">=":
		return btree.CmpGE, nil
	case ">":
		return btree.CmpGT, nil
	case "<=":
		return btree.CmpLE, nil
	case "<":
		return btree.CmpLT, nil
	}
	return 0, fmt.Errorf("not a range comparison: %s", op)
}
