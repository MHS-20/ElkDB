package queries

// ---------------------------------------------------------------------------
// Token kinds
// ---------------------------------------------------------------------------

type TokenKind uint8

const (
	TokenError TokenKind = iota // sentinel: lexer error
	TokenEOF                    // end of input
	TokenIdent                  // identifier or keyword
	TokenInt                    // integer literal
	TokenStr                    // single-quoted string literal
	TokenSym                    // single punctuation character
	TokenCmp                    // comparison operator: == != < <= > >=
)

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

// Token is a single lexical token.
type Token struct {
	Kind TokenKind
	Text string // raw text from the source
}

// ---------------------------------------------------------------------------
// Expression nodes
// ---------------------------------------------------------------------------

// ExprKind discriminates the Expr union.
type ExprKind uint8

const (
	ExprNum   ExprKind = iota // integer literal
	ExprStr                   // string literal
	ExprCol                   // column reference
	ExprBinop                 // binary operator
)

// Expr is a recursive expression node (used for WHERE / SET values).
type Expr struct {
	Kind ExprKind

	// ExprNum: integer value
	Num int64

	// ExprStr: string value (unquoted)
	Str []byte

	// ExprCol: column name
	Col string

	// ExprBinop: operator and operands
	Op    string // "+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">="
	Left  *Expr
	Right *Expr
}

// ---------------------------------------------------------------------------
// Statement nodes
// ---------------------------------------------------------------------------

// StmtKind discriminates the Statement union.
type StmtKind uint8

const (
	StmtSelect StmtKind = iota
	StmtInsert          // also covers UPSERT (Mode field)
	StmtUpdate
	StmtDelete
	StmtCreateTable
)

// ColDef describes one column inside a CREATE TABLE statement.
type ColDef struct {
	Name string
	Type uint32 // table.TypeBytes or table.TypeInt64
}

// IndexDef describes one index inside a CREATE TABLE statement.
type IndexDef struct {
	Cols []string
}

// Statement is the parsed form of a single SQL-like query.
type Statement struct {
	Kind StmtKind

	// Table name (all statements).
	Table string

	// SELECT: column list ("*" expands to all columns).
	Cols []string

	// SELECT / UPDATE / DELETE: optional filter expression on primary key
	// columns.  nil means no filter (full scan for SELECT/DELETE, or no
	// key restriction for UPDATE).
	Where *Expr

	// SELECT: comparison operators that bound the range scan when Where is
	// a simple primary-key comparison.
	Cmp1 int // btree.CmpGE / CmpGT / CmpLT / CmpLE
	Cmp2 int

	// INSERT / UPDATE: column = value assignments.
	Assigns []Assign

	// INSERT: mode — btree.ModeInsertOnly, ModeUpsert, ModeUpdateOnly.
	Mode int

	// CREATE TABLE
	ColDefs []ColDef
	PKeys   int // number of leading columns that form the primary key
	Indexes []IndexDef
}

// Assign is one "col = expr" pair used in INSERT and UPDATE.
type Assign struct {
	Col  string
	Expr Expr
}
