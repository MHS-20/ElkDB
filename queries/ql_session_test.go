package queries

// Tests the *complete* client pipeline:
//
//   raw text (chunked, line-by-line, or in one shot)
//       │
//       ▼
//   StmtSplitter.Feed / Pop          — statement boundary detection
//       │
//       ▼
//   DBTXExecString / DBReaderExecString  — parse + execute
//       │
//       ▼
//   QLResult

import (
	"os"
	"strings"
	"testing"

	is "github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Session — the "client connection"
//
// A Session owns a StmtSplitter (the input buffer) and a DB handle.
// Callers feed raw text in any chunk size; the session drives the splitter,
// opens a transaction per complete statement, executes it, and collects
// results — exactly what a network read-loop would do.
// ---------------------------------------------------------------------------

type Session struct {
	db       DB
	splitter StmtSplitter
}

func newSession(t *testing.T, path string) *Session {
	t.Helper()
	os.Remove(path)
	s := &Session{}
	s.db.Path = path
	// s.db.NoSync = true
	err := s.db.Open()
	is.NoError(t, err)
	t.Cleanup(func() {
		s.db.Close()
		os.Remove(path)
	})
	return s
}

// SendChunk simulates a partial network write: bytes arrive in one chunk.
// All complete statements found in the buffer are executed immediately;
// incomplete trailing input stays buffered for the next call.
func (s *Session) SendChunk(t *testing.T, chunk string) []QLResult {
	t.Helper()
	s.splitter.Feed(chunk)
	return s.drainSplitter(t)
}

// SendLines simulates a line-oriented protocol (e.g. telnet / readline):
// each line is fed separately, as if the client pressed Enter after each one.
func (s *Session) SendLines(t *testing.T, lines ...string) []QLResult {
	t.Helper()
	var results []QLResult
	for _, line := range lines {
		s.splitter.Feed(line + "\n")
		results = append(results, s.drainSplitter(t)...)
	}
	return results
}

// drainSplitter pops and executes every complete statement currently in the
// buffer, returning their results in order.
func (s *Session) drainSplitter(t *testing.T) []QLResult {
	t.Helper()
	var results []QLResult
	for {
		stmt, ok := s.splitter.Pop()
		if !ok {
			break
		}
		tx := DBTX{}
		s.db.Begin(&tx)
		result, err := DBTXExecString(&tx, stmt)
		if err != nil {
			s.db.Abort(&tx)
			t.Fatalf("SQL error: %v\nstatement: %q", err, stmt)
		}
		err = s.db.Commit(&tx)
		is.NoError(t, err)
		results = append(results, result)
	}
	return results
}

// SendChunkErr is like SendChunk but expects the first complete statement to
// return a SQL-level error (parse or execution). The DB transaction is
// aborted and the error is returned for the caller to inspect.
func (s *Session) SendChunkErr(t *testing.T, chunk string) error {
	t.Helper()
	s.splitter.Feed(chunk)
	stmt, ok := s.splitter.Pop()
	if !ok {
		t.Fatal("SendChunkErr: no complete statement found")
	}
	tx := DBTX{}
	s.db.Begin(&tx)
	_, err := DBTXExecString(&tx, stmt)
	s.db.Abort(&tx)
	return err
}

// ---------------------------------------------------------------------------
// Helper: collect all rows from repeated SELECT chunks
// ---------------------------------------------------------------------------

func rows(results []QLResult) []Record {
	var out []Record
	for _, r := range results {
		out = append(out, r.Records...)
	}
	return out
}

// ---------------------------------------------------------------------------
// 1. Schema setup sent as one chunk
// ---------------------------------------------------------------------------

func TestSession_SchemaInOneChunk(t *testing.T) {
	s := newSession(t, "sess1.db")

	results := s.SendChunk(t,
		"CREATE TABLE users (id int64, name string, score int64, PRIMARY KEY (id));")

	is.Len(t, results, 1)
	is.Empty(t, results[0].Records)
}

// ---------------------------------------------------------------------------
// 2. Schema split across multiple lines (realistic terminal input)
// ---------------------------------------------------------------------------

func TestSession_SchemaAcrossLines(t *testing.T) {
	s := newSession(t, "sess2.db")

	// Client types the CREATE TABLE statement line by line.
	results := s.SendLines(
		t,
		"CREATE TABLE products (",
		"  id    int64,",
		"  name  string,",
		"  price int64,",
		"  PRIMARY KEY (id)",
		");",
	)

	is.Len(t, results, 1, "exactly one statement should have been executed")
}

// ---------------------------------------------------------------------------
// 3. Multiple statements in a single chunk (pipeline / batch send)
// ---------------------------------------------------------------------------

func TestSession_BatchInOneChunk(t *testing.T) {
	s := newSession(t, "sess3.db")

	// All three statements arrive in one network write.
	results := s.SendChunk(t, strings.Join([]string{
		"CREATE TABLE t (id int64, val string, PRIMARY KEY (id));",
		"INSERT INTO t (id, val) VALUES (1, 'a');",
		"INSERT INTO t (id, val) VALUES (2, 'b');",
	}, ""))

	is.Len(t, results, 3)
	is.Equal(t, uint64(1), results[1].Added)
	is.Equal(t, uint64(1), results[2].Added)
}

// ---------------------------------------------------------------------------
// 4. Statement boundary split across two chunks
//    (the semicolon arrives in a later TCP packet)
// ---------------------------------------------------------------------------

func TestSession_StatementSplitAcrossChunks(t *testing.T) {
	s := newSession(t, "sess4.db")

	// First chunk: schema (complete) + first half of INSERT
	r1 := s.SendChunk(t,
		"CREATE TABLE t (id int64, val string, PRIMARY KEY (id));"+
			"INSERT INTO t (id, val) VALUES (1, 'hel")

	// Only the CREATE should have executed; INSERT is still buffered.
	is.Len(t, r1, 1, "only CREATE should be done; INSERT is incomplete")

	// Second chunk: rest of the INSERT value + semicolon
	r2 := s.SendChunk(t, "lo');")
	is.Len(t, r2, 1)
	is.Equal(t, uint64(1), r2[0].Added)

	// Verify the row was written correctly.
	tx := DBTX{}
	s.db.Begin(&tx)
	result, err := DBTXExecString(&tx, []byte("SELECT * FROM t"))
	s.db.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, result.Records, 1)
	is.Equal(t, []byte("hello"), result.Records[0].Get("val").Str)
}

// ---------------------------------------------------------------------------
// 5. Semicolons inside string literals must not split the statement
// ---------------------------------------------------------------------------

func TestSession_SemicolonInStringLiteral(t *testing.T) {
	s := newSession(t, "sess5.db")

	s.SendChunk(t, "CREATE TABLE t (id int64, msg string, PRIMARY KEY (id));")

	// The semicolons inside 'hello; world' and 'foo; bar' are NOT delimiters.
	results := s.SendChunk(t,
		"INSERT INTO t (id, msg) VALUES (1, 'hello; world'), (2, 'foo; bar');")

	is.Len(t, results, 1, "should be one INSERT, not split by in-string semicolons")
	is.Equal(t, uint64(2), results[0].Added)

	tx := DBTX{}
	s.db.Begin(&tx)
	res, err := DBTXExecString(&tx, []byte("SELECT * FROM t"))
	s.db.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, res.Records, 2)
	is.Equal(t, []byte("hello; world"), res.Records[0].Get("msg").Str)
	is.Equal(t, []byte("foo; bar"), res.Records[1].Get("msg").Str)
}

// ---------------------------------------------------------------------------
// 6. Escaped quote inside a string does not terminate the literal early
// ---------------------------------------------------------------------------

func TestSession_EscapedQuoteInString(t *testing.T) {
	s := newSession(t, "sess6.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, msg string, PRIMARY KEY (id));")

	results := s.SendChunk(t, `INSERT INTO t (id, msg) VALUES (1, 'it\'s fine');`)
	is.Len(t, results, 1)
	is.Equal(t, uint64(1), results[0].Added)

	tx := DBTX{}
	s.db.Begin(&tx)
	res, err := DBTXExecString(&tx, []byte("SELECT * FROM t"))
	s.db.Abort(&tx)
	is.NoError(t, err)
	is.Equal(t, []byte("it's fine"), res.Records[0].Get("msg").Str)
}

// ---------------------------------------------------------------------------
// 7. Full CRUD workflow sent line by line (simulates an interactive REPL)
// ---------------------------------------------------------------------------

func TestSession_CRUDWorkflow(t *testing.T) {
	s := newSession(t, "sess7.db")

	// --- CREATE ---
	s.SendLines(
		t,
		"CREATE TABLE employees (",
		"  id     int64,",
		"  dept   string,",
		"  salary int64,",
		"  PRIMARY KEY (id)",
		");",
	)

	// --- INSERT three rows, each on its own line ---
	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (1, 'eng', 90000);")
	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (2, 'eng', 80000);")
	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (3, 'hr',  70000);")

	// --- SELECT all ---
	{
		tx := DBTX{}
		s.db.Begin(&tx)
		res, err := DBTXExecString(&tx, []byte("SELECT * FROM employees"))
		s.db.Abort(&tx)
		is.NoError(t, err)
		is.Len(t, res.Records, 3)
	}

	// --- UPDATE ---
	s.SendLines(t, "UPDATE employees SET salary = 95000 FILTER id = 1;")

	{
		tx := DBTX{}
		s.db.Begin(&tx)
		res, err := DBTXExecString(&tx,
			[]byte("SELECT * FROM employees FILTER id = 1"))
		s.db.Abort(&tx)
		is.NoError(t, err)
		is.Equal(t, int64(95000), res.Records[0].Get("salary").I64)
	}

	// --- DELETE ---
	s.SendLines(t, "DELETE FROM employees FILTER dept = 'hr';")

	{
		tx := DBTX{}
		s.db.Begin(&tx)
		res, err := DBTXExecString(&tx, []byte("SELECT * FROM employees"))
		s.db.Abort(&tx)
		is.NoError(t, err)
		is.Len(t, res.Records, 2)
	}
}

// ---------------------------------------------------------------------------
// 8. Mixed read/write stream: SELECT interleaved with writes
//    Verifies that a SELECT sees only previously committed state.
// ---------------------------------------------------------------------------

func TestSession_SelectSeesCommittedState(t *testing.T) {
	s := newSession(t, "sess8.db")

	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	// SELECT before any inserts — must return empty.
	r1 := s.SendChunk(t, "SELECT * FROM t;")
	is.Empty(t, rows(r1))

	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'x');")

	// SELECT after insert — must return exactly one row.
	r2 := s.SendChunk(t, "SELECT * FROM t;")
	is.Len(t, rows(r2), 1)

	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (2, 'y');")
	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (3, 'z');")
	s.SendChunk(t, "DELETE FROM t FILTER id = 2;")

	r3 := s.SendChunk(t, "SELECT * FROM t;")
	recs := rows(r3)
	is.Len(t, recs, 2)
	ids := []int64{recs[0].Get("id").I64, recs[1].Get("id").I64}
	is.NotContains(t, ids, int64(2))
}

// ---------------------------------------------------------------------------
// 9. Error in one statement does not corrupt the session buffer:
//    subsequent valid statements still execute correctly.
// ---------------------------------------------------------------------------

func TestSession_ErrorDoesNotCorruptBuffer(t *testing.T) {
	s := newSession(t, "sess9.db")

	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	// Bad statement: table does not exist.
	err := s.SendChunkErr(t, "INSERT INTO ghost (id) VALUES (1);")
	is.Error(t, err, "insert into non-existent table should fail")

	// The session must still be usable.
	results := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (42, 'ok');")
	is.Len(t, results, 1)
	is.Equal(t, uint64(1), results[0].Added)
}

// ---------------------------------------------------------------------------
// 10. Byte-by-byte delivery (extreme chunking)
//     Simulates a pathological sender that sends one byte at a time.
// ---------------------------------------------------------------------------

func TestSession_ByteByByte(t *testing.T) {
	s := newSession(t, "sess10.db")

	sql := "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));"
	var finalResults []QLResult
	for i, b := range []byte(sql) {
		results := s.SendChunk(t, string([]byte{b}))
		if i < len(sql)-1 {
			is.Empty(t, results, "no result until semicolon is received")
		} else {
			is.Len(t, results, 1)
			finalResults = results
		}
	}
	is.Empty(t, finalResults[0].Records)

	// Table must be usable after byte-by-byte creation.
	results := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'hi');")
	is.Equal(t, uint64(1), results[0].Added)
}

// ---------------------------------------------------------------------------
// 11. UPSERT through the session: insert then overwrite
// ---------------------------------------------------------------------------

func TestSession_UpsertThroughSession(t *testing.T) {
	s := newSession(t, "sess11.db")

	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	r1 := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'first');")
	is.Equal(t, uint64(1), r1[0].Added)

	r2 := s.SendChunk(t, "UPSERT INTO t (id, v) VALUES (1, 'second');")
	is.Equal(t, uint64(0), r2[0].Added)
	is.Equal(t, uint64(1), r2[0].Updated)

	tx := DBTX{}
	s.db.Begin(&tx)
	res, _ := DBTXExecString(&tx, []byte("SELECT * FROM t"))
	s.db.Abort(&tx)
	is.Len(t, res.Records, 1)
	is.Equal(t, []byte("second"), res.Records[0].Get("v").Str)
}

// ---------------------------------------------------------------------------
// 12. Large batch: 500 inserts arriving in 10 chunks of 50
//     Exercises the splitter's buffer management under sustained load.
// ---------------------------------------------------------------------------

func TestSession_LargeBatch(t *testing.T) {
	s := newSession(t, "sess12.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, v int64, PRIMARY KEY (id));")

	const total = 500
	const chunksz = 50

	for base := 0; base < total; base += chunksz {
		var b strings.Builder
		for i := base; i < base+chunksz; i++ {
			b.WriteString("INSERT INTO t (id, v) VALUES (")
			b.WriteString(itoa(i))
			b.WriteString(", ")
			b.WriteString(itoa(i * 2))
			b.WriteString(");")
		}
		results := s.SendChunk(t, b.String())
		is.Len(t, results, chunksz)
		for _, r := range results {
			is.Equal(t, uint64(1), r.Added)
		}
	}

	tx := DBTX{}
	s.db.Begin(&tx)
	res, err := DBTXExecString(&tx, []byte("SELECT * FROM t"))
	s.db.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, res.Records, total)
}

// itoa is a minimal int→string helper to avoid importing fmt/strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
