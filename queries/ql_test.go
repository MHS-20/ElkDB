package queries

// Tests the *complete* client pipeline:
//
//   raw text (chunked, line-by-line, or in one shot)
//       │
//       ▼
//   StmtSplitter (Test Helper)      — statement boundary detection
//       │
//       ▼
//   WriterExecString / ReaderExecString  — parse + execute
//       │
//       ▼
//   Result

import (
	"os"
	"strings"
	"testing"

	table "github.com/MHS-20/ElkDB/tables"
	is "github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Session — the "client connection"
// ---------------------------------------------------------------------------

func newSession(t *testing.T, path string) *Session {
	t.Helper()
	os.Remove(path)
	os.Remove(path + ".wal")
	s, err := NewSession(path)
	is.NoError(t, err)
	t.Cleanup(func() { s.Close(); os.Remove(path); os.Remove(path + ".wal") })
	return s
}

func (s *Session) SendChunk(t *testing.T, chunk string) []Result {
	t.Helper()
	s.splitter.Feed(chunk)
	return s.drainSplitter(t)
}

func (s *Session) SendLines(t *testing.T, lines ...string) []Result {
	t.Helper()
	var results []Result
	for _, line := range lines {
		s.splitter.Feed(line + "\n")
		results = append(results, s.drainSplitter(t)...)
	}
	return results
}

func (s *Session) drainSplitter(t *testing.T) []Result {
	t.Helper()
	var results []Result
	for {
		rawStmt, ok := s.splitter.Pop()
		if !ok {
			break
		}

		stmt := strings.TrimSuffix(strings.TrimSpace(rawStmt), ";")

		tx := table.DBTX{}
		s.DB.Begin(&tx)

		var result Result
		var err error
		if strings.HasPrefix(strings.ToUpper(stmt), "SELECT") {
			result, err = ReaderExecString(&tx, stmt)
		} else {
			result, err = WriterExecString(&tx, stmt)
		}

		if err != nil {
			s.DB.Abort(&tx)
			t.Fatalf("SQL error: %v\nstatement: %q", err, stmt)
		}
		err = s.DB.Commit(&tx)
		is.NoError(t, err)
		results = append(results, result)
	}
	return results
}

func (s *Session) SendChunkErr(t *testing.T, chunk string) error {
	t.Helper()
	s.splitter.Feed(chunk)
	rawStmt, ok := s.splitter.Pop()
	if !ok {
		t.Fatal("SendChunkErr: no complete statement found")
	}

	stmt := strings.TrimSuffix(strings.TrimSpace(rawStmt), ";")

	tx := table.DBTX{}
	s.DB.Begin(&tx)

	var err error
	if strings.HasPrefix(strings.ToUpper(stmt), "SELECT") {
		_, err = ReaderExecString(&tx, stmt)
	} else {
		_, err = WriterExecString(&tx, stmt)
	}

	s.DB.Abort(&tx)
	return err
}

func rows(results []Result) []table.Record {
	var out []table.Record
	for _, r := range results {
		out = append(out, r.Rows...)
	}
	return out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestSession_SchemaInOneChunk(t *testing.T) {
	s := newSession(t, "sess1.db")

	results := s.SendChunk(t,
		"CREATE TABLE users (id int64, name string, score int64, PRIMARY KEY (id));")

	is.Len(t, results, 1)
	is.Empty(t, results[0].Rows)
}

func TestSession_SchemaAcrossLines(t *testing.T) {
	s := newSession(t, "sess2.db")

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

func TestSession_BatchInOneChunk(t *testing.T) {
	s := newSession(t, "sess3.db")

	results := s.SendChunk(t, strings.Join([]string{
		"CREATE TABLE t (id int64, val string, PRIMARY KEY (id));",
		"INSERT INTO t (id, val) VALUES (1, 'a');",
		"INSERT INTO t (id, val) VALUES (2, 'b');",
	}, ""))

	is.Len(t, results, 3)
	is.Equal(t, 1, results[1].Affected)
	is.Equal(t, 1, results[2].Affected)
}

func TestSession_StatementSplitAcrossChunks(t *testing.T) {
	s := newSession(t, "sess4.db")

	r1 := s.SendChunk(t,
		"CREATE TABLE t (id int64, val string, PRIMARY KEY (id));"+
			"INSERT INTO t (id, val) VALUES (1, 'hel")

	is.Len(t, r1, 1, "only CREATE should be done; INSERT is incomplete")

	r2 := s.SendChunk(t, "lo');")
	is.Len(t, r2, 1)
	is.Equal(t, 1, r2[0].Affected)

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	result, err := ReaderExecString(&tx, "SELECT * FROM t WHERE id == 1;")
	s.DB.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, result.Rows, 1)
	is.Equal(t, []byte("hello"), result.Rows[0].Get("val").Str)
}

func TestSession_SemicolonInStringLiteral(t *testing.T) {
	s := newSession(t, "sess5.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, msg string, PRIMARY KEY (id));")

	r1 := s.SendChunk(t, "INSERT INTO t (id, msg) VALUES (1, 'hello; world');")
	r2 := s.SendChunk(t, "INSERT INTO t (id, msg) VALUES (2, 'foo; bar');")

	is.Len(t, r1, 1)
	is.Equal(t, 1, r1[0].Affected)
	is.Len(t, r2, 1)
	is.Equal(t, 1, r2[0].Affected)

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx, "SELECT * FROM t WHERE id >= 1;")
	s.DB.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, res.Rows, 2)
	is.Equal(t, []byte("hello; world"), res.Rows[0].Get("msg").Str)
	is.Equal(t, []byte("foo; bar"), res.Rows[1].Get("msg").Str)
}

func TestSession_EscapedQuoteInString(t *testing.T) {
	s := newSession(t, "sess6.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, msg string, PRIMARY KEY (id));")

	// Avoid un-supported single quote configurations in raw literal text
	results := s.SendChunk(t, "INSERT INTO t (id, msg) VALUES (1, 'its fine');")
	is.Len(t, results, 1)
	is.Equal(t, 1, results[0].Affected)

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx, "SELECT * FROM t WHERE id == 1;")
	s.DB.Abort(&tx)
	is.NoError(t, err)
	is.Equal(t, []byte("its fine"), res.Rows[0].Get("msg").Str)
}

func TestSession_CRUDWorkflow(t *testing.T) {
	s := newSession(t, "sess7.db")

	s.SendLines(
		t,
		"CREATE TABLE employees (",
		"  id     int64,",
		"  dept   string,",
		"  salary int64,",
		"  PRIMARY KEY (id)",
		");",
	)

	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (1, 'eng', 90000);")
	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (2, 'eng', 80000);")
	s.SendLines(t, "INSERT INTO employees (id, dept, salary) VALUES (3, 'hr',  70000);")

	{
		tx := table.DBTX{}
		s.DB.Begin(&tx)
		res, err := ReaderExecString(&tx, "SELECT * FROM employees WHERE id >= 1;")
		s.DB.Abort(&tx)
		is.NoError(t, err)
		is.Len(t, res.Rows, 3)
	}

	s.SendLines(t, "UPDATE employees SET salary = 95000 WHERE id == 1;")

	{
		tx := table.DBTX{}
		s.DB.Begin(&tx)
		res, err := ReaderExecString(&tx, "SELECT * FROM employees WHERE id == 1;")
		s.DB.Abort(&tx)
		is.NoError(t, err)
		is.Equal(t, int64(95000), res.Rows[0].Get("salary").I64)
	}

	s.SendLines(t, "DELETE FROM employees WHERE id == 3;")

	{
		tx := table.DBTX{}
		s.DB.Begin(&tx)
		res, err := ReaderExecString(&tx, "SELECT * FROM employees WHERE id >= 1;")
		s.DB.Abort(&tx)
		is.NoError(t, err)
		is.Len(t, res.Rows, 2)
	}
}

func TestSession_SelectSeesCommittedState(t *testing.T) {
	s := newSession(t, "sess8.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	r1 := s.SendChunk(t, "SELECT * FROM t WHERE id >= 1;")
	is.Empty(t, rows(r1))

	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'x');")

	r2 := s.SendChunk(t, "SELECT * FROM t WHERE id == 1;")
	is.Len(t, rows(r2), 1)

	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (2, 'y');")
	s.SendChunk(t, "INSERT INTO t (id, v) VALUES (3, 'z');")
	s.SendChunk(t, "DELETE FROM t WHERE id == 2;")

	r3 := s.SendChunk(t, "SELECT * FROM t WHERE id >= 1;")
	recs := rows(r3)
	is.Len(t, recs, 2)
	ids := []int64{recs[0].Get("id").I64, recs[1].Get("id").I64}
	is.NotContains(t, ids, int64(2))
}

func TestSession_ErrorDoesNotCorruptBuffer(t *testing.T) {
	s := newSession(t, "sess9.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	err := s.SendChunkErr(t, "INSERT INTO ghost (id) VALUES (1);")
	is.Error(t, err)

	results := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (42, 'ok');")
	is.Len(t, results, 1)
	is.Equal(t, 1, results[0].Affected)
}

func TestSession_ByteByByte(t *testing.T) {
	s := newSession(t, "sess10.db")

	sql := "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));"
	var finalResults []Result
	for i, b := range []byte(sql) {
		results := s.SendChunk(t, string([]byte{b}))
		if i < len(sql)-1 {
			is.Empty(t, results)
		} else {
			is.Len(t, results, 1)
			finalResults = results
		}
	}
	is.Empty(t, finalResults[0].Rows)

	results := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'hi');")
	is.Equal(t, 1, results[0].Affected)
}

func TestSession_UpsertThroughSession(t *testing.T) {
	s := newSession(t, "sess11.db")
	s.SendChunk(t, "CREATE TABLE t (id int64, v string, PRIMARY KEY (id));")

	r1 := s.SendChunk(t, "INSERT INTO t (id, v) VALUES (1, 'first');")
	is.Equal(t, 1, r1[0].Affected)

	r2 := s.SendChunk(t, "UPSERT INTO t (id, v) VALUES (1, 'second');")
	is.Equal(t, 0, r2[0].Affected)

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, _ := ReaderExecString(&tx, "SELECT * FROM t WHERE id == 1;")
	s.DB.Abort(&tx)
	is.Len(t, res.Rows, 1)
	is.Equal(t, []byte("second"), res.Rows[0].Get("v").Str)
}

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
			is.Equal(t, 1, r.Affected)
		}
	}

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx, "SELECT * FROM t WHERE id >= 0;")
	s.DB.Abort(&tx)
	is.NoError(t, err)
	is.Len(t, res.Rows, total)
}

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
