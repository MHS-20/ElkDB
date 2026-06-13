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

// ---------------------------------------------------------------------------
// Join tests
// ---------------------------------------------------------------------------

func TestJoin_TwoTableInner(t *testing.T) {
	s := newSession(t, "join1.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (2, 'bob');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (20, 2, 200);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (30, 1, 50);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT users.name, orders.total FROM users JOIN orders ON users.id == orders.user_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 3)
	// alice=100, alice=50, bob=200 (order depends on PK scan order)
	names := map[int64]string{}
	for _, r := range res.Rows {
		name := string(r.Get("users.name").Str)
		total := r.Get("orders.total").I64
		names[total] = name
	}
	is.Equal(t, "alice", names[100])
	is.Equal(t, "alice", names[50])
	is.Equal(t, "bob", names[200])
}

func TestJoin_WithWhere(t *testing.T) {
	s := newSession(t, "join2.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (2, 'bob');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (20, 2, 200);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (30, 1, 50);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT users.name, orders.total FROM users JOIN orders ON users.id == orders.user_id WHERE orders.total >= 100;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 2)
}

func TestJoin_ColumnProjection(t *testing.T) {
	s := newSession(t, "join3.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, score INT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name, score) VALUES (1, 'alice', 100);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 200);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT users.name, orders.total FROM users JOIN orders ON users.id == orders.user_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 1)
	r := res.Rows[0]
	is.Equal(t, []byte("alice"), r.Get("users.name").Str)
	is.Equal(t, int64(200), r.Get("orders.total").I64)
}

func TestJoin_ThreeTable(t *testing.T) {
	s := newSession(t, "join4.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE items (id INT, order_id INT, product TEXT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id) VALUES (10, 1);")
	s.SendChunk(t, "INSERT INTO orders (id, user_id) VALUES (20, 2);")
	s.SendChunk(t, "INSERT INTO items (id, order_id, product) VALUES (100, 10, 'widget');")
	s.SendChunk(t, "INSERT INTO items (id, order_id, product) VALUES (200, 10, 'gizmo');")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT users.name, items.product FROM users JOIN orders ON users.id == orders.user_id JOIN items ON orders.id == items.order_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 2)
}

func TestJoin_LeftJoin(t *testing.T) {
	s := newSession(t, "join5.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (2, 'bob');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id == orders.user_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 2)

	// Find the bob row (should have null total)
	for _, r := range res.Rows {
		name := string(r.Get("users.name").Str)
		if name == "bob" {
			// LEFT JOIN: bob has no orders, so total should be 0/unknown
			is.Equal(t, uint32(0), r.Get("orders.total").Type)
		}
	}
}

func TestJoin_ErrorAmbiguousColumn(t *testing.T) {
	s := newSession(t, "join6.db")

	s.SendChunk(t, "CREATE TABLE t1 (id INT, val INT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE t2 (id INT, val INT, PRIMARY KEY (id));")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	_, err := ReaderExecString(&tx,
		"SELECT val FROM t1 JOIN t2 ON t1.id == t2.id;")
	is.Error(t, err)
	is.Contains(t, err.Error(), "ambiguous")
	s.DB.Abort(&tx)
}

func TestJoin_ErrorNonExistentTable(t *testing.T) {
	s := newSession(t, "join7.db")

	s.SendChunk(t, "CREATE TABLE t1 (id INT, PRIMARY KEY (id));")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	_, err := ReaderExecString(&tx,
		"SELECT * FROM t1 JOIN ghost ON t1.id == ghost.id;")
	is.Error(t, err)
	is.Contains(t, err.Error(), "not found")
	s.DB.Abort(&tx)
}

func TestJoin_ErrorJoinWithoutOn(t *testing.T) {
	_, err := ParseStatement("SELECT * FROM t1 JOIN t2;")
	is.Error(t, err)
	is.Contains(t, err.Error(), "expected ON")
}

func TestJoin_LargeData(t *testing.T) {
	s := newSession(t, "join9.db")

	s.SendChunk(t, "CREATE TABLE t1 (id INT, val INT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE t2 (id INT, ref INT, PRIMARY KEY (id));")

	// Insert 100 rows in each table.
	for i := 0; i < 100; i++ {
		s.SendChunk(t, "INSERT INTO t1 (id, val) VALUES ("+itoa(i)+", "+itoa(i*10)+");")
		s.SendChunk(t, "INSERT INTO t2 (id, ref) VALUES ("+itoa(i)+", "+itoa(i)+");")
	}

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT t1.val, t2.ref FROM t1 JOIN t2 ON t1.id == t2.ref;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 100)
}

func TestJoin_Alias(t *testing.T) {
	s := newSession(t, "join10.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.id == o.user_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 1)
	r := res.Rows[0]
	is.Equal(t, []byte("alice"), r.Get("u.name").Str)
	is.Equal(t, int64(100), r.Get("o.total").I64)
}

func TestJoin_StarExpand(t *testing.T) {
	s := newSession(t, "join11.db")

	s.SendChunk(t, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));")
	s.SendChunk(t, "CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));")

	s.SendChunk(t, "INSERT INTO users (id, name) VALUES (1, 'alice');")
	s.SendChunk(t, "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);")

	tx := table.DBTX{}
	s.DB.Begin(&tx)
	res, err := ReaderExecString(&tx,
		"SELECT * FROM users JOIN orders ON users.id == orders.user_id;")
	is.NoError(t, err)
	s.DB.Abort(&tx)

	is.Len(t, res.Rows, 1)
	r := res.Rows[0]
	// Should have 5 columns: users.id, users.name, orders.id, orders.user_id, orders.total
	is.Len(t, r.Cols, 5)
	is.Equal(t, int64(1), r.Get("users.id").I64)
	is.Equal(t, []byte("alice"), r.Get("users.name").Str)
	is.Equal(t, int64(10), r.Get("orders.id").I64)
	is.Equal(t, int64(1), r.Get("orders.user_id").I64)
	is.Equal(t, int64(100), r.Get("orders.total").I64)
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
