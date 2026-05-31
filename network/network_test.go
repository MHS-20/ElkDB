package network_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MHS-20/ElkDB/network"
	table "github.com/MHS-20/ElkDB/tables"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// startServer spins up a Server on a random free port and returns a connected
// Conn plus a cleanup function. The server runs in a background goroutine for
// the duration of the test.
func startServer(t *testing.T) (*network.Conn, func()) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")

	// Grab a free port by binding to :0 and immediately closing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := &network.Server{Addr: addr, DBPath: dbPath}
	go func() {
		// ListenAndServe blocks; errors after the test is done are expected.
		_ = srv.ListenAndServe()
	}()

	// Wait until the server is actually accepting connections (max 500 ms).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := network.Dial(addr)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}

	cleanup := func() { conn.Close() }
	return conn, cleanup
}

// mustExec calls conn.Exec and fails the test on error.
func mustExec(t *testing.T, conn *network.Conn, sql string) network.Result {
	t.Helper()
	res, err := conn.Exec(sql)
	if err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
	return res
}

// requireAffected asserts the affected-row count.
func requireAffected(t *testing.T, res network.Result, want int) {
	t.Helper()
	if res.Affected != want {
		t.Errorf("affected: got %d, want %d", res.Affected, want)
	}
}

// requireRowCount asserts the number of rows in the result.
func requireRowCount(t *testing.T, res network.Result, want int) {
	t.Helper()
	if len(res.Rows) != want {
		t.Errorf("row count: got %d, want %d", len(res.Rows), want)
	}
}

// colValue returns the value of the named column in row i.
func colValue(t *testing.T, res network.Result, row int, col string) table.Value {
	t.Helper()
	if row >= len(res.Rows) {
		t.Fatalf("row %d out of range (len=%d)", row, len(res.Rows))
	}
	r := res.Rows[row]
	for i, c := range r.Cols {
		if c == col {
			return r.Vals[i]
		}
	}
	t.Fatalf("column %q not found in row %d", col, row)
	return table.Value{}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPing(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestCreateTable(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	res := mustExec(t, conn, `CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id));`)
	requireRowCount(t, res, 0)
	requireAffected(t, res, 0)
}

func TestInsertAndSelect(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id));`)

	res := mustExec(t, conn, `INSERT INTO users (id, name) VALUES (1, 'alice');`)
	requireAffected(t, res, 1)

	res = mustExec(t, conn, `INSERT INTO users (id, name) VALUES (2, 'bob');`)
	requireAffected(t, res, 1)

	res = mustExec(t, conn, `SELECT * FROM users;`)
	requireRowCount(t, res, 2)

	v := colValue(t, res, 0, "id")
	if v.Type != table.TypeInt64 || v.I64 != 1 {
		t.Errorf("row 0 id: got %+v, want I64=1", v)
	}
	v = colValue(t, res, 0, "name")
	if v.Type != table.TypeBytes || string(v.Str) != "alice" {
		t.Errorf("row 0 name: got %+v, want 'alice'", v)
	}
	v = colValue(t, res, 1, "id")
	if v.Type != table.TypeInt64 || v.I64 != 2 {
		t.Errorf("row 1 id: got %+v, want I64=2", v)
	}
}

func TestSelectWhereInt(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE items (id INT, label TEXT, PRIMARY KEY (id));`)
	for i := 1; i <= 5; i++ {
		mustExec(t, conn, fmt.Sprintf(`INSERT INTO items (id, label) VALUES (%d, 'item%d');`, i, i))
	}

	res := mustExec(t, conn, `SELECT * FROM items WHERE id == 3;`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "id")
	if v.I64 != 3 {
		t.Errorf("expected id=3, got %d", v.I64)
	}
}

func TestUpsert(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE kv (k TEXT, v TEXT, PRIMARY KEY (k));`)

	// First upsert: new row → Affected == 1 (Added == true).
	res := mustExec(t, conn, `UPSERT INTO kv (k, v) VALUES ('x', 'first');`)
	requireAffected(t, res, 1)

	// Second upsert: overwrite existing row → Affected == 0 (Added == false),
	// because the DB reports Affected = req.Added (new-row flag), not
	// "rows touched". The value must still be updated.
	res = mustExec(t, conn, `UPSERT INTO kv (k, v) VALUES ('x', 'second');`)
	requireAffected(t, res, 0)

	res = mustExec(t, conn, `SELECT * FROM kv WHERE k == 'x';`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "v")
	if string(v.Str) != "second" {
		t.Errorf("upsert overwrite: got %q, want 'second'", string(v.Str))
	}
}

func TestUpdate(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE scores (id INT, score INT, PRIMARY KEY (id));`)
	mustExec(t, conn, `INSERT INTO scores (id, score) VALUES (1, 10);`)

	res := mustExec(t, conn, `UPDATE scores SET score = 99 WHERE id == 1;`)
	requireAffected(t, res, 1)

	res = mustExec(t, conn, `SELECT * FROM scores WHERE id == 1;`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "score")
	if v.I64 != 99 {
		t.Errorf("expected score=99, got %d", v.I64)
	}
}

func TestDelete(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE things (id INT, val TEXT, PRIMARY KEY (id));`)
	mustExec(t, conn, `INSERT INTO things (id, val) VALUES (1, 'a');`)
	mustExec(t, conn, `INSERT INTO things (id, val) VALUES (2, 'b');`)

	res := mustExec(t, conn, `DELETE FROM things WHERE id == 1;`)
	requireAffected(t, res, 1)

	res = mustExec(t, conn, `SELECT * FROM things;`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "id")
	if v.I64 != 2 {
		t.Errorf("expected only id=2 remaining, got %d", v.I64)
	}
}

func TestSelectEmptyTable(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE empty (id INT, PRIMARY KEY (id));`)
	res := mustExec(t, conn, `SELECT * FROM empty;`)
	requireRowCount(t, res, 0)
	requireAffected(t, res, 0)
}

func TestInsertDuplicatePrimaryKey(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE uniq (id INT, v TEXT, PRIMARY KEY (id));`)
	mustExec(t, conn, `INSERT INTO uniq (id, v) VALUES (1, 'first');`)

	// Second insert with the same primary key should be rejected by the DB.
	res, err := conn.Exec(`INSERT INTO uniq (id, v) VALUES (1, 'duplicate');`)
	if err == nil {
		// Some engines silently ignore duplicate inserts; what matters is that
		// the row was not overwritten.
		res = mustExec(t, conn, `SELECT * FROM uniq WHERE id == 1;`)
		requireRowCount(t, res, 1)
		v := colValue(t, res, 0, "v")
		if string(v.Str) != "first" {
			t.Errorf("duplicate insert overwrote row: got %q", string(v.Str))
		}
	}
	// If err != nil the server correctly rejected the duplicate — also fine.
	_ = res
}

func TestServerErrorPropagation(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	// Query against a non-existent table must return an error, not crash.
	_, err := conn.Exec(`SELECT * FROM does_not_exist;`)
	if err == nil {
		t.Fatal("expected error for missing table, got nil")
	}
}

func TestBytesRoundtrip(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	// Store and retrieve a value with spaces and punctuation.
	mustExec(t, conn, `CREATE TABLE blobs (k TEXT, v TEXT, PRIMARY KEY (k));`)
	mustExec(t, conn, `INSERT INTO blobs (k, v) VALUES ('key', 'hello, world!');`)

	res := mustExec(t, conn, `SELECT * FROM blobs WHERE k == 'key';`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "v")
	if string(v.Str) != "hello, world!" {
		t.Errorf("bytes roundtrip: got %q", string(v.Str))
	}
}

func TestProtoRoundtrip(t *testing.T) {
	// Unit-test the codec directly, without a real server or DB.
	t.Run("result_encode_decode", func(t *testing.T) {
		original := network.Result{
			Affected: 3,
			Rows: []table.Record{
				{
					Cols: []string{"id", "name"},
					Vals: []table.Value{
						{Type: table.TypeInt64, I64: 42},
						{Type: table.TypeBytes, Str: []byte("alice")},
					},
				},
				{
					Cols: []string{"id", "name"},
					Vals: []table.Value{
						{Type: table.TypeInt64, I64: -1},
						{Type: table.TypeBytes, Str: []byte("bob")},
					},
				},
			},
		}

		var buf safeBuffer
		if err := network.SendResult(&buf, 7, original); err != nil {
			t.Fatalf("SendResult: %v", err)
		}

		frame, err := network.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if frame.MsgType != network.MsgResult {
			t.Fatalf("MsgType: got 0x%02x, want 0x%02x", frame.MsgType, network.MsgResult)
		}
		if frame.ReqID != 7 {
			t.Fatalf("ReqID: got %d, want 7", frame.ReqID)
		}

		decoded, err := network.ReadResult(&buf, frame.PayloadLen)
		if err != nil {
			t.Fatalf("ReadResult: %v", err)
		}
		if decoded.Affected != original.Affected {
			t.Errorf("Affected: got %d, want %d", decoded.Affected, original.Affected)
		}
		if len(decoded.Rows) != len(original.Rows) {
			t.Fatalf("row count: got %d, want %d", len(decoded.Rows), len(original.Rows))
		}
		for i, row := range decoded.Rows {
			orig := original.Rows[i]
			for j, col := range row.Cols {
				if col != orig.Cols[j] {
					t.Errorf("row %d col %d name: got %q, want %q", i, j, col, orig.Cols[j])
				}
				got, want := row.Vals[j], orig.Vals[j]
				if got.Type != want.Type {
					t.Errorf("row %d col %d type: got %d, want %d", i, j, got.Type, want.Type)
					continue
				}
				switch want.Type {
				case table.TypeInt64:
					if got.I64 != want.I64 {
						t.Errorf("row %d col %d int64: got %d, want %d", i, j, got.I64, want.I64)
					}
				case table.TypeBytes:
					if string(got.Str) != string(want.Str) {
						t.Errorf("row %d col %d bytes: got %q, want %q", i, j, got.Str, want.Str)
					}
				}
			}
		}
	})

	t.Run("error_encode_decode", func(t *testing.T) {
		msg := "something went wrong"
		var buf safeBuffer
		if err := network.SendError(&buf, 99, msg); err != nil {
			t.Fatalf("SendError: %v", err)
		}
		frame, err := network.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if frame.MsgType != network.MsgError {
			t.Fatalf("MsgType: got 0x%02x, want 0x%02x", frame.MsgType, network.MsgError)
		}
		got, err := network.ReadError(&buf, frame.PayloadLen)
		if err != nil {
			t.Fatalf("ReadError: %v", err)
		}
		if got != msg {
			t.Errorf("error message: got %q, want %q", got, msg)
		}
	})

	t.Run("ping_pong_encode_decode", func(t *testing.T) {
		var buf safeBuffer
		if err := network.SendPing(&buf, 1); err != nil {
			t.Fatalf("SendPing: %v", err)
		}
		frame, err := network.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if frame.MsgType != network.MsgPing {
			t.Fatalf("MsgType: got 0x%02x, want 0x%02x", frame.MsgType, network.MsgPing)
		}
		if frame.PayloadLen != 0 {
			t.Errorf("ping payload len: got %d, want 0", frame.PayloadLen)
		}
	})

	t.Run("query_encode_decode", func(t *testing.T) {
		sql := "SELECT * FROM users WHERE id == 1"
		var buf safeBuffer
		if err := network.SendQuery(&buf, 5, sql, true); err != nil {
			t.Fatalf("SendQuery: %v", err)
		}
		frame, err := network.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if frame.MsgType != network.MsgQuery {
			t.Fatalf("MsgType: got 0x%02x, want 0x%02x", frame.MsgType, network.MsgQuery)
		}
		gotSQL, readOnly, err := network.ReadQuery(&buf, frame.PayloadLen)
		if err != nil {
			t.Fatalf("ReadQuery: %v", err)
		}
		if gotSQL != sql {
			t.Errorf("sql: got %q, want %q", gotSQL, sql)
		}
		if !readOnly {
			t.Errorf("readOnly: got false, want true")
		}
	})
}

func TestConcurrentClients(t *testing.T) {
	// One server, N simultaneous clients each doing their own create+insert+select.
	const N = 5

	// Each client needs its own DB file because ElkDB holds a file lock.
	// We start N servers on N ports.
	var wg sync.WaitGroup
	errs := make(chan error, N)

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, cleanup := startServer(t)
			defer cleanup()

			tbl := fmt.Sprintf("t%d", i)
			if _, err := conn.Exec(fmt.Sprintf(
				`CREATE TABLE %s (id INT, v TEXT, PRIMARY KEY (id));`, tbl,
			)); err != nil {
				errs <- fmt.Errorf("client %d create: %w", i, err)
				return
			}
			if _, err := conn.Exec(fmt.Sprintf(
				`INSERT INTO %s (id, v) VALUES (%d, 'val%d');`, tbl, i, i,
			)); err != nil {
				errs <- fmt.Errorf("client %d insert: %w", i, err)
				return
			}
			res, err := conn.Exec(fmt.Sprintf(`SELECT * FROM %s;`, tbl))
			if err != nil {
				errs <- fmt.Errorf("client %d select: %w", i, err)
				return
			}
			if len(res.Rows) != 1 {
				errs <- fmt.Errorf("client %d: expected 1 row, got %d", i, len(res.Rows))
				return
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestPersistence(t *testing.T) {
	// Write rows, close the connection (server keeps running), reconnect and
	// verify the data is still there.
	dbPath := filepath.Join(t.TempDir(), "persist.db")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := &network.Server{Addr: addr, DBPath: dbPath}
	go func() { _ = srv.ListenAndServe() }()

	// Wait for server.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// First connection: create table and insert.
	c1, err := network.Dial(addr)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	mustExec(t, c1, `CREATE TABLE persist (id INT, v TEXT, PRIMARY KEY (id));`)
	mustExec(t, c1, `INSERT INTO persist (id, v) VALUES (42, 'hello');`)
	c1.Close()

	// Second connection: data must still be there.
	c2, err := network.Dial(addr)
	if err != nil {
		t.Fatalf("dial c2: %v", err)
	}
	defer c2.Close()

	res := mustExec(t, c2, `SELECT * FROM persist WHERE id == 42;`)
	requireRowCount(t, res, 1)
	v := colValue(t, res, 0, "v")
	if string(v.Str) != "hello" {
		t.Errorf("persistence: got %q, want 'hello'", string(v.Str))
	}
}

func TestLargeResult(t *testing.T) {
	const rowCount = 500
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE big (id INT, val TEXT, PRIMARY KEY (id));`)
	for i := 0; i < rowCount; i++ {
		mustExec(t, conn, fmt.Sprintf(
			`INSERT INTO big (id, val) VALUES (%d, 'value-%d');`, i, i,
		))
	}

	res := mustExec(t, conn, `SELECT * FROM big;`)
	requireRowCount(t, res, rowCount)
}

func TestMultipleTablesIndependent(t *testing.T) {
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE a (id INT, v TEXT, PRIMARY KEY (id));`)
	mustExec(t, conn, `CREATE TABLE b (id INT, v TEXT, PRIMARY KEY (id));`)
	mustExec(t, conn, `INSERT INTO a (id, v) VALUES (1, 'from-a');`)
	mustExec(t, conn, `INSERT INTO b (id, v) VALUES (1, 'from-b');`)

	resA := mustExec(t, conn, `SELECT * FROM a;`)
	resB := mustExec(t, conn, `SELECT * FROM b;`)

	requireRowCount(t, resA, 1)
	requireRowCount(t, resB, 1)

	vA := colValue(t, resA, 0, "v")
	vB := colValue(t, resB, 0, "v")
	if string(vA.Str) == string(vB.Str) {
		t.Errorf("tables share data: both returned %q", string(vA.Str))
	}
}

func TestConnectionClosedMidQuery(t *testing.T) {
	// Closing the connection before reading the response must not panic the server.
	conn, cleanup := startServer(t)
	defer cleanup()

	mustExec(t, conn, `CREATE TABLE drop_test (id INT, PRIMARY KEY (id));`)

	// Close the underlying connection abruptly.
	conn.Close()

	// Give the server goroutine a moment to notice the EOF and exit cleanly.
	time.Sleep(30 * time.Millisecond)
	// If the server panicked, the test binary itself would have crashed.
}

// ---------------------------------------------------------------------------
// safeBuffer — an in-memory io.ReadWriter safe for use in codec unit tests
// ---------------------------------------------------------------------------

// safeBuffer is a simple FIFO byte buffer implementing io.Reader and io.Writer,
// used to unit-test the codec without a network connection.
type safeBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *safeBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) == 0 {
		return 0, os.ErrDeadlineExceeded // signal EOF-like for tests
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}
