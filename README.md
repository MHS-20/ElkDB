# ElkDB

ElkDB is a relational database built from scratch. It is designed as a learning and reference implementation: every layer — from raw page storage up to SQL parsing and a TCP network protocol — is written without external dependencies.

<div align="center">
<img src="elk.png" alt="Logo" width="300"/>
</div>

---

## Features

- Copy-on-write B-tree with MVCC snapshot isolation
- Write-ahead log (WAL) for crash-safe durability with fast recovery
- Multiple concurrent writers with optimistic concurrency control (OCC) and transparent retry
- Versioned free page list with safe concurrent-reader reclamation
- Read-write and read-only transactions with serialisable isolation
- Relational table layer with primary keys, secondary indexes, and schema persistence
- SQL-like query language supporting CREATE TABLE, INSERT, UPSERT, UPDATE, DELETE, SELECT with WHERE, and **INNER JOIN / LEFT JOIN**
- Binary network protocol (ElkWire) with **connection multiplexing** (multiple in-flight requests per connection)
- **Async API** (`ExecAsync` / `PingAsync`) returning channels for non-blocking client applications
- Go SDK for embedding database access in any application
- Interactive REPL supporting both local (embedded) and remote (server) modes

---

## Internal Architecture

The codebase is organised as a strict dependency stack. Each layer knows only about the layer directly below it; nothing reaches upward.

```
queries/      SQL parser and executor (including JOIN)
tables/       relational schema, encoding, index management
kv/           transactional key-value store, WAL, pager, mmap
btree/        copy-on-write B-tree, free list
network/      ElkWire protocol, server, client SDK
cmd/          binary entry points
```

### B-tree (`btree/`)

The foundation of ElkDB is a copy-on-write B-tree. Nodes are fixed-size 4096-byte slices that map directly to on-disk pages — there is no serialisation step because a node in memory is exactly the bytes that will be written to disk.

Every mutation (insert, update, delete) traverses the tree top-down, allocates new nodes along the modified path, and never touches existing nodes. Old nodes are handed to the free list for eventual reclamation. This means every version of the tree remains readable until no transaction holds a reference to it, which is the property that makes MVCC possible.

The tree supports three insert modes: insert-only (fails if the key already exists), update-only (fails if the key does not exist), and upsert (always succeeds). Range scans are supported via an iterator that walks the leaf level in key order. Internal nodes carry only keys and child pointers; values are stored exclusively in leaf nodes.

Node splitting and merging are handled automatically. A node that overflows a page is split into up to three nodes; a node that falls below a quarter of a page is merged with a sibling. The root is collapsed when it becomes an internal node with a single child.

The B-tree has no knowledge of files, memory maps, or transactions. It interacts with storage exclusively through a `PageStore` interface with three methods: read a page, allocate a new page, and mark a page as freed. The KV layer injects its transaction as the concrete implementation.

Keys are limited to 1000 bytes and values to 3000 bytes, ensuring that a single key-value pair always fits within one page.

### Free Page List (`btree/`)

When a write transaction frees a page it cannot immediately be reused, because a concurrent read transaction may still be reading from it. The free list tracks which pages have been freed and at which transaction version, and only makes a page available for reuse once no active reader holds a snapshot older than that version.

The list itself is stored on disk as a linked list of pages in the same file, using the same 4096-byte page format as B-tree nodes. Each free-list node records a batch of freed page numbers alongside the transaction version at which they were freed. The free list is updated atomically as part of every commit.

When the list needs to write new nodes to record freshly freed pages, it first tries to recycle free-list nodes that are themselves old enough to be reused. This self-recycling loop keeps the on-disk footprint of the free list stable under steady-state workloads.

The minimum active reader version is tracked through a min-heap of all open read transactions. On every write transaction begin, this minimum version is passed to the free list so it knows the reclamation boundary.

### Pager and Memory-Mapped I/O (`kv/`)

The KV layer owns the file and its memory mapping. On open, the file is mapped with `mmap` using `MAP_SHARED`, which means writes to the mapped region are visible to the OS page cache without a separate `write` syscall. When the database grows beyond the current mapping, an additional mapping is appended rather than remapping the whole file; this preserves the validity of pointers held by active read transactions.

The first page of the file is reserved as the master page. It contains a fixed-size header with the database signature, the root page number of the B-tree, the total number of allocated pages, the head of the free list, and the current transaction version. This is the single authoritative record of the database state and the atomic commit point.

File growth is managed with `fallocate`, which pre-allocates disk space in geometric increments to amortise the cost of growth. The mmap is extended separately from the file to maintain the invariant that the mapped region is always at least as large as the live portion of the file.

### Write-Ahead Log (`kv/wal.go`)

ElkDB uses a sequential write-ahead log (WAL) for crash durability. On every commit, page data is written as WAL records (BeginTX, PageData, CommitTX) and the WAL is fsynced. The main database file is NOT touched during a normal commit — only the in-memory state is updated.

On clean shutdown (`KV.Close()`), a checkpoint flushes all WAL pages into the mmap, writes the master page, and truncates the WAL. On crash recovery (detected when the WAL is non-empty on open), the WAL is scanned and committed transactions are replayed to restore the database to a consistent state.

The WAL file format uses a 16-byte header (`ElkWAL` signature, version, CRC) followed by variable-length records. Each record has a type byte, CRC, length, and payload. Record types are BeginTX (1), PageData (2), PageFree (3), and CommitTX (4). Periodic checkpoints keep the WAL bounded.

### Transactions (`kv/`)

ElkDB supports two transaction kinds: read-only snapshots (`KVReader`) and read-write transactions (`KVTX`).

A **read-only transaction** captures the current B-tree root and the current mmap chunk list at the moment it is opened. All subsequent reads within the transaction see exactly this snapshot, regardless of concurrent writes. The transaction is registered in the reader heap so the free list knows it is active.

A **read-write transaction** operates on an in-memory update map: new and modified pages are buffered in a `map[uint64][]byte` and are not written to the mmap until commit. Reads within the transaction check this buffer first and fall back to the mmap for pages that have not been modified. Freed pages are recorded with a `nil` entry and handed to the free list at commit time.

Multiple read-write transactions may exist concurrently (the old single-writer mutex was removed in Phase 2). On commit, the transaction:

1. Acquires a short-lived **commit mutex** (`commitMu`).
2. Performs **optimistic concurrency control (OCC)**: if the transaction's snapshot version does not match the current committed version, the commit is aborted with a serialisation conflict. The client SDK retries transparently (up to 20 attempts).
3. Writes modified pages into the mmap (under `mmapMu`).
4. Appends commit records to the WAL and fsyncs.
5. Publishes the new B-tree root and increments the version.

Because pages are allocated from a central counter under `pageAllocMu`, concurrent writers never step on each other's page numbers. The version-gap OCC check prevents the "divergent roots" problem where two writers simultaneously modify disjoint keys but one overwrites the other's tree root.

Aborting a transaction simply discards the in-memory update map. Because nothing was written to disk, abort is instantaneous and infallible.

### Key-Value Store (`kv/`)

The KV layer exposes a simple get/update/delete interface over the B-tree. It is not used directly by application code; the tables layer sits on top of it and provides the relational abstraction.

### Tables and Schemas (`tables/`)

The tables layer builds a relational model on top of the key-value store. Each table has a named schema (`TableDef`) recording column names, column types, the number of leading primary-key columns, and any secondary indexes. Schemas are stored in a reserved system table (`@table`) as JSON-encoded values, making them durable and transactional like all other data.

ElkDB supports two column types: 64-bit signed integers (`TypeInt64`) and variable-length byte strings (`TypeBytes`). Rows are encoded as ordered byte keys using a type-preserving encoding: integers are bias-encoded so their unsigned byte representation is sort-order-compatible with their signed value; byte strings are null-terminated with an escape scheme that preserves order even when the data contains null bytes.

Primary keys are formed by encoding the primary-key columns in declaration order. This encoding is stored as the B-tree key; the remaining non-key columns are stored as the B-tree value.

Secondary indexes are implemented as additional B-tree entries whose keys encode the indexed columns concatenated with the primary key (to ensure uniqueness). Index entries contain no value data; a lookup on a secondary index returns a primary key which is then used to fetch the full row.

Table definitions are cached in memory after their first access. The cache is protected by a mutex and is consistent with the underlying B-tree: a schema read within a transaction always sees the schema as of that transaction's snapshot.

Range scans expose a `Scanner` abstraction that wraps the B-tree iterator. The scanner can be positioned with comparison operators (greater-than, greater-than-or-equal, less-than, less-than-or-equal) on a partial primary key.

### Query Language (`queries/`)

The query layer provides a SQL-like interpreter. It consists of a lexer, a recursive-descent parser, an AST, and an executor that maps AST nodes to table operations.

#### Supported Statements

**CREATE TABLE** defines a new table with a list of column definitions, a primary key column count, and an optional list of secondary index definitions.

**INSERT** adds a new row. Fails silently (returns affected = 0) if the primary key already exists.

**UPSERT** inserts a new row or replaces an existing one with the same primary key. Reports affected = 1 when a new row is created, affected = 0 when an existing row is replaced.

**UPDATE** modifies columns of existing rows matching the WHERE clause using SET assignments. Assignments can reference column values and arithmetic expressions.

**DELETE** removes rows matching the WHERE clause.

**SELECT** returns rows from one or more tables. Supports:

- Single-table queries with optional WHERE filter
- **INNER JOIN** and **LEFT JOIN** with ON conditions
- Table aliases (`FROM users u JOIN orders o ON u.id == o.user_id`)
- Qualified column references (`users.id`, `orders.total`)
- Star expansion (`SELECT *`) across all joined tables
- Arbitrary-depth join chains (three or more tables)

**JOIN syntax:**

```sql
SELECT cols FROM t1 [alias]
  [JOIN t2 [alias] ON condition] ...
  [WHERE expr]
```

LEFT JOIN emits NULL values (zero-typed) for the right-side columns when no match exists.

#### WHERE Expressions

WHERE accepts binary expressions with comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`) and arithmetic operators (`+`, `-`, `*`, `/`). Operands may be integer literals, single-quoted string literals, or column references.

When the WHERE clause is a simple comparison on the first primary-key column of a single-table query, the executor pushes it down into a B-tree range scan. All other filtering is applied in memory after the scan.

#### Session and Statement Streaming

A `Session` wraps a database handle and a `StmtSplitter` that buffers partial input and emits complete semicolon-terminated statements. This allows callers to feed text in arbitrary chunks (e.g. line by line from the REPL or in full from the network handler) without managing parse state themselves.

Each statement is executed in its own transaction. Read-only statements (SELECT) use a read-only transaction; all others use a read-write transaction. Results are returned as a slice of `Result` values, one per completed statement.

---

## Network Protocol (ElkWire)

ElkDB includes a binary network protocol called ElkWire, enabling client applications to connect to a running server over TCP and execute queries remotely. The server opens one `Session` per connection; connections are fully isolated from one another.

### Frame Layout

Every message in both directions uses the same fixed-size frame header:

```
┌──────────┬──────────┬────────────┬──────────────────┐
│ MsgType  │ ReqID    │ PayloadLen │ Payload          │
│ 1 byte   │ 4 bytes  │ 4 bytes    │ PayloadLen bytes │
└──────────┴──────────┴────────────┴──────────────────┘
```

All multi-byte integers are big-endian. The `ReqID` field is a monotonically increasing counter assigned by the client and echoed in the server's response.

### Message Types

| Direction        | Type     | Code   | Purpose                          |
|------------------|----------|--------|----------------------------------|
| Client → Server  | Query    | `0x01` | Execute a SQL string             |
| Client → Server  | Ping     | `0x02` | Liveness check                   |
| Server → Client  | Result   | `0x81` | Successful query result          |
| Server → Client  | Error    | `0x82` | Query or protocol error          |
| Server → Client  | Pong     | `0x83` | Ping response                    |

### Connection Multiplexing

The client SDK supports **multiple in-flight requests** over a single TCP connection. A background reader goroutine reads frames from the connection and dispatches each response to the correct waiting caller via a channel keyed by `ReqID`. This means you can fire several queries concurrently and collect their results as they arrive:

```go
ch1 := conn.ExecAsync("SELECT ...")
ch2 := conn.ExecAsync("SELECT ...")
r1, r2 := <-ch1, <-ch2
```

The server also dispatches queries to goroutines: after reading a frame, it parses the SQL and hands execution to a new goroutine. A per-connection write mutex serialises the resulting `MsgResult`/`MsgError` frames back to the wire, so responses may arrive in any order (identified by `ReqID`).

### Query Payload

The Query payload begins with a flags byte, followed by a 4-byte SQL string length, followed by the SQL string bytes. The only defined flag is `FlagReadOnly` (bit 0), which is set automatically by the client when the query begins with `SELECT` and allows the server to open a read-only transaction.

### Result Payload

The Result payload encodes the affected-row count and the full set of returned rows. Column names and type tags are included with each row, making each result self-describing without requiring a separate schema negotiation step. Integer values are encoded as 8-byte big-endian signed integers; byte string values are encoded as a 4-byte length prefix followed by raw bytes.

### Error Payload

The Error payload is a 4-byte length-prefixed UTF-8 string containing the error message from the database engine. Any error that would be returned by `Session.ExecChunk` — including parse errors, type errors, missing tables, and constraint violations — is transmitted as an Error frame rather than closing the connection. The connection remains usable after an error.

---

## Go SDK

The `network` package doubles as the client SDK. Any Go application can import it and connect to a running ElkDB server with three lines of setup.

The central type is `Conn`, obtained by calling `Dial` with the server address.

### Blocking API

- **`Exec(sql string) (Result, error)`** — sends a SQL string and returns the result. OCC conflicts are retried transparently.
- **`Ping() error`** — verifies the connection is alive.

### Async API

- **`ExecAsync(sql string) <-chan ResultWithError`** — sends a SQL string and returns a buffered channel that will receive the result. Non-blocking; the caller may do other work before receiving.
- **`PingAsync() <-chan ResultWithError`** — same pattern for pings.

`ResultWithError` contains:

```go
type ResultWithError struct {
    Result Result  // query result (rows + affected count)
    Err    error   // non-nil on failure
}
```

`Result` carries an `Affected` count for write statements and a `Rows` slice for SELECT statements; each row is a `table.Record` with named columns and typed values.

### Example Application

The following sketch shows a Go program that connects to an ElkDB server, creates two tables, inserts data with a foreign-key relationship, runs a JOIN query, and demonstrates the async API. It uses only the `network` package and the `tables` package for type constants — no other ElkDB internals are needed.

```go
package main

import (
    "fmt"
    "log"

    "github.com/MHS-20/ElkDB/network"
    table "github.com/MHS-20/ElkDB/tables"
)

func main() {
    conn, err := network.Dial("localhost:5433")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create tables.
    conn.Exec(`CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));`)
    conn.Exec(`CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));`)

    // Insert users.
    conn.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'alice', 30);`)
    conn.Exec(`INSERT INTO users (id, name, age) VALUES (2, 'bob', 25);`)

    // Insert orders.
    conn.Exec(`INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);`)
    conn.Exec(`INSERT INTO orders (id, user_id, total) VALUES (20, 2, 200);`)
    conn.Exec(`INSERT INTO orders (id, user_id, total) VALUES (30, 1, 50);`)

    // INNER JOIN.
    res, _ := conn.Exec(
        `SELECT users.name, orders.total ` +
        `FROM users JOIN orders ON users.id == orders.user_id;`,
    )
    fmt.Println("user orders:")
    for _, row := range res.Rows {
        fmt.Printf("  %s total=%d\n", row.Get("users.name").Str, row.Get("orders.total").I64)
    }

    // LEFT JOIN (users with no orders get NULL total).
    res, _ = conn.Exec(
        `SELECT users.name, orders.total ` +
        `FROM users LEFT JOIN orders ON users.id == orders.user_id;`,
    )
    fmt.Println("\nall users with orders (LEFT JOIN):")
    for _, row := range res.Rows {
        name := row.Get("users.name").Str
        total := row.Get("orders.total")
        if total.Type == 0 { // zero type = NULL
            fmt.Printf("  %s no orders\n", name)
        } else {
            fmt.Printf("  %s total=%d\n", name, total.I64)
        }
    }

    // Async API: fire two queries concurrently.
    ch1 := conn.ExecAsync(`SELECT * FROM users WHERE id == 1;`)
    ch2 := conn.ExecAsync(`SELECT * FROM users WHERE id == 2;`)

    r1 := <-ch1
    r2 := <-ch2
    fmt.Printf("\nasync results: %s age=%d, %s age=%d\n",
        r1.Result.Rows[0].Get("name").Str, r1.Result.Rows[0].Get("age").I64,
        r2.Result.Rows[0].Get("name").Str, r2.Result.Rows[0].Get("age").I64,
    )
}
```

---

## Running ElkDB Locally

### Start the server

```
./elkdb-server
```

### Connect with the interactive REPL

Local mode opens the database file directly without a server:

```
./elkdb
```

Remote mode connects to a running server:

```
./elkdb -remote localhost:5433
```

## Running ElkDB with Docker

Pull the latest image:

```bash
docker pull ghcr.io/MHS-20/elkdb:latest
```

Run with a named volume so your data survives container restarts:

```bash
docker run -d \
  -p 5433:5433 \
  -v elkdb-data:/data \
  --name elkdb \
  ghcr.io/MHS-20/elkdb:latest
```

Then connect with the CLI:

```bash
./elkdb -remote localhost:5433
```

> **Data** is stored in the `/data` volume. To back it up, copy `/data/elkdb.db` out of the container:
>
> ```bash
> docker cp elkdb:/data/elkdb.db ./elkdb.db
> ```
>
---

## Limitations

- WHERE pushdown is limited to simple comparisons on the first primary-key column. All other filtering is applied in memory after scanning.
- No `GROUP BY`, `ORDER BY`, or aggregate functions.
- Column types are limited to 64-bit integers and variable-length byte strings.
- The maximum key size is 1000 bytes; the maximum value size is 3000 bytes.
