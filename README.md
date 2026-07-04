# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![Go Report Card](https://goreportcard.com/badge/github.com/SimonWaldherr/tinySQL)](https://goreportcard.com/report/github.com/SimonWaldherr/tinySQL)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

## 🎥 Demo

[![Watch the video](https://img.youtube.com/vi/W28-aBk3BL0/hqdefault.jpg)](https://youtu.be/W28-aBk3BL0)

TinySQL is a lightweight, educational SQL database engine written in pure Go. It implements a comprehensive subset of SQL features using only Go's standard library, making it perfect for learning database internals and for applications that need a simple embedded SQL database.

## Product forms

tinySQL is being developed as one compatible engine with three runtime forms:

- **Core package**: in-process Go package for embedding and tests.
- **Embedded database**: SQLite-like local persistent database shape.
- **Server / Enterprise DBMS**: networked runtime for jobs, operations, security, and future HA.

The existing `NewDB`, `OpenDB`, `ParseSQL`, and `Execute` APIs remain the stable low-level API. New code can opt into the additive product-form helpers: `OpenPackage`, `OpenEmbedded`, `OpenServer`, and `OpenEnterprise`. See [docs/product-forms.md](./docs/product-forms.md).

## Production readiness

Durable product forms now expose explicit lifecycle and observability hooks:

- `Instance.Start`, `Instance.Stop`, `Instance.Restart`, and `Instance.Health`.
- `DB.HealthCheck` / `tinysql.HealthCheck` for storage, scheduler, WAL, sync, close, and recovery status.
- Idempotent close behavior for DB, WAL, and Advanced WAL resources.
- `RestartJobScheduler` for controlled scheduler restarts.
- `tinysqld` health/readiness/status endpoints backed by the same DB health snapshot.
- Atomic checkpoints: snapshots are written to a temp file, fsynced, and renamed into place, so a crash mid-checkpoint never corrupts the previous snapshot.
- `WALConfig.CheckpointMaxBytes` / `AdvancedWALConfig.CheckpointMaxBytes` force a checkpoint once the WAL file exceeds a size (default 64 MB), bounding WAL growth independently of the existing transaction-count and time-based triggers.
- PRIMARY KEY / UNIQUE / FOREIGN KEY constraint checks use a per-table hash index maintained incrementally on INSERT/UPDATE/DELETE, instead of scanning the whole table on every write — bulk-loading into a large constrained table is an order of magnitude faster.
- Single-table `GROUP BY` queries using `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` take a raw-row fast path that accumulates aggregates directly during the table scan, instead of buffering every matching row into a `Row` map and re-scanning it once per aggregate expression. See [BENCHMARKS.md](BENCHMARKS.md) for measured numbers against SQLite.
- Concurrent queries: `Execute` takes a shared read lock for SELECT/EXPLAIN/PRAGMA and an exclusive write lock for everything else, for the duration of the whole statement. Before this, `Table.Rows` had no lock at all — two goroutines calling `Execute` concurrently on the same table could race on the underlying slice. The lock is coarse (a write to one table blocks a concurrent read of an unrelated table) rather than per-table, trading some parallelism for a single, easy-to-audit correctness guarantee.
- `Execute` recovers from panics and returns them as an error instead of crashing the host process — important for applications that call it directly rather than through `cmd/server`/`cmd/tinysqld` (which already had their own recovery middleware at the request boundary).
- The parser rejects pathologically deep expression/subquery nesting (`maxParseDepth`, currently 200) with an ordinary parse error. Unbounded recursive-descent parsing on attacker-controlled input can otherwise exhaust the goroutine stack, which in Go is a fatal error that `recover()` cannot catch — this closes that gap before it becomes one.
- Trigger bodies are stored and re-parsed from their **original source text**, not a lossy reprint of the parsed AST. The previous AST-to-SQL serializer was an unfinished placeholder that produced non-executable text (`"INSERT INTO x ... (trigger body)"`), so **every trigger with an INSERT/UPDATE/DELETE body silently failed on every fire** — this is now fixed, including `NEW.col`/`OLD.col` references inside the body.

### Security checklist for `cmd/server` (gRPC + HTTP query server)

`cmd/server` ships with authentication **off** by default (`-auth ""`) and listens on all interfaces by default (`-http :8080`, `-grpc :9090`). Anyone who can reach those ports can run arbitrary SQL, including DDL, with no credentials. The server logs a startup warning if it detects this combination, but does not refuse to start — treat these as required steps before exposing the server beyond your own machine:

- Set `-auth <token>` (checked as a bearer token on both HTTP and gRPC) or bind to `127.0.0.1`/`localhost` if the server is only ever accessed from the same host.
- Configure TLS via `-http-tls-cert`/`-http-tls-key` and `-grpc-tls-cert`/`-grpc-tls-key`; without it, an auth token is sent in the clear.
- `cmd/tinysqld` (the lighter admin/health HTTP daemon) defaults to `127.0.0.1:8088`, so it's loopback-only out of the box; it still supports the same `-auth`-style token if you need to expose it further.

Example:

```go
inst, err := tinysql.OpenEnterprise(tinysql.StorageConfig{
    Mode: tinysql.ModeDisk,
    Path: "./data/tinysql",
}, "default")
if err != nil {
    log.Fatal(err)
}
defer inst.Close()

if !inst.Health().OK {
    log.Fatalf("database unhealthy: %s", inst.Health().Error)
}

if err := inst.Restart(); err != nil {
    log.Fatal(err)
}
```

### Read-only mode for bulk-load / serve-only workloads

If you load data on a schedule (e.g. nightly) and only serve reads in
between, opening the database read-only avoids the WAL entirely and
guarantees vector/FTS caches can never be invalidated by a stray write:

```go
// Load phase: in-memory, with a path so Close() writes a full GOB snapshot.
db, _ := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: "./data/db.gob"})
// ... bulk INSERT/UPDATE via tsql.Execute ...
db.Close() // writes the snapshot

// Serve phase: read-only, loaded from the same snapshot.
serveDB, _ := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: "./data/db.gob", ReadOnly: true})
defer serveDB.Close()

// Warm vector/FTS structures once, right after opening, so the first
// real query never pays the index-build cost:
warmStmt, _ := tsql.ParseSQL(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
tsql.Execute(context.Background(), serveDB, "default", warmStmt)
```

While `ReadOnly` is set, `INSERT`/`UPDATE`/`DELETE` and DDL statements are
rejected with an error; `SELECT`, `EXPLAIN`, and `PRAGMA` are unaffected.
Toggle it at runtime with `db.SetReadOnly(true/false)`.

> **Note on WAL durability:** `ModeAdvancedWAL` now logs every
> INSERT/UPDATE/DELETE automatically — `Execute` wraps each statement in its
> own implicit WAL transaction and logs each row change as it happens, so a
> process killed mid-write and restarted can recover via
> `wal.Recover(db)` with no extra code (see
> `internal/engine/wal_logging_test.go` for a simulated-crash test proving
> this end to end). `ModeWAL` (the older, table-snapshot-diff WAL) is
> **not** auto-wired the same way: it still requires the caller to snapshot
> the DB before and after a change, diff it with
> `storage.CollectWALChanges`, and call `db.WAL().LogTransaction(changes)`
> manually (see `internal/storage/lifecycle_test.go`). For automatic
> durability, prefer `ModeAdvancedWAL` — or `ModeMemory`/`ModeDisk` with a
> `Path`, which persist automatically on `Sync()`/`Close()` (whole-snapshot,
> not per-statement).

## Storage modes

`StorageConfig.Mode` (or `ParseStorageMode("...")`) selects how table data is
kept between memory and disk. All modes share the same SQL engine and
`*DB` API — only persistence behavior differs.

| Mode | String | Persistence | Notes |
|---|---|---|---|
| `ModeMemory` | `"memory"` | None, unless `Path` is set (GOB snapshot on `Close`) | Default; fastest |
| `ModeWAL` | `"wal"` | RAM + write-ahead log, periodic GOB checkpoints | Manual logging only — see the WAL durability note above |
| `ModeDisk` | `"disk"` | One GOB file per table, loaded on demand | Minimizes RAM at the cost of disk I/O |
| `ModeJSON` | `"json"` | One **JSON** file per table, loaded on demand | Same behavior as `ModeDisk`; files are human-readable/diffable/hand-editable. Larger on disk; `big.Rat`/`uuid.UUID` values round-trip as plain strings |
| `ModeIndex` | `"index"` | Schemas in RAM, rows on disk | RAM usage scales with schema, not data size |
| `ModeHybrid` | `"hybrid"` | LRU buffer pool, cold tables spill to disk | Bounded-memory mixed workloads |
| `ModeAdvancedWAL` | `"advanced_wal"` | Row-level WAL with LSNs | Logged automatically on every write — see the WAL durability note above |

```go
// Human-readable per-table JSON files instead of GOB — handy for
// debugging, version control, or interop with non-Go tooling.
db, err := tsql.OpenDB(tsql.StorageConfig{
    Mode: tsql.ModeJSON,
    Path: "./data/tinysql",
})
```

```sql
-- Equivalent via a driver DSN
-- file:./data/tinysql?mode=json
-- mem://?mode=json&path=./data/tinysql
```

## Quick start

### Install

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

### Use the engine directly

```go
package main

import (
    "context"
    "fmt"
    tsql "github.com/SimonWaldherr/tinySQL"
)

func main() {
    db := tsql.NewDB()

    p := tsql.NewParser(`CREATE TABLE users (id INT, name TEXT)`)
    st, _ := p.ParseStatement()
    tsql.Execute(context.Background(), db, "default", st)

    p = tsql.NewParser(`INSERT INTO users VALUES (1, 'Alice')`)
    st, _ = p.ParseStatement()
    tsql.Execute(context.Background(), db, "default", st)

    p = tsql.NewParser(`SELECT id, name FROM users`)
    st, _ = p.ParseStatement()
    rs, _ := tsql.Execute(context.Background(), db, "default", st)

    for _, row := range rs.Rows {
        fmt.Println(tsql.GetVal(row, "id"), tsql.GetVal(row, "name"))
    }
}
```

### Use with database/sql

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

func main() {
    cfg := tsqldriver.DefaultOpenConfig()
    cfg.Tenant = "default"
    cfg.BusyTimeout = 500 * time.Millisecond

    db, err := tsqldriver.OpenWithConfig(context.Background(), cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _, _ = db.Exec(`CREATE TABLE t (id INT, name TEXT)`)
    _, _ = db.Exec(`INSERT INTO t VALUES (?, ?)`, 1, "Alice")

    row := db.QueryRow(`SELECT name FROM t WHERE id = ?`, 1)
    var name string
    _ = row.Scan(&name)
    fmt.Println(name)
}
```

External projects should import `github.com/SimonWaldherr/tinySQL/driver`, not `internal/driver`. Go's `internal/` rule only allows packages inside this module tree to use `internal/*`.

### Build an agent context

tinySQL can also emit a compact, prompt-ready snapshot of live database metadata for AI agents. The output includes tables, columns, relationships, views, functions, jobs, connections, and a small version/features summary, with hard limits to keep it dense.

```go
cfg := tinysql.DefaultAgentContextConfig()
cfg.MaxTables = 8
cfg.MaxChars = 4000

profile, err := tinysql.BuildAgentContext(context.Background(), db, "default", cfg)
if err != nil {
    log.Fatal(err)
}
fmt.Println(profile)
```

## Run tests

```bash
# no cache
go test ./... -count=1

# with coverage output
go test -coverprofile=coverage.out ./...
```

## Available Tools (`cmd/`)

The `cmd/` directory contains ready-to-use binaries for common workflows. See [cmd/README.md](./cmd/README.md) for the full list and build instructions. Each tool has its own README with detailed usage.

| Command | Description | README |
|---------|-------------|--------|
| `demo` | Creates tables, inserts sample data, and runs example queries | [📖](./cmd/demo/README.md) |
| `repl` | Interactive SQL REPL with multiple output formats | [📖](./cmd/repl/README.md) |
| `server` | HTTP JSON API + gRPC server with optional peer federation | [📖](./cmd/server/README.md) |
| `tinysql` | SQLite-compatible CLI (file or in-memory databases) | [📖](./cmd/tinysql/README.md) |
| `sqltools` | SQL formatter, validator, explain, and REPL | [📖](./cmd/sqltools/README.md) |
| `tinysqlpage` | HTTP server that renders SQL-driven web pages | [📖](./cmd/tinysqlpage/README.md) |
| `studio` | Desktop GUI built with Wails | [📖](./cmd/studio/README.md) |
| `wasm_browser` | tinySQL compiled to WebAssembly for browsers | [📖](./cmd/wasm_browser/README.md) |
| `wasm_node` | tinySQL compiled to WebAssembly for Node.js | [📖](./cmd/wasm_node/README.md) |
| `query_files` | Query CSV / JSON / XML files with SQL (web UI + CLI) | [📖](./cmd/query_files/README.md) |
| `query_files_wasm` | WebAssembly build of query_files for the browser | [📖](./cmd/query_files_wasm/README.md) |
| `catalog_demo` | Demo of the catalog and job-scheduler APIs | [📖](./cmd/catalog_demo/README.md) |
| `debug` | SQL diagnostic tool — parse, execute and time statements | [📖](./cmd/debug/README.md) |
| `fsql` | Query the filesystem with SQL (TVFs: files, lines, csv_rows, json_rows) | [📖](./cmd/fsql/README.md) |
| `migrate` | Data pipeline CLI: import/export CSV/JSON + cross-database transfers | [📖](./cmd/migrate/README.md) |

## Repository Structure

For a guided overview of the repository layout, see [docs/repository-structure.md](./docs/repository-structure.md).

## Goals (and non-goals)

- Lightweight, educational SQL engine in pure Go
- Useful for embeddings, demos, and learning database internals
- Hardened enough for real embedded/single-process use (see "Production
  readiness" above: a content lock closing a real concurrency data race,
  automatic WAL durability under `ModeAdvancedWAL`, foreign key referential
  actions, panic recovery, parser depth limits) — but **not** a drop-in
  replacement for a multi-node, horizontally-scaled RDBMS like PostgreSQL or
  MySQL: there's no replication, no distributed transactions, and
  correctness-critical gaps remain (see "Limitations" and "Not yet
  implemented" below) that a team should evaluate before betting a business
  on it

## Requirements

- Go 1.25+ (see go.mod)

## DSN (Data Source Name) Format

When using the database/sql driver:

- **In-memory database**: `mem://?tenant=<tenant_name>`
- **File-based database (GOB snapshot)**: `file:/path/to/db.dat?tenant=<tenant_name>&autosave=1`
- **File-based database (any storage mode)**: `file:/path/to/dbdir?tenant=<tenant_name>&mode=json` (or `disk`, `hybrid`, `wal`, `advanced_wal`)

Parameters:
- `tenant` - Tenant name for multi-tenancy (required)
- `autosave` - Auto-save to file (optional, for file-based databases; ignored when `mode` selects a non-memory storage mode, which always persists via its own backend)
- `mode` - Any `storage.StorageMode` string (see "Storage modes" above); opens via `storage.OpenDB` instead of the default GOB-snapshot behavior. Requires a `file:` DSN with a path.
- `pool_readers` / `pool_writers` - tinySQL reader/writer pool sizes (optional)
- `busy_timeout` - wait timeout for busy pools, e.g. `250ms`, `2s`, or `250` (milliseconds)

Best-practice settings split:
- DSN (`tenant`, `autosave`, `mode`, `pool_*`, `busy_timeout`) configures tinySQL driver behavior
- `database/sql` pool settings (`SetMaxOpenConns`, `SetMaxIdleConns`, lifetimes) configure connection pooling
- per-request/query timeout should be passed via `context.WithTimeout(...)` to `PingContext` / `ExecContext` / `QueryContext`

## SQLite Feature Gaps

TinySQL implements a broad SQL dialect. The table below summarises the current
status relative to SQLite.

### Already supported in tinySQL

| Feature | Notes |
|---------|-------|
| SELECT / INSERT / UPDATE / DELETE | Full DML, including `INSERT` / `UPDATE` / `DELETE … RETURNING` |
| INNER / LEFT / RIGHT / FULL OUTER / CROSS JOIN | All standard join types |
| GROUP BY, HAVING, ORDER BY, LIMIT / OFFSET | `LIMIT`/`OFFSET` accept `ALL` or any constant expression (e.g. `LIMIT 2 + 3`); the SQL:2008 `OFFSET n ROWS FETCH {FIRST\|NEXT} m ROWS ONLY` form is also supported |
| `PIVOT` | `FROM t [WHERE ...] PIVOT (agg(expr) FOR col IN (v1 [AS a1], v2 [AS a2], ...))`; single aggregate, static value list |
| Subqueries and CTEs (`WITH`) | Including `WITH RECURSIVE` |
| Window functions (`OVER`, `PARTITION BY`, frame specs) | ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, MOVING_SUM, MOVING_AVG |
| Aggregates | COUNT, SUM, AVG, MIN, MAX, MIN_BY, MAX_BY, VEC_AVG, … |
| JSON functions | `json_extract`, `json_set`, `json_array`, `json_object`, … |
| YAML functions | `YAML_PARSE`, `YAML_GET` |
| URL functions | `URL_PARSE`, `URL_ENCODE`, `URL_DECODE` |
| Hash functions | `HASH(algo, text)` — md5/sha1/sha256/sha512/fnv |
| Bitmap functions | `BITMAP_NEW/SET/GET/COUNT/OR/AND` |
| String / math / date functions | Extensive built-in library |
| Views (`CREATE VIEW`) | Stored and queryable, including CTE-backed view definitions |
| Materialized views | `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, lazy stale refresh, interval refresh, daily scheduled refresh, and optional `INVALIDATE ON CHANGE` |
| View dependencies | `sys.dependencies` / `catalog.dependencies` expose base-object dependencies for views and materialized views |
| Schema-qualified names | Objects can use names such as `sales.orders`; system views expose `schema`, `name`, and `full_name` where applicable |
| EXPLAIN | `EXPLAIN SELECT …` and `EXPLAIN CREATE MATERIALIZED VIEW …` return a compact logical plan |
| Column constraints | `PRIMARY KEY`, `UNIQUE` (column-level), and `FOREIGN KEY`/`REFERENCES` (column-level `col TYPE REFERENCES tbl(col)`/`col TYPE FOREIGN KEY REFERENCES tbl(col)`, or table-level `FOREIGN KEY (col) REFERENCES tbl(col)`, single-column only) are enforced on INSERT/UPDATE. `ON DELETE`/`ON UPDATE` support `CASCADE`, `SET NULL`, `RESTRICT`, and `NO ACTION`; the default (no clause) is `RESTRICT` — deleting or changing a referenced value with existing child rows is rejected unless a clause says otherwise |
| Indexes (`CREATE INDEX`) | Metadata stored; query planner no-op |
| **Full-Text Search (FTS)** | `CREATE VIRTUAL TABLE t USING fts(col1, col2)` with `FTS_MATCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`; `FTS_SEARCH` TVF for cached, whole-row ranked search on any table; `ROW_TO_TEXT()` for ad-hoc whole-row substring search in `WHERE` |
| **Triggers** | `CREATE TRIGGER … BEFORE/AFTER INSERT/UPDATE/DELETE ON table FOR EACH ROW BEGIN … END` |
| MVCC + WAL | Snapshot isolation, crash-safe write-ahead log; the database/sql driver supports `BEGIN`, `COMMIT`, and `ROLLBACK` on a single connection |
| Multi-tenancy | Isolated namespaces inside one process |
| Job scheduler | `CREATE JOB` for periodic or one-shot SQL |
| Vector search | `VEC_SEARCH` / `VEC_TOP_K` TVFs with cosine/L2/dot/manhattan and cached `ivf`/`hnsw` indexes; `VEC_WARM` prebuilds those indexes; `VEC_AVG` aggregate |
| RAG helpers | `RECENCY_SCORE`, `RAG_HYBRID_SCORE`, `RAG_RANK_SCORE`, `RAG_CONTEXT`, `RAG_CONTEXT_FROM` |
| Regex functions | `REGEXP_MATCH`, `REGEXP_EXTRACT`, `REGEXP_REPLACE` |
| Virtual system tables | `SELECT * FROM sys.tables`, `sys.columns`, `sys.triggers`, … |
| SQLite compatibility | `sqlite_schema`, `sqlite_master`, and common introspection PRAGMAs |
| Table-valued functions | Extensible via `RegisterExternalTableFunc` |
| Data types | INT, FLOAT, TEXT, BOOL, DATE, TIMESTAMP, UUID, BLOB, JSON, JSONB, VECTOR, YAML, URL, HASH, BITMAP, GEOMETRY, DECIMAL, MONEY, … |

### SQLite compatibility quick start

tinySQL exposes SQLite-style metadata for tools and ORMs that probe embedded
databases before running application queries:

```sql
SELECT type, name, tbl_name, sql
FROM sqlite_schema
WHERE type IN ('table', 'view');

SELECT name, sql
FROM sqlite_master
WHERE name = 'users';

PRAGMA table_info(users);
PRAGMA table_xinfo('users');
PRAGMA table_list;
PRAGMA database_list;
PRAGMA journal_mode;
PRAGMA foreign_keys = ON;
PRAGMA integrity_check;
PRAGMA compile_options;
```

The current PRAGMA layer is intentionally introspection-first. Settings such as
`foreign_keys = ON` are accepted for compatibility, while constraint enforcement
continues to be governed by tinySQL's engine semantics.

### Views and materialized views quick start

Views store and re-run their query definition. The definition can include CTEs:

```sql
CREATE VIEW paid_customer_totals AS
WITH paid_orders AS (
  SELECT customer_id, amount
  FROM orders
  WHERE status = 'paid'
)
SELECT customer_id, SUM(amount) AS total
FROM paid_orders
GROUP BY customer_id;

SELECT customer_id, total
FROM paid_customer_totals
ORDER BY customer_id;
```

Materialized views store the query result in an internal cache table. They can
be refreshed manually, lazily when stale, or by scheduler policies:

```sql
CREATE MATERIALIZED VIEW paid_customer_totals_mv AS
WITH paid_orders AS (
  SELECT customer_id, amount
  FROM orders
  WHERE status = 'paid'
)
SELECT customer_id, SUM(amount) AS total
FROM paid_orders
GROUP BY customer_id
REFRESH ON STALE AFTER 6 HOURS
REFRESH EVERY 30 MINUTES
REFRESH DAILY AT '02:00' TIMEZONE 'Europe/Berlin'
INVALIDATE ON CHANGE
WITH DATA;

-- Force a complete rebuild of the materialized cache.
REFRESH MATERIALIZED VIEW paid_customer_totals_mv;

-- Inspect refresh policy and runtime metadata.
SELECT name, cache_table_name, last_refresh_at, refresh_every_ms, daily_at, last_error
FROM catalog.materialized_views;

-- Inspect base-object dependencies.
SELECT object_type, object_name, depends_on_schema, depends_on_name, depends_on_type
FROM sys.dependencies
WHERE object_name = 'paid_customer_totals_mv';
```

`WITH NO DATA` stores only the definition. Combined with
`REFRESH ON STALE AFTER ...`, the first read materializes the cache lazily.
`INVALIDATE ON CHANGE` is opt-in: writes to dependent base tables mark the
materialized view stale, and the next read refreshes the cache.

Existing views can be converted in either direction without rewriting the query:

```sql
ALTER VIEW paid_customer_totals
MATERIALIZE
REFRESH EVERY 15 MINUTES
WITH DATA;

ALTER MATERIALIZED VIEW paid_customer_totals TO VIEW;
```

For a single operational overview across tables, views, materialized views,
jobs, triggers, and registered functions, use `sys.objects` or
`catalog.objects`:

```sql
SELECT object_type, name, status, rows, is_stale, last_refresh_at, next_run_at, last_error
FROM sys.objects
ORDER BY object_type, name;
```

Schema-qualified objects are stored and reported with separate schema metadata:

```sql
CREATE TABLE sales.orders (id INT PRIMARY KEY, amount INT);
CREATE VIEW sales.large_orders AS
SELECT id, amount FROM sales.orders WHERE amount >= 100;

SELECT schema, name, full_name
FROM sys.tables
WHERE schema = 'sales';
```

Use `EXPLAIN` to inspect a compact logical plan without executing the query:

```sql
EXPLAIN
WITH recent AS (
  SELECT id, amount FROM sales.orders WHERE amount > 10
)
SELECT id
FROM recent
WHERE amount < 100
ORDER BY amount
LIMIT 5;
```

The `database/sql` driver also accepts transaction control commands on the
same connection:

```sql
BEGIN;
INSERT INTO sales.orders VALUES (3, 42);
COMMIT;
```

More copy-pasteable examples are available in `example_showcase.sql`; the
Go example in `view_examples_test.go` demonstrates the same lifecycle through
the public API.

### Full-Text Search quick start

```sql
-- Create a virtual FTS table (inverted index maintained automatically)
CREATE VIRTUAL TABLE docs USING fts(title, body);

-- Insert documents
INSERT INTO docs VALUES ('Getting Started', 'A guide to tinySQL queries');
INSERT INTO docs VALUES ('Advanced Joins', 'Window functions and recursive CTEs');

-- Boolean match
SELECT title FROM docs WHERE FTS_MATCH(body, 'tinySQL') = 1;

-- BM25-ranked search
SELECT title, FTS_RANK(body, 'guide queries') AS score
FROM docs
ORDER BY score DESC;

-- Highlighted snippet
SELECT FTS_SNIPPET(body, 'tinySQL') AS excerpt FROM docs;
```

`FTS_SEARCH(table, query, k [, column, ...])` runs a ranked search directly
against any regular table — no `CREATE VIRTUAL TABLE` required. With no
column arguments it searches **every column of the row**, not just text
ones (a numeric order ID or status code is searchable too); pass explicit
column names to restrict the search. Repeated calls against an unchanged
table reuse a per-table tokenization cache (invalidated automatically on
INSERT/UPDATE/DELETE), so it's cheap to call from something like a live
search box that re-queries per keystroke:

```sql
CREATE TABLE articles (id INT, title TEXT, body TEXT);
INSERT INTO articles VALUES (1, 'Go Programming', 'Go is a fast compiled language');
INSERT INTO articles VALUES (2, 'Python Tutorial', 'Python is dynamic and popular');

-- Whole-row search, BM25-ranked, top 2
SELECT id, title, _fts_score, _fts_rank
FROM FTS_SEARCH('articles', 'programming language', 2)
ORDER BY _fts_rank;

-- Restrict to specific columns
SELECT id FROM FTS_SEARCH('articles', 'dynamic', 5, 'body');
```

For ad-hoc substring search across a whole row inside an ordinary `WHERE`
clause — no ranking, no setup, combinable with other conditions —
`ROW_TO_TEXT()` concatenates every column of the current row:

```sql
SELECT * FROM orders
WHERE ROW_TO_TEXT() LIKE '%acme corp%' AND status = 'open';
```

### Trigger quick start

```sql
CREATE TABLE orders (id INT, total FLOAT, status TEXT);
CREATE TABLE audit_log (order_id INT, msg TEXT, ts TIMESTAMP);

CREATE TRIGGER orders_after_insert
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
  INSERT INTO audit_log VALUES (NEW.id, 'created', NOW());
END;

INSERT INTO orders VALUES (1, 99.99, 'new');
SELECT * FROM audit_log;
```

### Foreign key quick start

```sql
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);

-- Column-level, standard SQL shorthand:
CREATE TABLE orders (
  id INT PRIMARY KEY,
  customer_id INT REFERENCES customers(id) ON DELETE CASCADE
);

-- Equivalent table-level form (also supported):
CREATE TABLE shipments (
  id INT,
  order_id INT,
  FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL
);

INSERT INTO customers VALUES (1, 'Acme');
INSERT INTO orders VALUES (100, 1);
INSERT INTO shipments VALUES (1000, 100);

DELETE FROM customers WHERE id = 1;
-- ON DELETE CASCADE removes the order too, which transitively triggers
-- shipments' ON DELETE SET NULL — order_id becomes NULL, the row survives.
SELECT * FROM orders;    -- empty
SELECT * FROM shipments; -- order_id is NULL
```

**Gotcha:** the default, when a `FOREIGN KEY`/`REFERENCES` has no `ON DELETE`/`ON UPDATE`
clause at all, is `RESTRICT` — deleting a customer that still has orders (or
changing a referenced column's value) is rejected with an error, not
silently allowed. This matches the SQL standard's default but is worth
calling out since earlier tinySQL versions had no referential-action
enforcement at all and would have orphaned the child rows silently. Add
`ON DELETE CASCADE`/`SET NULL` (and `ON UPDATE CASCADE`/`SET NULL`) explicitly
wherever orphaning or cascading is the desired behavior. Composite
(multi-column) foreign keys are not supported — only a single local column
referencing a single column in another table.

### PIVOT quick start

`PIVOT` spreads the distinct values of one column into new output columns,
aggregating another column into each. Every other selected column becomes
an implicit `GROUP BY` key — you don't write `GROUP BY` yourself.

```sql
CREATE TABLE sales (region TEXT, category TEXT, amount INT);
INSERT INTO sales VALUES ('East', 'Electronics', 100);
INSERT INTO sales VALUES ('East', 'Furniture', 50);
INSERT INTO sales VALUES ('West', 'Electronics', 200);
INSERT INTO sales VALUES ('West', 'Furniture', 75);

-- One row per region, one column per category
SELECT *
FROM sales
PIVOT (SUM(amount) FOR category IN ('Electronics' AS electronics, 'Furniture' AS furniture));

-- WHERE filters the source rows before pivoting
SELECT *
FROM sales
WHERE amount >= 100
PIVOT (SUM(amount) FOR category IN ('Electronics' AS electronics));
```

Scope: one aggregate function and a static (literal) `IN (...)` list — no
dynamic pivot driven by a subquery, and no combining with an explicit
`GROUP BY` on the same query.

**Gotcha:** *every* source column other than the pivot column and the
aggregated value column becomes an implicit `GROUP BY` key — including a
primary key or other per-row-unique column, if the source table has one.
`SELECT *` won't hide it: grouping happens on the underlying row set before
the outer `SELECT` list is applied. If your table has an `id` column,
project it out via a subquery before pivoting:

```sql
SELECT * FROM (SELECT region, category, amount FROM sales) AS s
PIVOT (SUM(amount) FOR category IN ('Electronics' AS electronics));
```

### Vectors

`VEC_SEARCH` and `VEC_TOP_K` perform k-nearest-neighbour search. By default they use exact top-k search; pass a sixth argument (`'ivf'` or `'hnsw'`) to use cached approximate vector indexes for larger RAG corpora. `VEC_AVG` computes the element-wise average of a set of vectors, useful for building centroid embeddings:

```sql
-- Store embeddings
CREATE TABLE embeddings (id INT, label TEXT, vec VECTOR);

-- Find the 5 nearest neighbours to a query vector
SELECT id, label, _vec_distance
FROM VEC_SEARCH('embeddings', 'vec', '[0.1, 0.2, 0.3]', 5, 'cosine')
ORDER BY _vec_rank;

-- Approximate indexed retrieval for larger RAG tables
SELECT id, label, _vec_distance
FROM VEC_SEARCH('embeddings', 'vec', '[0.1, 0.2, 0.3]', 20, 'cosine', 'hnsw')
ORDER BY _vec_rank;

SELECT id, label, _vec_distance
FROM VEC_SEARCH('embeddings', 'vec', '[0.1, 0.2, 0.3]', 20, 'cosine', 'ivf')
ORDER BY _vec_rank;

-- Compute centroid of a cluster
SELECT VEC_AVG(vec) AS centroid FROM embeddings WHERE label = 'science';
```

`ivf`/`hnsw` indexes are built lazily on first use per (table, column,
metric) and cached until the table's version changes. For a bulk-load-then-serve
workload, call `VEC_WARM` right after loading to pay that build cost once,
up front, instead of on whichever query happens to run first:

```sql
-- Prebuild the HNSW index and column cache for cosine search; returns one
-- row with row_count, vector_count, dims for confirmation.
SELECT * FROM VEC_WARM('embeddings', 'vec', 'cosine', 'hnsw');
```

### RAG retrieval with recency

For RAG-style ranking that balances semantic similarity and freshness, tinySQL adds:

- `RECENCY_SCORE(timestamp, half_life_days [, now])`
- `RAG_HYBRID_SCORE(similarity, timestamp, half_life_days [, sim_weight, now])`
- `RAG_RANK_SCORE(similarity, timestamp, half_life_days, quality [, sim_weight, recency_weight, quality_weight, now])`
- `RAG_CONTEXT(source, doc_id_col, chunk_index_col, doc_id, chunk_index, before [, after])`
- `RAG_CONTEXT_FROM(source, doc_id_col, chunk_index_col, hits, hit_doc_id_col, hit_chunk_index_col, before [, after])`

```sql
CREATE TABLE docs (
  id INT,
  title TEXT,
  created_at TEXT, -- ISO-like timestamp string is fine
  embedding VECTOR
);

-- Similarity + recency blend (70% similarity, 30% recency by default)
SELECT id, title,
       RAG_HYBRID_SCORE(
         VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, 0.2, 0.3]')),
         created_at,
         30,
         0.7
       ) AS rag_score
FROM docs
ORDER BY rag_score DESC
LIMIT 5;

-- Optional strict freshness filter
SELECT id, title,
       RAG_HYBRID_SCORE(
         VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, 0.2, 0.3]')),
         created_at,
         30,
         0.8,
         '2026-01-31 00:00:00'
       ) AS rag_score
FROM docs
WHERE
  VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, 0.2, 0.3]')) > 0.4
  AND RECENCY_SCORE(created_at, 30, '2026-01-31 00:00:00') > 0.25
ORDER BY rag_score DESC;
```

For pure top-k similarity, first use `VEC_SEARCH` and apply recency scoring after that (this avoids re-scoring all rows during ordering):

```sql
WITH topk AS (
  SELECT * FROM VEC_SEARCH('docs', 'embedding', VEC_FROM_JSON('[0.1, 0.2, 0.3]'), 40, 'cosine', 'hnsw')
)
SELECT id, created_at, title,
       RAG_HYBRID_SCORE(
         VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, 0.2, 0.3]')),
         created_at,
         30,
         0.7
       ) AS rag_score
FROM topk
ORDER BY rag_score DESC
LIMIT 20;
```

For chunked documents, expand the winning chunks with local context before the final answer-building step:

```sql
WITH topk AS (
  SELECT doc_id, chunk_index, _vec_rank
  FROM VEC_SEARCH('docs', 'embedding', VEC_FROM_JSON('[0.1, 0.2, 0.3]'), 8, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset
FROM RAG_CONTEXT_FROM('docs', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1, 1)
ORDER BY _context_rank;
```

For ranked RAG over similarity, recency and quality:

```sql
SELECT id, title,
       RAG_RANK_SCORE(
         VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, 0.2, 0.3]')),
         created_at,
         30,
         quality,
         0.65,
         0.25,
         0.10
       ) AS rag_score
FROM docs
ORDER BY rag_score DESC
LIMIT 20;
```

Benchmarks for vector/RAG behavior are in `internal/engine/vector_search_benchmark_test.go`. Run:

```bash
go test ./internal/engine -run '^$' -bench 'Benchmark(WhereVectorAndSimpleCondition|OrderByVectorLimit|CompareTopK_VecSearchVsOrderBy|VecSearchCosineTopK|RAGRankScoreOrderByLimit|RAGContextFromTopK)' -count=1
```

For SIMD vector math and ANN index-mode comparisons:

```bash
go test ./internal/engine -run '^$' -bench 'BenchmarkVector(Dot768|DotUnrolled768|L2Squared768|L2SquaredUnrolled768|L1Distance768|L1DistanceUnrolled768)|BenchmarkVecSearchIndexModesSameTable' -benchmem -count=3
```

GitHub Actions also runs `Vector SIMD (linux/amd64)` on an Ubuntu x86_64 runner with `GOARCH=amd64` and `GOAMD64=v1`, and uploads the benchmark output as the `vector-amd64-bench` artifact.

**SIMD kernel coverage** (`internal/engine/vector_math_*.{go,s}`), used automatically by `VEC_DOT`/`VEC_COSINE_*`, `VEC_L2_DISTANCE`/`VEC_SEARCH`, and `VEC_MANHATTAN_DISTANCE`/`VEC_DISTANCE(...,'manhattan')` above a 128-element threshold (smaller vectors use the portable 4-way-unrolled Go path, where SIMD call/setup overhead isn't worth it):

| Metric | amd64 (SSE2) | arm64 (NEON) | Fallback |
|---|---|---|---|
| Dot product | ✅ `vectorDotSSE2` | ✅ `vectorDotNEON` | 4-way unrolled Go |
| L2 squared distance | ✅ `vectorL2SquaredSSE2` | ✅ `vectorL2SquaredNEON` | 4-way unrolled Go |
| Manhattan (L1) distance | ✅ `vectorL1SSE2` (~4.5x over the unrolled fallback at 768 dims) | Not yet — see note below | 4-way unrolled Go |

The arm64 L1 kernel is intentionally left unimplemented rather than shipped unverified: a hand-derived NEON float-abs encoding can't be validated for correctness without running it on real ARM64 hardware, and a wrong bit pattern would silently corrupt distance results rather than fail to build. Contributions with ARM64 hardware to test on are welcome — see `vectorL1Kernel` in `vector_math_arm64.go`.

### Not yet implemented

| Feature | SQLite equivalent | Priority |
|---------|-------------------|----------|
| **Composite (multi-column) constraints** | `PRIMARY KEY (a, b)`, `FOREIGN KEY (a, b) REFERENCES other(a, b)` | Medium |
| **CHECK constraints** | `CHECK (expr)` in `CREATE TABLE` | Medium |
| **UPSERT / ON CONFLICT** | `INSERT OR REPLACE`, `INSERT … ON CONFLICT DO UPDATE/NOTHING` | Medium |
| **Generated / computed columns** | `col AS (expr) STORED/VIRTUAL` | Low |
| **SAVEPOINT / nested transactions** | `SAVEPOINT sp; ROLLBACK TO sp; RELEASE sp` | Low |
| **Broader PRAGMA coverage** | `PRAGMA cache_size`, `PRAGMA page_size`, `PRAGMA optimize`, … | Low |
| **ATTACH / DETACH DATABASE** | Cross-file queries | Low |
| **Partial indexes** | `CREATE INDEX … WHERE expr` | Low |
| **Persistent ANN vector index files** | HNSW/IVF indexes currently rebuild in memory per table version | Low |
| **WITHOUT ROWID tables** | Storage optimisation | Low |
| **VACUUM** | Reclaim space, re-pack storage | Low |

## Limitations

TinySQL is designed primarily for education, embedding, and single-process
workloads. Known, current limitations (as opposed to planned features — see
"Not yet implemented" above):

- **No true statement-level rollback in `Execute`.** If a multi-row
  INSERT/UPDATE/DELETE fails partway through (e.g. a later row violates a
  constraint), rows already applied earlier in that same statement stay
  applied — there's no automatic all-or-nothing undo. Real atomicity across
  multiple statements is only available via `internal/driver`'s
  `BeginTx`/`Commit`/`Rollback`, which clones an MVCC snapshot rather than
  mutating live state until commit.
- **No composite (multi-column) `PRIMARY KEY`/`FOREIGN KEY`.** Only a single
  local column can participate in either constraint; see "Foreign key quick
  start" above and "Not yet implemented".
- **No `CHECK` constraints or `UPSERT`/`ON CONFLICT`.**
- **`CREATE INDEX` is metadata-only.** It's parsed and stored but the query
  planner never uses it — every `WHERE`/`JOIN` lookup is a full table scan
  regardless of declared indexes.
- **WAL durability differs by mode.** `ModeAdvancedWAL` logs every
  INSERT/UPDATE/DELETE automatically; the older `ModeWAL` still requires the
  caller to snapshot-diff and log manually. See the WAL durability note
  under "Production readiness".
- **Single-process only.** No built-in replication, clustering, or sharding;
  concurrent access is one `*storage.DB` shared across goroutines in the
  same process, not across machines.
- **The engine's concurrency lock is coarse, not per-table.** A write to one
  table blocks concurrent reads/writes on every other table in the same
  `DB`, trading some parallelism for a simple, easy-to-audit correctness
  guarantee (see "Production readiness").
- **No SAVEPOINT / nested transactions.**
- **`cmd/server` ships with authentication off by default.** See the
  "Security checklist" above — this is a deliberate opt-in design, not a
  bug, but it means an unconfigured server run beyond localhost is an open
  SQL endpoint.
- **No NEON kernel for Manhattan/L1 vector distance on arm64** (Dot and L2
  have one); it uses the portable unrolled Go path there. See "SIMD kernel
  coverage" under Vectors.

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests multiple times to check consistency
go test -v -count=3 ./...
```

## Contributing

This is an educational project. Contributions that improve code clarity, add comprehensive examples, or enhance the learning experience are welcome.

## Educational Goals

TinySQL demonstrates:

- SQL parsing and AST construction
- Query execution and optimization basics
- Database storage concepts
- Go's database/sql driver interface
- 3-valued logic (NULL semantics)
- JSON / YAML data handling in SQL
- Full-Text Search with BM25 ranking
- Trigger execution (BEFORE/AFTER INSERT/UPDATE/DELETE)
- Recursive CTEs (WITH RECURSIVE)
- Window functions (OVER, PARTITION BY, frame specs)
- Vector similarity search (cosine, L2, dot, Manhattan)
- Multi-tenancy patterns

Perfect for computer science students, developers learning database internals, or anyone who wants to understand how SQL databases work under the hood.
