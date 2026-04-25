# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![Go Report Card](https://goreportcard.com/badge/github.com/SimonWaldherr/tinySQL)](https://goreportcard.com/report/github.com/SimonWaldherr/tinySQL)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

## 🎥 Demo

[![Watch the video](https://img.youtube.com/vi/W28-aBk3BL0/hqdefault.jpg)](https://youtu.be/W28-aBk3BL0)

TinySQL is a lightweight, educational SQL database engine written in pure Go. It implements a comprehensive subset of SQL features using only Go's standard library, making it perfect for learning database internals and for applications that need a simple embedded SQL database.

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
    "database/sql"
    "fmt"
    tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

func main() {
    db, _ := sql.Open(tsqldriver.DriverName, "mem://?tenant=default")
    defer db.Close()

    db.Exec(`CREATE TABLE t (id INT, name TEXT)`)
    db.Exec(`INSERT INTO t VALUES (?, ?)`, 1, "Alice")

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

## Goals (and non-goals)

- Lightweight, educational SQL engine in pure Go
- Useful for embeddings, demos, and learning database internals
- Not intended as a production-grade relational database

## Requirements

- Go 1.25+ (see go.mod)

## DSN (Data Source Name) Format

When using the database/sql driver:

- **In-memory database**: `mem://?tenant=<tenant_name>`
- **File-based database**: `file:/path/to/db.dat?tenant=<tenant_name>&autosave=1`

Parameters:
- `tenant` - Tenant name for multi-tenancy (required)
- `autosave` - Auto-save to file (optional, for file-based databases)

## SQLite Feature Gaps

TinySQL implements a broad SQL dialect. The table below summarises the current
status relative to SQLite.

### Already supported in tinySQL

| Feature | Notes |
|---------|-------|
| SELECT / INSERT / UPDATE / DELETE | Full DML |
| INNER / LEFT / RIGHT / FULL OUTER / CROSS JOIN | All standard join types |
| GROUP BY, HAVING, ORDER BY, LIMIT / OFFSET | |
| Subqueries and CTEs (`WITH`) | Including `WITH RECURSIVE` |
| Window functions (`OVER`, `PARTITION BY`, frame specs) | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, MOVING_SUM, MOVING_AVG |
| Aggregates | COUNT, SUM, AVG, MIN, MAX, MIN_BY, MAX_BY, VEC_AVG, … |
| JSON functions | `json_extract`, `json_set`, `json_array`, `json_object`, … |
| YAML functions | `YAML_PARSE`, `YAML_GET` |
| URL functions | `URL_PARSE`, `URL_ENCODE`, `URL_DECODE` |
| Hash functions | `HASH(algo, text)` — md5/sha1/sha256/sha512/fnv |
| Bitmap functions | `BITMAP_NEW/SET/GET/COUNT/OR/AND` |
| String / math / date functions | Extensive built-in library |
| Views (`CREATE VIEW`) | Stored and queryable |
| Indexes (`CREATE INDEX`) | Metadata stored; query planner no-op |
| **Full-Text Search (FTS)** | `CREATE VIRTUAL TABLE t USING fts(col1, col2)` with `FTS_MATCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25` |
| **Triggers** | `CREATE TRIGGER … BEFORE/AFTER INSERT/UPDATE/DELETE ON table FOR EACH ROW BEGIN … END` |
| MVCC + WAL | Snapshot isolation, crash-safe write-ahead log |
| Multi-tenancy | Isolated namespaces inside one process |
| Job scheduler | `CREATE JOB` for periodic or one-shot SQL |
| Vector search | `VEC_SEARCH` / `VEC_TOP_K` TVFs with cosine/L2/dot/manhattan; `VEC_AVG` aggregate |
| Regex functions | `REGEXP_MATCH`, `REGEXP_EXTRACT`, `REGEXP_REPLACE` |
| Virtual system tables | `SELECT * FROM sys.tables`, `sys.columns`, `sys.triggers`, … |
| Table-valued functions | Extensible via `RegisterExternalTableFunc` |
| Data types | INT, FLOAT, TEXT, BOOL, DATE, TIMESTAMP, UUID, BLOB, JSON, JSONB, VECTOR, YAML, URL, HASH, BITMAP, GEOMETRY, DECIMAL, MONEY, … |

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

### Vectors

`VEC_SEARCH` and `VEC_TOP_K` perform k-nearest-neighbour search. `VEC_AVG` computes the element-wise average of a set of vectors, useful for building centroid embeddings:

```sql
-- Store embeddings
CREATE TABLE embeddings (id INT, label TEXT, vec VECTOR);

-- Find the 5 nearest neighbours to a query vector
SELECT id, label, _vec_distance
FROM VEC_SEARCH('embeddings', 'vec', '[0.1, 0.2, 0.3]', 5, 'cosine')
ORDER BY _vec_rank;

-- Compute centroid of a cluster
SELECT VEC_AVG(vec) AS centroid FROM embeddings WHERE label = 'science';
```

### Not yet implemented

| Feature | SQLite equivalent | Priority |
|---------|-------------------|----------|
| **FOREIGN KEY constraints** | `FOREIGN KEY (col) REFERENCES other(col)` + enforcement | Medium |
| **CHECK constraints** | `CHECK (expr)` in `CREATE TABLE` | Medium |
| **RETURNING clause** | `INSERT … RETURNING`, `UPDATE … RETURNING` | Medium |
| **UPSERT / ON CONFLICT** | `INSERT OR REPLACE`, `INSERT … ON CONFLICT DO UPDATE/NOTHING` | Medium |
| **Generated / computed columns** | `col AS (expr) STORED/VIRTUAL` | Low |
| **SAVEPOINT / nested transactions** | `SAVEPOINT sp; ROLLBACK TO sp; RELEASE sp` | Low |
| **PRAGMA statements** | `PRAGMA journal_mode`, `PRAGMA foreign_keys = ON`, … | Low |
| **ATTACH / DETACH DATABASE** | Cross-file queries | Low |
| **Partial indexes** | `CREATE INDEX … WHERE expr` | Low |
| **Persistent ANN vector index** | HNSW/IVF — current `VEC_SEARCH` is a sequential scan | Low |
| **WITHOUT ROWID tables** | Storage optimisation | Low |
| **VACUUM** | Reclaim space, re-pack storage | Low |
| **`sqlite_master` / `sqlite_schema`** | Metadata table (tinySQL uses `sys.*` instead) | Low |

## Limitations

TinySQL is designed for educational purposes

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
