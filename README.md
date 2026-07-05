# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![Go Report Card](https://goreportcard.com/badge/github.com/SimonWaldherr/tinySQL)](https://goreportcard.com/report/github.com/SimonWaldherr/tinySQL)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

TinySQL is a lightweight SQL database engine written in pure Go. It is built for
learning database internals, embedding in Go programs, demos, tests, and
single-process workloads that need a small SQL layer without external services.

Demo: [youtu.be/W28-aBk3BL0](https://youtu.be/W28-aBk3BL0)

## Install

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Requirements: Go 1.25+.

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    tsql "github.com/SimonWaldherr/tinySQL"
)

func main() {
    db := tsql.NewDB()
    ctx := context.Background()

    for _, sql := range []string{
        `CREATE TABLE users (id INT, name TEXT)`,
        `INSERT INTO users VALUES (1, 'Alice')`,
    } {
        stmt, _ := tsql.ParseSQL(sql)
        _, _ = tsql.Execute(ctx, db, "default", stmt)
    }

    stmt, _ := tsql.ParseSQL(`SELECT id, name FROM users`)
    rs, _ := tsql.Execute(ctx, db, "default", stmt)

    for _, row := range rs.Rows {
        fmt.Println(tsql.GetVal(row, "id"), tsql.GetVal(row, "name"))
    }
}
```

## database/sql Driver

```go
import (
    "context"
    "database/sql"

    _ "github.com/SimonWaldherr/tinySQL/driver"
)

func open() (*sql.DB, error) {
    return sql.Open("tinysql", "mem://?tenant=default")
}

func run(db *sql.DB) error {
    _, err := db.ExecContext(context.Background(), `CREATE TABLE t (id INT, name TEXT)`)
    return err
}
```

Common DSNs:

| DSN | Use |
|---|---|
| `mem://?tenant=default` | In-memory database |
| `file:/path/to/db.gob?tenant=default&autosave=1` | GOB snapshot file |
| `file:/path/to/dbdir?tenant=default&mode=json` | JSON table files |
| `file:/path/to/dbdir?tenant=default&mode=advanced_wal` | Row-level WAL mode |

External projects should import `github.com/SimonWaldherr/tinySQL/driver`, not
`internal/driver`.

## Storage Modes

All modes use the same SQL engine and `*DB` API.

| Mode | String | Notes |
|---|---|---|
| `ModeMemory` | `memory` | Default; in-memory, optional GOB snapshot via `Path` |
| `ModeDisk` | `disk` | One GOB file per table |
| `ModeJSON` | `json` | One readable JSON file per table |
| `ModeWAL` | `wal` | Older WAL mode; manual logging |
| `ModeAdvancedWAL` | `advanced_wal` | Row-level WAL logged automatically on writes |
| `ModeIndex` | `index` | Schemas in memory, rows on disk |
| `ModeHybrid` | `hybrid` | LRU buffer pool with spill-to-disk behavior |

JSON mode example:

```go
db, err := tsql.OpenDB(tsql.StorageConfig{
    Mode: tsql.ModeJSON,
    Path: "./data/tinysql",
})
```

Read-only serve mode example:

```go
// Load phase: write a snapshot.
db, _ := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: "./data/db.gob"})
// ... bulk INSERT/UPDATE via tsql.Execute ...
db.Close()

// Serve phase: reopen the same snapshot read-only.
serveDB, _ := tsql.OpenDB(tsql.StorageConfig{
    Mode:     tsql.ModeMemory,
    Path:     "./data/db.gob",
    ReadOnly: true,
})
defer serveDB.Close()

warmStmt, _ := tsql.ParseSQL(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
tsql.Execute(context.Background(), serveDB, "default", warmStmt)
```

`ReadOnly` rejects `INSERT`, `UPDATE`, `DELETE`, and DDL. `SELECT`, `EXPLAIN`,
and `PRAGMA` still run.

## Features

- SELECT, INSERT, UPDATE, DELETE, RETURNING, CTEs, subqueries, joins, grouping,
  window functions, PIVOT, EXPLAIN, and common SQLite-compatible PRAGMAs.
- Views, materialized views, triggers, table-valued functions, system catalog
  views, job scheduling, and multi-tenancy.
- Constraints: single-column PRIMARY KEY, UNIQUE, and FOREIGN KEY with
  referential actions.
- Built-in functions for JSON, YAML, URLs, hashes, bitmaps, regex, text, math,
  dates, full-text search, vector search, and RAG scoring.
- Operational hooks for health checks, lifecycle management, read-only mode,
  RBAC, audit logging, and encryption at rest for `ModeDisk`/`ModeJSON`.

For a broader feature reference, see [FUNCTIONS.sql](./FUNCTIONS.sql),
[example_showcase.sql](./example_showcase.sql), and the Go tests.

## Command Line Tools

The `cmd/` directory contains small tools and demos. See
[cmd/README.md](./cmd/README.md) for the full list.

Common entries:

| Command | Purpose |
|---|---|
| `cmd/tinysql` | SQLite-like CLI |
| `cmd/repl` | Interactive SQL REPL |
| `cmd/server` | HTTP JSON API and gRPC server |
| `cmd/tinysqld` | Lightweight admin/health daemon |
| `cmd/sqltools` | Format, validate, explain, and REPL helpers |
| `cmd/query_files` | Query CSV, JSON, and XML files with SQL |
| `cmd/fsql` | Query filesystem metadata and file contents with SQL |
| `cmd/studio` | Desktop GUI |
| `cmd/wasm_browser` | Browser WebAssembly build |

Security note: `cmd/server` defaults to authentication off and listens on all
interfaces. Use `-auth`, bind to localhost, and configure TLS before exposing it
outside a trusted environment.

## Limitations

TinySQL is not a PostgreSQL/MySQL replacement. Important current limits:

- Single-process database engine; no built-in replication, clustering,
  failover, sharding, or distributed transactions.
- No true statement-level rollback for partially applied multi-row statements
  through direct `Execute`.
- No composite primary keys or composite foreign keys.
- No CHECK constraints, UPSERT/ON CONFLICT, SAVEPOINT, ATTACH/DETACH, VACUUM,
  partial indexes, generated columns, or persistent ANN vector index files.
- `CREATE INDEX` stores metadata, but the planner does not use indexes yet.
- RBAC checks are coarse and single-table oriented.
- Encryption at rest currently covers `ModeDisk`/`ModeJSON` table files, not
  every backend mode or metadata file.

Evaluate these limits before using TinySQL for production-critical data.

## Development

```bash
go test ./... -count=1
go test -coverprofile=coverage.out ./...
```

Useful docs:

- [Repository structure](./docs/repository-structure.md)
- [Developer integration](./docs/developer-integration.md)
- [Product forms](./docs/product-forms.md)
- [Benchmarks](./BENCHMARKS.md)

## Project Goals

TinySQL is primarily an educational and embeddable SQL engine. It demonstrates
SQL parsing, AST construction, execution, storage backends, Go's `database/sql`
driver interface, full-text search, triggers, recursive CTEs, window functions,
vector search, RAG helpers, and multi-tenant database patterns.
