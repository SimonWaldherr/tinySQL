# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

TinySQL is a lightweight SQL database engine written in pure Go. It is built for
learning database internals, embedding in Go programs, demos, tests, and
single-process workloads that need a small SQL layer without external services.

Demos:

- Video: [youtu.be/W28-aBk3BL0](https://youtu.be/W28-aBk3BL0)
- Browser playground: [simonwaldherr.github.io/tinySQL](https://simonwaldherr.github.io/tinySQL/)

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

## Features

- SELECT, INSERT, UPDATE, DELETE, RETURNING, CTEs, subqueries, joins, grouping,
  window functions, PIVOT, EXPLAIN, and common SQLite-compatible PRAGMAs.
- Views, materialized views, triggers, table-valued functions, system catalog
  views, job scheduling, and multi-tenancy.
- Constraints: single-column PRIMARY KEY, UNIQUE, and FOREIGN KEY with
  referential actions.
- Built-in functions for JSON, YAML, URLs, hashes, bitmaps, regex, text, math,
  dates, full-text search, vector search, and RAG scoring.
- Geodata imports and SQL helpers for GeoJSON, KML, OSM XML, Shapefiles,
  MBTiles, routing graphs, points, distance, radius, and bounding-box queries.
- Operational hooks for health checks, lifecycle management, read-only mode,
  RBAC, audit logging, and encryption at rest for `ModeDisk`/`ModeJSON`.

For a broader feature reference, see [FUNCTIONS.sql](./FUNCTIONS.sql),
[example_showcase.sql](./example_showcase.sql), and the Go tests.

## Guides

Pick the guide that matches what you're building:

| Guide | Scenario |
|---|---|
| [RAG / AI usage](./docs/rag-guide.md) | Retrieval-augmented generation: vector search, hybrid retrieval, reranking, context expansion, LLM agent integration |
| [Developer integration](./docs/developer-integration.md) | Embedding TinySQL in Go, using the `database/sql` driver, running it as WASM in the browser or a custom frontend |
| [Storage & persistence](./docs/storage-guide.md) | Storage modes, DSNs, connection strings, read-only serving |
| [Command line tools](./docs/cli-guide.md) | Using the `cmd/tinysql` CLI, REPL, servers, file-query tools, and the browser playground |
| [Development guide](./docs/development-guide.md) | Running tests, the Makefile, building the WASM demo |
| [Repository structure](./docs/repository-structure.md) | Codebase layout for contributors |
| [Benchmarks](./BENCHMARKS.md) | TinySQL-vs-SQLite numbers and internal optimization history |

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

## Project Goals

TinySQL is primarily an educational and embeddable SQL engine. It demonstrates
SQL parsing, AST construction, execution, storage backends, Go's `database/sql`
driver interface, full-text search, triggers, recursive CTEs, window functions,
vector search, RAG helpers, and multi-tenant database patterns.
