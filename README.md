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
- Geodata imports and SQL helpers for GeoJSON, KML, OSM XML, Shapefiles,
  MBTiles, routing graphs, points, distance, radius, and bounding-box queries.
- Operational hooks for health checks, lifecycle management, read-only mode,
  RBAC, audit logging, and encryption at rest for `ModeDisk`/`ModeJSON`.

For a broader feature reference, see [FUNCTIONS.sql](./FUNCTIONS.sql),
[example_showcase.sql](./example_showcase.sql), and the Go tests.

## Using TinySQL for RAG

TinySQL ships everything a single-process RAG (Retrieval-Augmented
Generation) pipeline needs — vector storage, SIMD-accelerated k-NN search,
BM25 full-text search, recency/quality scoring, and chunk-context expansion —
embedded in your Go program, with no external vector database. All examples
below run as-is.

### 1. Store chunks and embeddings

Declare the embedding column as `VECTOR` (alias: `EMBEDDING`). Embeddings come
from your embedding model; TinySQL stores them as `[]float64`.

```sql
CREATE TABLE chunks (
    doc_id      TEXT,
    chunk_index INT,
    chunk_text  TEXT,
    created_at  TEXT,
    quality     FLOAT,
    embedding   VECTOR
);

INSERT INTO chunks VALUES
    ('doc-1', 0, 'TinySQL is a lightweight SQL engine...',
     '2026-07-01 10:00:00', 0.9, VEC_FROM_JSON('[0.12, -0.03, 0.87]'));
```

From Go, pass the vector directly as a query parameter — both a `[]float64`
value and a JSON string through `VEC_FROM_JSON(?)` work with the
`database/sql` driver:

```go
vec := embed(chunkText) // []float64 from your embedding model
db.ExecContext(ctx, `INSERT INTO chunks VALUES (?, ?, ?, ?, ?, ?)`,
    docID, idx, chunkText, createdAt, quality, vec)
```

For memory-constrained setups, `VEC_TO_BYTES`/`VEC_FROM_BYTES` round-trip
vectors through a compact float32 encoding at half the storage cost.

### 2. Retrieve: VEC_SEARCH

`VEC_SEARCH(table, column, query_vector, k [, metric [, index]])` returns the
k nearest rows plus `_vec_distance` and `_vec_rank`:

```sql
SELECT doc_id, chunk_index, chunk_text, _vec_distance, _vec_rank
FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 5, 'cosine');
```

Metrics: `cosine` (default), `l2`/`euclidean`, `manhattan`/`l1`,
`dot`/`inner_product`. Index modes:

| Index | Behavior | Use when |
|---|---|---|
| `flat` (default) | Exact scan; SIMD + multi-core, column cache | Default choice; stays in low single-digit ms up to ~100k rows |
| `ivf` | Approximate (inverted file); ~2-3x faster than flat | Larger tables, small recall loss acceptable |
| `hnsw` | Approximate graph; fastest repeated queries, highest build cost | Static data, many queries; prebuild with `VEC_WARM` |

Indexes and column caches build lazily on first query and invalidate
automatically on writes. After a bulk load, prebuild them explicitly so no
query pays the one-time cost:

```sql
SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'hnsw');
```

Prefer `VEC_SEARCH` over `ORDER BY VEC_COSINE_SIMILARITY(...) LIMIT k` for
plain k-NN — it uses cached norms, a top-k heap, and a parallel scan
(~7x faster at 12k rows). The `ORDER BY` form is still fast and the right
tool when the ranking expression blends more than similarity.

### 3. Rerank: blend similarity with freshness and quality

Cosine *distance* converts to similarity as `1.0 - _vec_distance`.
`RAG_RANK_SCORE(similarity, ts, half_life_days, quality [, w_sim, w_rec, w_q])`
combines normalized similarity, exponential recency decay, and a quality
signal; `RAG_HYBRID_SCORE` (similarity + recency) and `RECENCY_SCORE` are the
simpler variants:

```sql
WITH hits AS (
    SELECT * FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 20, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text,
       RAG_RANK_SCORE(1.0 - _vec_distance, created_at, 30, quality, 0.65, 0.25, 0.10) AS score
FROM hits
ORDER BY score DESC
LIMIT 5;
```

Retrieve generously (k=20), rerank, then keep the top few — reranking is
cheap compared to a second retrieval round.

### 4. Expand context: neighboring chunks

LLM answers improve when retrieved chunks arrive with their surrounding text.
`RAG_CONTEXT_FROM` takes a hit set (a CTE or table) and returns each hit plus
its neighbors within the same document, annotated with `_hit_rank`,
`_context_offset` (position relative to the hit), and `_context_rank`:

```sql
WITH topk AS (
    SELECT doc_id, chunk_index
    FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 5, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset
FROM RAG_CONTEXT_FROM('chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1, 1)
ORDER BY _context_rank;
```

The trailing `1, 1` fetches one chunk before and one after each hit.
`RAG_CONTEXT` does the same for a single known chunk.

### 5. Hybrid retrieval: vectors + keywords

Embeddings miss exact identifiers, error codes, and rare terms; BM25 misses
paraphrases. Fuse both with reciprocal rank fusion (RRF) over `VEC_SEARCH`
and `FTS_SEARCH`:

```sql
SELECT c.doc_id, c.chunk_index, c.chunk_text,
       1.0/(60.0 + COALESCE(v._vec_rank, 1000))
     + 1.0/(60.0 + COALESCE(f._fts_rank, 1000)) AS rrf_score
FROM chunks c
LEFT JOIN (SELECT doc_id, chunk_index, _vec_rank
           FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 20, 'cosine')) v
    ON v.doc_id = c.doc_id AND v.chunk_index = c.chunk_index
LEFT JOIN (SELECT doc_id, chunk_index, _fts_rank
           FROM FTS_SEARCH('chunks', 'hnsw index build', 20)) f
    ON f.doc_id = c.doc_id AND f.chunk_index = c.chunk_index
WHERE v.doc_id IS NOT NULL OR f.doc_id IS NOT NULL
ORDER BY rrf_score DESC
LIMIT 5;
```

For a lighter variant, retrieve by vector and rerank with the scalar
`FTS_RANK` (BM25) inside one CTE:
`0.7 * (1.0 - _vec_distance) + 0.3 * FTS_RANK(chunk_text, 'query terms')`.
`FTS_SNIPPET` and `FTS_HIGHLIGHT` format the matched passages for prompts.

### 6. Serving and performance notes

- **Load once, serve read-only.** Bulk-insert into a snapshot, then reopen it
  with `ReadOnly: true` and run `VEC_WARM` at startup (full example under
  [Storage Modes](#storage-modes)).
- **Query-vector literals are free.** `VEC_FROM_JSON('[...]')` with a literal
  argument is folded to a constant at parse time — it is not re-parsed per
  row, so passing the query vector as JSON text costs nothing.
- **SIMD is automatic.** Distance kernels use AVX2+FMA on amd64 (detected at
  startup, SSE2 fallback) and NEON on arm64, with a portable fallback
  everywhere else; no build tags or cgo required.
- **Exposing the schema to an LLM agent:** `tsql.BuildAgentContext(...)`
  renders a compact, token-budgeted schema summary for system prompts, and
  `cmd/tinysql-mcp-server` serves the database over MCP.

See [BENCHMARKS.md](./BENCHMARKS.md) for measured numbers on the vector and
RAG query paths.

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
| `cmd/query_files_wasm` | Static browser playground used by gh-pages |
| `cmd/fsql` | Query filesystem metadata and file contents with SQL |
| `cmd/studio` | Desktop GUI |
| `cmd/wasm_browser` | Browser WebAssembly build |

Security note: `cmd/server` defaults to authentication off and listens on all
interfaces. Use `-auth`, bind to localhost, and configure TLS before exposing it
outside a trusted environment.

## Browser Playground

The [gh-pages playground](https://simonwaldherr.github.io/tinySQL/) is built
from `cmd/query_files_wasm`. It runs tinySQL as WebAssembly in the browser and
demonstrates the current feature set without a backend:

- local-first file analytics for CSV, JSON, JSONL/NDJSON, YAML, XML, Excel,
  GeoJSON, KML, OSM XML, and routing graph files;
- SQL joins, CTEs, window functions, exports, schema inspection, and persisted
  browser snapshots;
- geodata recipes for distance matrices, radius filters, bounding boxes, zones,
  routing graph edges, and node lookups;
- full-text search, vector search, hybrid retrieval, and in-memory stored
  procedure examples;
- shareable demo URLs where the SQL and sample data are encoded in the URL hash.

Build and publish helpers:

```bash
make build-gh-pages-demo
make update-gh-pages
make push-gh-pages
```

## Makefile

The repository `Makefile` wraps the common build, test, demo, and release
tasks. Run `make` or `make help` to list all documented targets.

Common workflow:

```bash
make deps
make verify-ci
make build-all
```

Useful targets:

| Target | Purpose |
|---|---|
| `make help` | Show all documented targets. |
| `make build` | Build the main `cmd/tinysql` CLI into `bin/tinysql`. |
| `make build-all` | Build the main CLI and the common command demos into `bin/`. |
| `make build-query-files-wasm` | Build the browser playground WASM artifacts. |
| `make build-gh-pages-demo` | Build the static files used by the GitHub Pages demo. |
| `make update-gh-pages` | Build the demo, update the `gh-pages` worktree, and commit changes there. |
| `make push-gh-pages` | Run `update-gh-pages` and push the `gh-pages` branch. |
| `make test` / `make test-all` | Run root tests plus standalone module tests for query file demos. |
| `make test-unit` | Run short unit tests. |
| `make test-query-files-wasm` | Run tests inside `cmd/query_files_wasm`. |
| `make coverage` | Run tests and open an HTML coverage report. |
| `make bench` | Run Go benchmarks with allocation output. |
| `make fmt` / `make fmt-check` | Format Go files or check formatting without modifying files. |
| `make vet` | Run `go vet ./...`. |
| `make lint` | Run `golangci-lint`; requires it to be installed locally. |
| `make verify` | Run mutating local verification: format, vet, lint, and tests. |
| `make verify-ci` | Run non-mutating CI-style verification: format check, vet, build check, and tests. |
| `make clean` | Remove generated binaries, WASM artifacts, coverage files, and WAL leftovers. |
| `make run-repl` / `make run-server` / `make run-demo` | Build and start the corresponding demo tool. |
| `make info` | Print build version, Go version, and configured paths. |

The Makefile uses overridable variables. Examples:

```bash
make build BINARY_DIR=dist
make test GO_TEST_FLAGS="-run TestGeo -count=1"
make update-gh-pages GH_PAGES_COMMIT_MESSAGE="Update playground"
make update-gh-pages GH_PAGES_WORKTREE=/tmp/tinysql-gh-pages
```

Notes:

- `make verify-ci` is the safest pre-push check because it does not rewrite Go
  files.
- `make verify` runs `make fmt`, so it may modify tracked Go files.
- `make update-gh-pages` creates or refreshes a local worktree for the
  `gh-pages` branch and commits only when the generated static demo changed.
- `make push-gh-pages` pushes only `gh-pages`; push `main` separately after
  committing source changes.

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

## Performance

TinySQL wins most read-heavy and low-latency-write workloads against
`modernc.org/sqlite` at the row counts tested (full scans, joins, aggregates,
single inserts) by operating on native Go values with no `database/sql`
`Scan()` marshaling. Vector search (`VEC_SEARCH`) supports `flat` (exact),
`ivf`, and `hnsw` index modes, with `VEC_WARM` for prebuilding an index ahead
of the first query. See [BENCHMARKS.md](./BENCHMARKS.md) for detailed
tinySQL-vs-SQLite numbers and a log of internal engine optimizations
(allocation and SIMD-kernel fixes to vector search and row scanning) with
before/after measurements.

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
