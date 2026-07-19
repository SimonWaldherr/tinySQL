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

Requirements: Go 1.26.5+ (the minimum patched release for the Go 1.26
standard-library security fixes).

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
  referential actions, plus `NOT NULL` and literal `DEFAULT` values.
- SQLite-style type declarations and affinities, including `INTEGER`, `REAL`,
  `TEXT`, `NUMERIC`, `VARCHAR(n)`, `CLOB`, typeless columns, and `ANY`.
- Built-in functions for JSON, YAML, URLs, hashes, bitmaps, regex, text, math,
  dates, full-text search, vector search, RAG scoring, and provenance-aware
  context expansion.
- Geodata imports and SQL helpers for GeoJSON, KML, OSM XML, Shapefiles,
  MBTiles, routing graphs, points, distance, radius, and bounding-box queries.
- Operational hooks for health checks, lifecycle management, read-only mode,
  RBAC, audit logging, and encryption at rest for `ModeDisk`, `ModeJSON`,
  `ModeHybrid`, and `ModeIndex` table files.
- `ANALYZE` persists exact table and column statistics; `sys.statistics`
  exposes them and the planner uses fresh distinct-count estimates to prefer
  more selective equality indexes.
- Direct multi-row `INSERT`, `UPDATE`, and `DELETE` are statement-atomic,
  including trigger side effects; materialized secondary indexes are updated
  incrementally instead of rebuilt after each mutation.
- Browser WASM demos run the engine directly in the browser. Their loaders use
  a pre-compressed `.wasm.gz` artifact when the browser supports streaming
  decompression and fall back to the regular `.wasm` file.

For a broader feature reference, see [FUNCTIONS.sql](./FUNCTIONS.sql),
[example_showcase.sql](./example_showcase.sql), and the Go tests.

## Fluent query builder

For programmatic query construction, use the root package's fluent builder.
`Build` returns a statement that can be passed directly to `Execute`, while
`ToSQL` is useful for logging or inspecting the generated SQL.

```go
activeTags := tsql.SelectStar().
    From("tags").
    Where(tsql.Eq(tsql.Col("active"), tsql.Val(true)))

query := tsql.Select(tsql.Col("name")).
    From("items").
    Where(tsql.Exists(activeTags)).
    OrderBy("name").
    Build()

rs, err := tsql.Execute(ctx, db, "default", query)
if err != nil {
    panic(err)
}
```

The builder supports projections, joins, CTEs, ordering, limits, expressions,
and `Exists`/`NotExists` subquery predicates. See the executable
[`ExampleExists`](./example_exists_test.go) and
[`Example_viewsAndMaterializedViews`](./view_examples_test.go) for complete
examples.

## Optional import profiles

The core has no SQLite or Shapefile runtime dependency. Enable specialized
importers only in builds that need them:

```bash
# SQLite files and MBTiles (pure-Go modernc SQLite)
go build -tags=sqliteimport ./...

# ESRI Shapefile and Shapefile ZIP imports
go build -tags=shapefile ./...

# Both optional importers
go build -tags=sqliteimport,shapefile ./...
```

Without the respective tag, the import API remains available but returns a
clear feature-disabled error. SQLite remains the recommended production
backend for standard MBTiles serving.

## Statically linked Go extensions

Applications can add an extension by importing its Go package and activating
it for one database instance. This is intentionally static rather than a Go
shared-object plugin, so it remains portable across Go, TinyGo, and WebAssembly
builds.

```go
type ExampleExtension struct{}

func (ExampleExtension) ExtensionInfo() tinysql.ExtensionInfo {
    return tinysql.ExtensionInfo{
        Name: "example",
        Version: "1.0.0",
        Capabilities: []tinysql.ExtensionCapability{
            tinysql.CapabilityFilesystem,
        },
    }
}

func (ExampleExtension) Register(db *tinysql.DB) error {
    // Register supported extension points for this DB.
    return nil
}

db := tinysql.NewDB()
if err := db.Use(ExampleExtension{}); err != nil {
    panic(err)
}
```

`SELECT * FROM sys.extensions` lists the extensions active for the current DB;
`sys.objects` includes them as `EXTENSION` objects. Extensions are deliberately
not persisted or unloadable: their code must be linked into each process and
explicitly activated after opening a database. Capability declarations are
visible metadata today; enforcement is a future server-policy feature.

## Portable import and export

CSV/TSV imports normalize text to UTF-8. UTF-8, UTF-16 LE/BE, ISO-8859-1,
ISO-8859-2, ISO-8859-15, and Windows-1252 can be selected explicitly. Invalid
UTF-8 is rejected instead of silently replaced. BLOB values stay binary:
CSV/XML use self-identifying Base64 or hex, JSON uses a BLOB envelope, and SQL
exports use SQLite-compatible `X'...'` literals.

`exporter.ExportTableManifest` writes a versioned JSON schema manifest with
declared types, affinity, constraints, row count, and an ordered typed-row
SHA-256 fingerprint. It can be paired with CSV, JSON, or SQL data exports for
verifiable transfers.

The public `github.com/SimonWaldherr/tinySQL/exporter` package exports a
`ResultSet` as CSV, TSV, JSON, NDJSON, XML, GOB, or SQL. JSON preserves BLOB
values with a tagged Base64 envelope by default; set
`Options.JSONBinaryMode` to `"legacy-string"` only when compatibility with
plain Base64 JSON strings is required.

```go
import (
    "bytes"

    "github.com/SimonWaldherr/tinySQL/exporter"
)

var out bytes.Buffer
if err := exporter.ExportJSON(&out, rs, exporter.Options{PrettyJSON: true}); err != nil {
    panic(err)
}
// out contains a JSON array of result-row objects.
```

Use `ExportNDJSON` for streaming one JSON object per row, and
`ExportTableManifest` when a schema and data fingerprint should accompany an
export. The runnable [`ExampleExportJSON`](./exporter/example_test.go) shows
the minimal JSON path.

## Vector search cache and analytics

VEC_SEARCH already maintains bounded internal column and ANN-index caches.
The optional result cache stores only deterministic top-K row IDs, never RAG
answers or raw query vectors. Its key includes tenant, table version, column,
metric, index mode, `k`, and a vector hash, so table mutations invalidate
results naturally.

The default is deliberately lean: no result cache and no analytics. Enable a
small bounded cache only for repeated queries:

```go
cfg := tsql.DefaultVectorCacheConfig()
cfg.ResultCacheEntries = 128 // 0 keeps it disabled
cfg.Analytics = true
tsql.ConfigureVectorCache(cfg)

stats := tsql.VectorCacheAnalytics()
```

Enabled caches default to a 30-second TTL. Analytics defaults to a 60-second,
128-event in-memory window and records query shape and timing, not vector
contents. The configuration is process-wide because vector indexes and caches
are process-wide.

For `tinysqld`:

```bash
tinysqld -analytics -vector-cache-entries 128 -vector-cache-ttl 30s
```

The authenticated `GET /api/analytics/vector` endpoint is available only with
`-analytics`; otherwise it returns `404`.

## Guides

Pick the guide that matches what you're building:

| Guide | Scenario |
|---|---|
| [RAG / AI usage](./docs/rag-guide.md) | Retrieval-augmented generation: vector search, hybrid retrieval, reranking, context expansion, LLM agent integration |
| [Developer integration](./docs/developer-integration.md) | Embedding TinySQL in Go, using the `database/sql` driver, running it as WASM in the browser or a custom frontend |
| [TinyGo / embedded](./docs/tinygo-guide.md) | Running tinySQL in TinyGo WebAssembly, RP2350-class boards, and other memory-rich embedded targets |
| [Storage & persistence](./docs/storage-guide.md) | Storage modes, DSNs, connection strings, read-only serving |
| [Command line tools](./docs/cli-guide.md) | Using the `cmd/tinysql` CLI, REPL, servers, file-query tools, and the browser playground |
| [Development guide](./docs/development-guide.md) | Running tests, the Makefile, building the WASM demo |
| [Memory optimization](./docs/memory-optimization.md) | Where tinySQL spends memory, landed wins, and proposals for further reductions |
| [Repository structure](./docs/repository-structure.md) | Codebase layout for contributors |
| [Benchmarks](./BENCHMARKS.md) | TinySQL-vs-SQLite numbers and internal optimization history |

`RAG_HYBRID_SCORE`/`RAG_RANK_SCORE` assume cosine `[-1, 1]` similarity input —
see the [reranking caveat](./docs/rag-guide.md#3-rerank-blend-similarity-with-freshness-and-quality)
before pairing them with a non-cosine `VEC_SEARCH` metric.

## Limitations

TinySQL is not a PostgreSQL/MySQL replacement. Important current limits:

- Single-process database engine; no built-in replication, clustering,
  failover, sharding, or distributed transactions.
- Direct multi-row `INSERT`, `UPDATE`, and `DELETE` statements are atomic,
  including their trigger side effects. Cross-statement transactions and
  `SAVEPOINT` are not implemented.
- No composite primary keys or composite foreign keys.
- No CHECK constraints, UPSERT/ON CONFLICT, SAVEPOINT, ATTACH/DETACH, VACUUM,
  partial indexes, generated columns, or persistent ANN vector index files.
- Materialized secondary indexes currently support equality point/prefix seeks
  on their leading columns. They are maintained incrementally for
  `INSERT`/`UPDATE` and remapped on `DELETE`, then persisted with
  snapshots/backends; pager-native incremental index pages and range planning
  are not implemented yet. `ModeIndex`/`ModeHybrid` now keep backend-loaded
  tables out of the permanent DB catalog and enforce their buffer-pool budget,
  but the legacy GOB table codec still decodes a full table on a cache miss.
  They are therefore not yet suitable as a strict per-record, multi-gigabyte
  MBTiles serving engine; SQLite remains the production MBTiles default.
- RBAC checks are coarse and single-table oriented.
- Encryption at rest currently covers table files for `ModeDisk`, `ModeJSON`,
  `ModeHybrid`, and `ModeIndex`, not WAL-backed modes or metadata files.
- The optional VEC_SEARCH result cache is process-local and in-memory; it is
  not a distributed cache and does not persist across process restarts.

Evaluate these limits before using TinySQL for production-critical data.

## Project Goals

TinySQL is primarily an educational and embeddable SQL engine. It demonstrates
SQL parsing, AST construction, execution, storage backends, Go's `database/sql`
driver interface, full-text search, triggers, recursive CTEs, window functions,
vector search, RAG helpers, and multi-tenant database patterns.
