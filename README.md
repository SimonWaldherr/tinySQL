# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

TinySQL is a lightweight SQL database engine in pure Go, for learning database
internals, embedding in Go programs, demos, tests, and single-process workloads
that need a small SQL layer without external services.

Demos: [video](https://youtu.be/W28-aBk3BL0),
[browser playground](https://simonwaldherr.github.io/tinySQL/).

## Install

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Requirements: Go 1.26.5+ (the minimum patched release for the Go 1.26
standard-library security fixes).

## Quick start

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
- Row triggers for `BEFORE`/`AFTER` INSERT, UPDATE, and DELETE, including
  `DELETE FROM table` without a WHERE clause. Trigger side effects roll back
  with the surrounding statement; recursive chains stop after 32 nested
  executions.
- Constraints: single-column PRIMARY KEY, UNIQUE, and FOREIGN KEY with
  referential actions, plus `NOT NULL` and literal `DEFAULT` values.
- SQLite-style type declarations and affinities, including `INTEGER`, `REAL`,
  `TEXT`, `NUMERIC`, `VARCHAR(n)`, `CLOB`, typeless columns, and `ANY`.
- Built-in functions for JSON, YAML, URLs, hashes, bitmaps, regex, text, math,
  dates, full-text search, vector search, RAG scoring, and provenance-aware
  context expansion.
- Geodata imports and SQL helpers for GeoJSON, KML, OSM XML, Shapefiles,
  MBTiles, routing graphs, points, distance, radius, and bounding-box queries.
- Map tiles: Web Mercator addressing functions (`TILE_X`, `TILE_Y`, `TILE_ZXY`,
  `TILE_BBOX`, `TILE_QUADKEY`, and `TILE_FLIP_Y` for the XYZ/TMS row
  convention), MBTiles import/export, in-place `.mbtiles` querying, and an
  optional XYZ tile endpoint in `tinysqld`. See
  [Serving map tiles](#serving-map-tiles).
- Operational hooks for health checks, lifecycle management, read-only mode,
  RBAC, audit logging, and encryption at rest for `ModeDisk`, `ModeJSON`,
  `ModeHybrid`, and `ModeIndex` table files.
- `ANALYZE` persists exact table and column statistics; `sys.statistics` exposes
  them, and the planner uses fresh distinct-count estimates to prefer more
  selective equality indexes.
- Direct multi-row `INSERT`, `UPDATE`, and `DELETE` are statement-atomic,
  including trigger side effects; materialized secondary indexes are updated
  incrementally instead of rebuilt after each mutation.
- The `database/sql` driver supports snapshot-based cross-statement transactions
  via `BEGIN`, `COMMIT`, `ROLLBACK`, or Go's `BeginTx`.
  `BEGIN [TRANSACTION] READ ONLY` rejects writes while retaining a stable read
  snapshot; concurrent writes to the same changed table are reported as a
  retryable transaction conflict.
- Specialized raw execution on hot paths where it is safe: direct
  `ORDER BY FLOAT ... LIMIT` pagination, simple aggregates, JOIN/WHERE filter
  pushdown, and triggerless INSERTs. [BENCHMARKS.md](./BENCHMARKS.md) documents
  scope, fallback rules, and reproducible measurements; `make bench-hotpaths`
  runs the focused local suite.
- Browser WASM demos run the engine in the browser. Their loaders use a
  pre-compressed `.wasm.gz` artifact when the browser supports streaming
  decompression and fall back to the regular `.wasm` file.

Broader feature reference: [FUNCTIONS.sql](./FUNCTIONS.sql),
[example_showcase.sql](./example_showcase.sql), and the Go tests.

## Fluent query builder

`Build` returns a statement for `Execute`; `ToSQL` renders the generated SQL for
logging or inspection.

```go
activeTags := tsql.SelectStar().
    From("tags").
    Where(tsql.Eq(tsql.Col("active"), tsql.Val(true)))

query := tsql.Select(tsql.Col("name")).
    From("items").
    Where(tsql.Exists(activeTags)).
    OrderBy("name").
    Build()

rs, err := tsql.Execute(ctx, db, "default", query) // handle err
```

Projections, joins, CTEs, ordering, limits, expressions, and
`Exists`/`NotExists` subquery predicates are supported. See
[`ExampleExists`](./example_exists_test.go) and
[`Example_viewsAndMaterializedViews`](./view_examples_test.go).

## Transactions and triggers

Use the Go driver for transactions spanning several statements. A transaction
sees its own writes. `SAVEPOINT` is not implemented.

```go
import (
    "context"
    "errors"

    tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

db, err := tsqldriver.OpenInMemory("default")
if err != nil {
    panic(err)
}
defer db.Close()

tx, err := db.BeginTx(context.Background(), nil)
if err != nil {
    panic(err)
}
if _, err = tx.Exec(`INSERT INTO users VALUES (2, 'Bob')`); err != nil {
    _ = tx.Rollback()
    panic(err)
}
if err = tx.Commit(); errors.Is(err, tsqldriver.ErrTransactionConflict) {
    panic("retry transaction") // conflict is retryable
} else if err != nil {
    panic(err)
}
```

For SQL-issued read snapshots use `BEGIN READ ONLY` (also accepted as
`BEGIN TRANSACTION READ ONLY`) followed by `COMMIT` or `ROLLBACK`. Row triggers
run for each affected row, including a WHERE-less DELETE; all writes performed
by their bodies are rolled back if the outer statement fails.

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
clear feature-disabled error. The tags are only needed to *read and write*
`.mbtiles` files; serving a tileset already loaded into tinySQL needs neither.
For a tileset larger than memory, SQLite remains the better backend — see
[Serving map tiles](#serving-map-tiles).

## Statically linked Go extensions

An application adds an extension by importing its Go package and activating it
for one database instance. Static linking rather than Go shared-object plugins
keeps this portable across Go, TinyGo, and WebAssembly builds.

```go
type ExampleExtension struct{}

func (ExampleExtension) ExtensionInfo() tinysql.ExtensionInfo {
    return tinysql.ExtensionInfo{
        Name:    "example",
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

`github.com/SimonWaldherr/tinySQL/exporter` exports a `ResultSet` as CSV, TSV,
JSON, NDJSON, XML, GOB, or SQL. JSON preserves BLOB values with a tagged Base64
envelope by default; set `Options.JSONBinaryMode` to `"legacy-string"` only when
compatibility with plain Base64 JSON strings is required. `ExportJSON(w, rs,
exporter.Options{PrettyJSON: true})` writes a JSON array of result-row objects;
`ExportNDJSON` streams one JSON object per row.

`exporter.ExportTableManifest` writes a versioned JSON schema manifest with
declared types, affinity, constraints, row count, and an ordered typed-row
SHA-256 fingerprint, to accompany a CSV, JSON, or SQL data export for verifiable
transfers. Runnable example:
[`ExampleExportJSON`](./exporter/example_test.go).

## Serving map tiles

MBTiles is a SQLite database with a prescribed schema, so tinySQL treats a
tileset as an ordinary table plus the addressing functions and transport that
make it usable as a map.

**The row convention.** Web clients and `/{z}/{x}/{y}.png` URLs count tile rows
from the top (XYZ); the MBTiles specification stores them counted from the
bottom (TMS). They differ by `2^zoom - 1 - y`, and getting it wrong yields a
vertically mirrored map that looks almost right. `TILE_FLIP_Y` is the one
explicit conversion; every other tile function speaks XYZ:

```sql
-- The tile covering Berlin at zoom 14, ready for an MBTiles lookup.
SELECT TILE_ZXY(13.405, 52.520, 14);
-- {"tile_row":11010,"x":8802,"y":5373,"z":14}

SELECT tile_data FROM tiles
WHERE zoom_level = 14
  AND tile_column = TILE_X(13.405, 14)
  AND tile_row    = TILE_FLIP_Y(TILE_Y(52.520, 14), 14);
```

Also available: `TILE_LON`/`TILE_LAT` (tile edges), `TILE_BBOX` (bounds as
`[west, south, east, north]`), `TILE_QUADKEY`/`TILE_FROM_QUADKEY`,
`TILE_PARENT`, `TILE_CONTAINS`, and `TILE_COUNT`.

**Index the tileset.** A declared `PRIMARY KEY` does not create an index, and a
tile lookup without one is a full scan:

```sql
CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
```

With it, a tile lookup is an index seek — measured at parity with SQLite's
`:memory:` and roughly 4-5x faster than a SQLite file
([BENCHMARKS.md](./BENCHMARKS.md)).

**Import, export, or query in place.** These need the `sqliteimport` build tag,
which pulls in the SQLite driver used to read and write `.mbtiles` files:

```go
// Import a tileset. Tiles stream in bounded batches, so the file need not fit in RAM.
importer.ImportMBTiles(ctx, db, "default", "tiles", "berlin.mbtiles",
    &importer.ImportOptions{CreateTable: true, BatchSize: 1000})

// Write a spec-compliant .mbtiles that any MBTiles tool can read.
importer.ExportMBTiles(ctx, db, "default", "out.mbtiles",
    &importer.ExportMBTilesOptions{TileRowIsTMS: true})

// Query a large tileset without copying it: overview zooms only, or index only.
importer.OpenMBTiles(ctx, db, "default", "planet.mbtiles",
    &importer.OpenMBTilesOptions{Zooms: []int{0, 1, 2, 3, 4}})
importer.OpenMBTiles(ctx, db, "default", "planet.mbtiles",
    &importer.OpenMBTilesOptions{WithoutTileData: true}) // addressing + size + hash
```

**Serve tiles over HTTP** with `tinysqld -tiles`:

```text
GET /tiles/{tileset}/{z}/{x}/{y}.{ext}   one tile (XYZ; flipped to TMS internally)
GET /tiles/{tileset}.json                TileJSON 3.0.0
GET /tiles/{tileset}/metadata            raw MBTiles metadata rows
```

Content-Type and Content-Encoding follow the tileset's `format` metadata, so
`pbf` tiles are served as gzipped protobuf, which is how MBTiles stores them.
The endpoint is **unauthenticated by design** — a browser cannot attach a bearer
token to a map-tile request — so it is off unless `-tiles` is passed. Put a
proxy in front for referer/IP restrictions or signed URLs.

A tileset larger than memory is still better served by SQLite, or queried in
place with `OpenMBTiles`; see the
[Storage & Persistence Guide](./docs/storage-guide.md).

## SQL formatting

`BeautifySQL` formats SQL for logs, review, or an editor; `MinifySQL` compacts it
for transport or storage. Both are dependency-free and preserve string literals,
quoted identifiers, and comments. `MinifySQL` keeps the newline after a `--`
comment, since removing it would comment out the rest of the statement.

```go
query := "select id,name from users where status = 'active' and id=42"

pretty := tinysql.BeautifySQL(query)
// SELECT id, name
// FROM users
// WHERE status = 'active'
// AND id = 42

compact := tinysql.MinifySQL(pretty)
// SELECT id,name FROM users WHERE status='active' AND id=42
```

[`ExampleBeautifySQL`](./sql_format_example_test.go) documents the full round
trip. The command-line equivalents are `sqltools beautify` and
`sqltools normalize` (canonical comparison output).

## Vector search cache and analytics

VEC_SEARCH always maintains bounded internal column and ANN-index caches. The
optional result cache stores only deterministic top-K row IDs, never RAG answers
or raw query vectors. Its key includes tenant, table version, column, metric,
index mode, `k`, and a vector hash, so table mutations invalidate results
naturally. Defaults: no result cache, no analytics. Enable a small bounded cache
only for repeated queries:

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

The authenticated `GET /api/analytics/vector` endpoint exists only with
`-analytics`; otherwise it returns `404`.

## Guides

| Guide | Scenario |
|---|---|
| [RAG / AI usage](./docs/rag-guide.md) | Retrieval-augmented generation: vector search, hybrid retrieval, reranking, context expansion, LLM agent integration |
| [Developer integration](./docs/developer-integration.md) | Embedding TinySQL in Go, using the `database/sql` driver, running it as WASM in the browser or a custom frontend |
| [TinyGo / embedded](./docs/tinygo-guide.md) | Running tinySQL in TinyGo WebAssembly, RP2350-class boards, and other memory-rich embedded targets |
| [Storage & persistence](./docs/storage-guide.md) | Storage modes, DSNs, connection strings, read-only serving |
| [Command line tools](./docs/cli-guide.md) | Using the `cmd/tinysql` CLI, REPL, servers, file-query tools, and the browser playground |
| [Development guide](./docs/development-guide.md) | Running tests, the Makefile, building the WASM demo |
| [Memory optimization](./docs/memory-optimization.md) | Where tinySQL spends memory, landed wins, and proposals for further reductions |
| [Architecture](./docs/architecture.md) | How the layers fit together, the life of a statement, and the invariants to preserve when changing internals |
| [Architecture in diagrams](./docs/architecture-diagrams.md) | The same material drawn: the stack, the core types, a write and a transaction end to end, query planning, the write-ahead log and recovery, and choosing a storage mode |
| [Repository structure](./docs/repository-structure.md) | Codebase layout for contributors |
| [Benchmarks](./BENCHMARKS.md) | TinySQL-vs-SQLite numbers and internal optimization history |

`RAG_HYBRID_SCORE`/`RAG_RANK_SCORE` assume cosine `[-1, 1]` similarity input —
see the [reranking caveat](./docs/rag-guide.md#3-rerank-blend-similarity-with-freshness-and-quality)
before pairing them with a non-cosine `VEC_SEARCH` metric.

## Limitations

TinySQL is not a PostgreSQL/MySQL replacement. Current limits:

- Single-process database engine; no built-in replication, clustering,
  failover, sharding, or distributed transactions.
- Direct multi-row `INSERT`, `UPDATE`, and `DELETE` statements are atomic,
  including their trigger side effects. Cross-statement transactions are
  available through the `database/sql` driver; nested transactions and
  `SAVEPOINT` are not implemented.
- No composite primary keys or composite foreign keys.
- No CHECK constraints, UPSERT/ON CONFLICT, SAVEPOINT, ATTACH/DETACH, VACUUM,
  partial indexes, generated columns, or persistent ANN vector index files.
- Materialized secondary indexes support only equality point/prefix seeks on
  their leading columns. They are maintained incrementally for `INSERT`/`UPDATE`,
  remapped on `DELETE`, and persisted with snapshots/backends; pager-native
  incremental index pages and range planning are not implemented yet.
  `ModeIndex`/`ModeHybrid` keep backend-loaded tables out of the permanent DB
  catalog and enforce their buffer-pool budget, but the legacy GOB table codec
  still decodes a full table on a cache miss, so they are not yet suitable as a
  strict per-record, multi-gigabyte MBTiles serving engine; SQLite remains the
  production MBTiles default.
- RBAC checks are coarse and single-table oriented.
- Encryption at rest currently covers table files for `ModeDisk`, `ModeJSON`,
  `ModeHybrid`, and `ModeIndex`, not WAL-backed modes or metadata files.
- The optional VEC_SEARCH result cache is process-local and in-memory; it is
  not a distributed cache and does not persist across process restarts.

Evaluate these limits before using TinySQL for production-critical data.

## Project goals

TinySQL is primarily an educational and embeddable SQL engine. It demonstrates
SQL parsing, AST construction, execution, storage backends, Go's `database/sql`
driver interface, full-text search, triggers, recursive CTEs, window functions,
vector search, RAG helpers, and multi-tenant database patterns.
