# Repository Structure

Three layers: the public Go API at the repository root, engine and storage
internals under `internal/`, and tools/demos under `cmd/`. The tree below
highlights the relevant paths, not every file.

For what the layers do, how a statement flows through them, and which invariants
to preserve when changing internals, see [architecture.md](./architecture.md).

## Top level

```text
/
|-- doc.go                   Package documentation
|-- tinysql.go               Core DB/table types, column constants, constructors
|-- query.go                 Parsing, compilation, row/stream/columnar execution
|-- storage.go               Storage configuration, MVCC, WAL, snapshots, encryption
|-- import.go                Structured-file import and JSON table export
|-- retrieval.go             Vector cache configuration and typed RAG filters
|-- catalog.go               Catalog and authorization facade
|-- jobs.go / subscribe.go   Scheduler and change subscriptions
|-- extensions.go            Extensions, table functions, stored procedures
|-- mbtiles_compat.go        Deprecated MBTiles compatibility facade
|-- builder*.go              SELECT, expressions, mutations, SQL rendering
|-- agent_context.go         Compact database metadata snapshot builder
|-- driver/                  Public database/sql driver wrapper
|-- exporter/                Public ResultSet export helpers
|-- internal/                Engine, storage, importer, driver internals
|-- cmd/                     Executables, demos, web apps, WASM builds
|-- examples/                TinyGo smoke test and RP2350 bare-metal example
|-- benchmarks/              Benchmark tests
|-- bindings/c/              C ABI shared by all native bindings (tinysql.h)
|-- bindings/python/         Python DB-API 2.0 package on the C ABI
|-- bindings/rust/           Rust crate linking the C ABI statically
|-- bindings/swift/          Swift package for macOS, iOS and iPadOS
|-- bindings/apple/          XCFramework build script for the Swift package
|-- odbc/                    ODBC bridge and its tests
|-- data/                    Sample datasets for demos and tests
|-- docs/                    Guides and integration notes
|-- deploy/observability/    Alerting and SLO manifests
|-- tests/examples.yml       Example-driven test data
|-- demos/sql/showcase.sql   Runnable SQL feature tour
|-- scripts/                 Build checks, cleanup, output-format demo
|-- docs/reference/          SQL source and generated HTML function reference
|-- bin/                     Ignored binary, coverage and profile output
|-- .github/                 Actions workflows and repo automation
|-- Makefile                 Build, test, and helper targets
`-- README.md                Overview, quick start, tool index
```

## `internal/`

Implementation detail for the module. External projects must not import these.

```text
internal/
|-- engine/          SQL parser, lexer, compiler, executor, built-ins, FTS,
|                    triggers, vector search, virtual tables
|-- storage/         Database state, catalog, MVCC, WAL, concurrency, buffer
|   |                pools, scheduler
|   `-- pager/       Page format, B-tree pages, freelist, GC, recovery, WAL
|                    primitives
|-- importer/        CSV, JSON, XML, GeoJSON, KML, Shapefile, encoding helpers
|-- exporter/        Export helpers
|-- driver/          In-repo database/sql driver used by bundled commands
`-- testhelper/      Shared test utilities
```

The top-level `driver/` package is the public `database/sql` entry point:
import `github.com/SimonWaldherr/tinySQL/driver`, not `internal/driver`.

## `cmd/`

Every direct subdirectory is a separate binary, demo, or tool. Most have a
README next to the code.

```text
cmd/
|-- tinysql/             SQLite-compatible CLI
|-- tinysqld/            Lightweight admin/health daemon
|-- repl/                Interactive SQL REPL
|-- server/              HTTP JSON API and gRPC server
|   `-- loadtest/        HTTP load generator for the server
|-- sqltools/            Formatter, validator, explain, REPL helpers
|-- query_files/         Query CSV, JSON, XML files (plus sample inputs)
|-- query_files_wasm/    Browser-oriented WASM build of query_files
|-- wasm_browser/web/    tinySQL as WASM for browsers, plus UI assets
|-- wasm_node/           tinySQL as WASM for Node.js
|-- studio/frontend/     Desktop GUI (Wails) and its frontend
|-- accessweb/           Web UI for querying data (templates/, static/)
|-- formigo/             Form and data collection web app (templates/, static/)
|-- tinysqlpage/         SQL-driven web page server
|-- migrate/             Import/export and database transfer pipeline
|-- fsql/                Filesystem-as-SQL tool
|-- demo/                Lightweight demo program
|-- catalog_demo/        Catalog and scheduler console demo plus web dashboard
|-- docsearch/           Local document search and persistent full-text index
|-- geofence-service/    GeoJSON geofencing service with browser frontend
|-- import-validate/     CSV validation and explicit import workflow
|-- offline_demo/        Read-only POI snapshot CLI and browser explorer
|-- ragdemo/             Retrieval-augmented generation demo
|-- supportdesk/         Support knowledge base and ticketing example
|-- tinyorm_demo/        tinyORM CLI demo and persistent places directory
|-- worklog/             Local work-time tracker with reports and CSV export
|-- debug/               SQL diagnostic helper
`-- tinysql-mcp-server/  MCP server for tinySQL
```

## Examples

`example_test.go`, `example_exists_test.go`,
`example_view_dependencies_test.go`, `import_example_test.go`, and
[`demos/sql/showcase.sql`](../demos/sql/showcase.sql) demonstrate the public API and SQL features.
`exporter/example_test.go` covers the public export facade.

## Public API layout

All root Go files belong to the same `tinysql` package. This grouping changes
source navigation, not import paths or exported signatures. Builder declarations
live in `builder.go` (SELECT/CTE), `builder_expr.go` (expressions),
`builder_mutation.go` (schema and DML), and `builder_sql.go` (SQL rendering).
Public-package tests remain beside the API they exercise.

`examples/` contains target-specific embedding examples; `demos/README.md` is the
index for runnable feature demonstrations, including programs under `cmd/`.
Reference examples live in `docs/reference/functions.sql`; `make reference`
regenerates its HTML view and the browser playground's JSON overview. Generation
never executes FILE/HTTP examples or captures the current clock.

The former root paths remain compatible: `FUNCTIONS.sql` and
`example_showcase.sql` are copies refreshed by `make reference`,
`FUNCTIONS.sql.html` redirects to the canonical HTML page, and
`demo_formats.sh` forwards to the script under `scripts/`. Edit the canonical
sources; freshness tests protect the compatibility copies from drifting.

Builds and coverage use `bin/`. Put profiling output there too, for example
`go test -o bin/engine.test -cpuprofile=bin/engine.cpu.prof ./internal/engine`.
`make clean` removes generated build output and recognized ignored root leftovers,
including the obsolete `.gocache/`. It preserves database files, WALs and the local
`.gh-pages-worktree/`; worktrees have their own Git lifecycle.
The default coverage path is `bin/coverage.out`; older automation that expects
a root profile can still use `make test-all COVERPROFILE=coverage.out`.
