# Repository Structure

Three layers: the public Go API at the repository root, engine and storage
internals under `internal/`, and tools/demos under `cmd/`. The tree below
highlights the relevant paths, not every file.

## Top level

```text
/
|-- tinysql.go               Public package entry points and helpers
|-- builder.go               Query/build helpers for the public API
|-- agent_context.go         Compact database metadata snapshot builder
|-- driver/                  Public database/sql driver wrapper
|-- exporter/                Public ResultSet export helpers
|-- internal/                Engine, storage, importer, driver internals
|-- cmd/                     Executables, demos, web apps, WASM builds
|-- examples/                TinyGo smoke test and RP2350 bare-metal example
|-- benchmarks/              Benchmark tests
|-- bindings/python/         Python bindings and packaging example
|-- odbc/                    ODBC bridge and its tests
|-- data/                    Sample datasets for demos and tests
|-- docs/                    Guides and integration notes
|-- deploy/observability/    Alerting and SLO manifests
|-- tests/examples.yml       Example-driven test data
|-- example_*.sql            Showcase SQL used in docs and demos
|-- FUNCTIONS.sql[.html]     Generated function reference
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
|-- offline_demo/        Offline-oriented demo
|-- ragdemo/             Retrieval-augmented generation demo
|-- tinyorm_demo/        ORM helper demo
|-- debug/               SQL diagnostic helper
|-- catalog_demo/        Catalog and job scheduler demo
`-- tinysql-mcp-server/  MCP server for tinySQL
```

## Examples

`example_test.go`, `example_exists_test.go`,
`example_view_dependencies_test.go`, `import_example_test.go`, and
`example_showcase.sql` demonstrate the public API and SQL features.
`exporter/example_test.go` covers the public export facade.
