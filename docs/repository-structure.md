# Repository Structure

This repository is organized around three layers:

1. The public Go API at the repository root.
2. Internal engine and storage packages under `internal/`.
3. User-facing tools, demos, and web apps under `cmd/`.

The tree below highlights the most relevant paths rather than every single file.

## Top-level layout

```text
/
|-- .github/                 GitHub Actions workflows and repo automation
|-- benchmarks/              Benchmark tests
|-- bindings/                Language bindings
|   `-- python/              Python bindings and packaging example
|-- README.md                 Project overview, quick start, and tool index
|-- tinysql.go                Public package entry points and helpers
|-- builder.go                Query/build helpers for the public API
|-- agent_context.go          Compact database metadata snapshot builder
|-- driver/                   Public `database/sql` driver wrapper
|-- exporter/                 Public ResultSet export helpers
|-- internal/                 Engine, storage, importer, and driver internals
|-- cmd/                      Executables, demos, web apps, and WASM builds
|-- odbc/                     ODBC bridge and its tests
|-- data/                     Sample datasets used by demos and tests
|-- docs/                     Written guides and integration notes
|-- deploy/observability/     Alerting and SLO manifests
|-- tests/                    Cross-cutting example fixtures
|-- example_*.sql             Showcase SQL used in docs and demos
|-- FUNCTIONS.sql*            Generated function reference and HTML output
`-- Makefile                  Common build, test, and helper targets
```

## `internal/`

Everything under `internal/` is implementation detail for the module itself.
External projects should not import these packages directly.

- `internal/engine/` contains the SQL parser, lexer, compiler, executor, built-ins, FTS, triggers, vector search, and virtual table support.
- `internal/storage/` contains database state, catalog logic, MVCC, WAL, concurrency, buffer pools, and scheduler code.
- `internal/storage/pager/` contains the low-level page format, B-tree pages, freelist, garbage collection, recovery, and WAL primitives.
- `internal/importer/` contains CSV, JSON, XML, GeoJSON, KML, Shapefile, and encoding-related import helpers.
- `internal/exporter/` contains export helpers.
- `internal/driver/` contains the in-repo `database/sql` driver used by bundled commands.
- `internal/testhelper/` contains shared test utilities.

## `driver/`

The top-level `driver/` package is the public `database/sql` entry point for external projects.
Use `github.com/SimonWaldherr/tinySQL/driver`, not `internal/driver`.

## `cmd/`

Every direct subdirectory of `cmd/` is a separate binary, demo, or tool.
Most of them have a dedicated README next to the code.

- `cmd/tinysql/` - SQLite-compatible CLI
- `cmd/repl/` - interactive SQL REPL
- `cmd/server/` - HTTP JSON API and gRPC server
- `cmd/sqltools/` - formatter, validator, explain, and REPL helpers
- `cmd/query_files/` - query CSV, JSON, and XML files
- `cmd/query_files_wasm/` - browser-oriented WASM build of `query_files`
- `cmd/wasm_browser/` - TinySQL compiled to WASM for browsers
- `cmd/wasm_node/` - TinySQL compiled to WASM for Node.js
- `cmd/studio/` - desktop GUI built with Wails
- `cmd/accessweb/` - web UI for accessing and querying data
- `cmd/formigo/` - form and data collection web application
- `cmd/tinysqlpage/` - SQL-driven web page server
- `cmd/migrate/` - import/export and database transfer pipeline
- `cmd/fsql/` - filesystem-as-SQL tool
- `cmd/demo/` - lightweight demo program
- `cmd/debug/` - SQL diagnostic helper
- `cmd/catalog_demo/` - catalog and job scheduler demo
- `cmd/tinysql-mcp-server/` - MCP server for TinySQL

Some `cmd/` subtrees contain their own support assets:

- `cmd/server/loadtest/` provides the HTTP load generator for the server.
- `cmd/studio/frontend/` contains the Wails frontend.
- `cmd/wasm_browser/web/` contains the browser UI assets.
- `cmd/formigo/templates/` and `cmd/formigo/static/` contain the web app assets.
- `cmd/accessweb/templates/` and `cmd/accessweb/static/` contain the web app assets.
- `cmd/query_files/` and `cmd/query_files_wasm/` include sample input files and browser assets used in demos.

## Data and examples

- `data/` contains small sample datasets for demos, tests, and documentation.
- `example_test.go`, `example_exists_test.go`, `example_view_dependencies_test.go`,
  `import_example_test.go`, and `example_showcase.sql` demonstrate the public API
  and SQL features. Examples in `exporter/example_test.go` demonstrate the
  public export facade.
- `tests/examples.yml` stores additional example-driven test data.

If you want, the next step is to add a generated tree view or a more detailed module map for one of the major subsystems.
