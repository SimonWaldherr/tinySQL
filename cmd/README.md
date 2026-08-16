# TinySQL commands

This directory contains runnable TinySQL applications, demos, servers, and
WebAssembly builds. For engine capabilities, GIS, storage modes, imports, and
limitations, start with the [root README](../README.md).

## Choose a command

| Goal | Command | Start here |
| --- | --- | --- |
| Explore SQL features with seed data | [`demo`](./demo/README.md) | `go run ./cmd/demo` |
| Use an interactive SQL shell | [`repl`](./repl/README.md) | `go run ./cmd/repl` |
| Use a SQLite-like CLI | [`tinysql`](./tinysql/README.md) | `go run ./cmd/tinysql` |
| Format, lint, validate, or explain SQL | [`sqltools`](./sqltools/README.md) | `go run ./cmd/sqltools` |
| Diagnose SQL against a fresh database | [`debug`](./debug/README.md) | `go run ./cmd/debug` |
| Query files directly with SQL | [`query_files`](./query_files/README.md) | `go run ./cmd/query_files` |
| Query filesystem metadata and content | [`fsql`](./fsql/README.md) | build from `cmd/fsql` |
| Move data between files and databases | [`migrate`](./migrate/README.md) | build from `cmd/migrate` |

## Servers and applications

| Goal | Command | Notes |
| --- | --- | --- |
| HTTP and gRPC SQL API | [`server`](./server/README.md) | General-purpose API server |
| Exercise the API under load | [`server/loadtest`](./server/loadtest/README.md) | Companion load generator |
| Durable DBMS daemon and tile server | [`tinysqld`](./tinysqld/README.md) | Health, jobs, storage, optional tiles |
| Browser database manager | [`accessweb`](./accessweb/README.md) | Datasheets, CRUD, SQL, export |
| SQL-driven HTML pages | [`tinysqlpage`](./tinysqlpage/README.md) | Render pages from `.sql` definitions |
| Form application | [`formigo`](./formigo/README.md) | TinySQL or SQL Server backend |
| Desktop SQL application | [`studio`](./studio/README.md) | Wails desktop app |
| MCP server for AI hosts | [`tinysql-mcp-server`](./tinysql-mcp-server/README.md) | stdio Model Context Protocol server |

## Demos and reference applications

| Goal | Command | Notes |
| --- | --- | --- |
| Catalog and scheduler APIs | [`catalog_demo`](./catalog_demo/README.md) | Tables, views, functions, jobs |
| Search a support knowledge base | [`supportdesk`](./supportdesk/README.md) | Driver, transactions, trigger, view, CTE, full-text search |
| Local read-only POI snapshot | [`offline_demo`](./offline_demo/README.md) | No service or network required |
| Evaluate local RAG retrieval | [`ragdemo`](./ragdemo/README.md) | Requires a local embedding server |
| Explore tinyORM | [`tinyorm_demo`](./tinyorm_demo/README.md) | Migrations and named parameters |

## Practical local applications

| Goal | Command | Notes |
| --- | --- | --- |
| Track zone entry and exit events | [`geofence-service`](./geofence-service/README.md) | GeoJSON zones, GPS API, event UI |
| Search a local document directory | [`docsearch`](./docsearch/README.md) | Persistent full-text index and browser UI |
| Validate CSV before importing it | [`import-validate`](./import-validate/README.md) | Rules, error history, explicit commit |
| Record work and break time | [`worklog`](./worklog/README.md) | Timeout checkout, reports, CSV export |

## Browser and WebAssembly

| Goal | Command | Start here |
| --- | --- | --- |
| Full local-first SQL playground | [`query_files_wasm`](./query_files_wasm/README.md) | GitHub Pages app and GIS map editor |
| Minimal browser WASM UI | [`wasm_browser`](./wasm_browser/README.md) | `./build.sh --serve` |
| Run TinySQL from Node.js WASM | [`wasm_node`](./wasm_node/README.md) | `./build.sh --run` |

The focused [map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
is built by `query_files_wasm` and its data generator in
[`mbtilesdemo`](./mbtilesdemo). It runs tile lookups and GeoJSON edits through
the same in-browser TinySQL engine.

## Build conventions

Most commands can be built from the repository root:

```bash
go build ./cmd/<name>
go run ./cmd/<name> --help
```

`fsql`, `migrate`, `studio`, and the WebAssembly applications have their own
build workflows; follow their command README. Optional file importers use build
tags:

```bash
go build -tags=sqliteimport ./...
go build -tags=shapefile ./...
go build -tags=sqliteimport,shapefile ./...
```

Use `go test ./...` from the repository root for the main suite. See the
[development guide](../docs/development-guide.md) for the Make targets and
release/demo workflow.
