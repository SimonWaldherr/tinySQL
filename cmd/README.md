# tinySQL commands

This directory contains runnable tinySQL applications, demos, servers, and
WebAssembly builds. For engine capabilities, GIS, storage modes, imports, and
limitations, start with the [root README](../README.md).

Supported geospatial formats and CRS profiles are summarized in the
[geospatial standards guide](../docs/geospatial-standards.md).

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
| Move data between files and databases | [`migrate`](./migrate/README.md) | Structured and geospatial standard formats; build from `cmd/migrate` |

## Servers and applications

| Goal | Command | Notes |
| --- | --- | --- |
| HTTP and gRPC SQL API | [`server`](./server/README.md) | General-purpose API server |
| Exercise the API under load | [`server/loadtest`](./server/loadtest/README.md) | Companion load generator |
| Durable DBMS daemon and tile server | [`tinysqld`](./tinysqld/README.md) | Health, jobs, storage, optional tiles |
| Browser database manager | [`accessweb`](./accessweb/README.md) | Datasheets, CRUD, SQL, export |
| SQL-driven HTML pages | [`tinysqlpage`](./tinysqlpage/README.md) | Render pages from `.sql` definitions |
| Form application | [`formigo`](./formigo/README.md) | tinySQL or SQL Server backend |
| Desktop SQL application | [`studio`](./studio/README.md) | Wails desktop app |
| MCP server for AI hosts | [`tinysql-mcp-server`](./tinysql-mcp-server/README.md) | stdio Model Context Protocol server |

## Reference and local applications

| Goal | Command | Notes |
| --- | --- | --- |
| Inspect catalog and scheduled jobs | [`catalog_demo`](./catalog_demo/README.md) | Console demo or browser dashboard for tables, views, functions, and jobs |
| Search a support knowledge base | [`supportdesk`](./supportdesk/README.md) | Console demo or persistent browser help desk with FTS and tickets |
| Explore a read-only POI snapshot | [`offline_demo`](./offline_demo/README.md) | CLI snapshot search or browser explorer; no map tiles required |
| Try OSM snapping, profiles and turn restrictions | [`routingdemo`](./routingdemo/README.md) | `go run ./cmd/routingdemo`; no external data needed |
| Evaluate local RAG retrieval | [`ragdemo`](./ragdemo/README.md) | Requires a local embedding server |
| Explore tinyORM | [`tinyorm_demo`](./tinyorm_demo/README.md) | CLI demo or persistent places directory with migrations and named parameters |

## Practical local applications

| Goal | Command | Notes |
| --- | --- | --- |
| Track zone entry and exit events | [`geofence-service`](./geofence-service/README.md) | GeoJSON zones, browser UI, and position-ingest API (`:8091`) |
| Search a local document directory | [`docsearch`](./docsearch/README.md) | Persistent full-text index and browser UI (`:8092`) |
| Validate CSV before importing it | [`import-validate`](./import-validate/README.md) | Rules, error history, and explicit commit (`:8093`) |
| Record work and break time | [`worklog`](./worklog/README.md) | Timeout checkout, reports, and CSV export (`:8094`) |

The application-specific browser modes in the two sections above expose small
JSON APIs for local integrations. They intentionally do not implement login,
authorization, or TLS, so they **listen on `127.0.0.1` by default**. Pass
`-addr :<port>` to accept connections from other machines — and put
authentication and TLS in front of them before you do.

Everything in the two sections above is a single-user local application. It is
useful, but it is not a product: there is no login, no per-user data, no backup
command, and no packaging beyond `go build`. Treat the *Reference and local
applications* table as worked examples you can run, and the *Practical local
applications* table as tools that solve a real task for one person on one
machine.

## Browser and WebAssembly

| Goal | Command | Start here |
| --- | --- | --- |
| Full local-first SQL playground | [`query_files_wasm`](./query_files_wasm/README.md) | GitHub Pages app and GIS map editor |
| Minimal browser WASM UI | [`wasm_browser`](./wasm_browser/README.md) | `./build.sh --serve` |
| Run tinySQL from Node.js WASM | [`wasm_node`](./wasm_node/README.md) | `./build.sh --run` |

The focused [map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
is built by `query_files_wasm` and its data generator in
[`mbtilesdemo`](./mbtilesdemo). It runs tile lookups and GeoJSON edits through
the same in-browser tinySQL engine.

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
go build -tags=sqliteimport ./... # SQLite, OGC GeoPackage, and MBTiles
go build -tags=shapefile ./...    # ESRI Shapefile and Shapefile ZIP
go build -tags=sqliteimport,shapefile ./...
```

Use `go test ./...` from the repository root for the main suite. See the
[development guide](../docs/development-guide.md) for the Make targets and
release/demo workflow.
