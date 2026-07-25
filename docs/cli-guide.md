# Using TinySQL from the Command Line

`cmd/` holds tools and demos built on the same engine as the Go library. Full
list: [cmd/README.md](../cmd/README.md).

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
| `cmd/tinysql-mcp-server` | Serves a TinySQL database over MCP for LLM agents |

Build with `make build` (main CLI) or `make build-all` (CLI + common demos);
see the [Development Guide](./development-guide.md).

Security note: `cmd/server` defaults to authentication off and listens on all
interfaces. Use `-auth`, bind to localhost, and configure TLS before exposing
it outside a trusted environment.

## Browser playground

The [gh-pages playground](https://simonwaldherr.github.io/tinySQL/) is built
from `cmd/query_files_wasm` and runs tinySQL as WebAssembly with no backend:

- local-first file analytics for CSV, JSON, JSONL/NDJSON, YAML, XML, Excel,
  GeoJSON, KML, OSM XML, and routing graph files;
- SQL joins, CTEs, window functions, exports, schema inspection, and persisted
  browser snapshots;
- geodata recipes for distance matrices, radius filters, bounding boxes, zones,
  routing graph edges, and node lookups;
- full-text search, vector search, hybrid retrieval, and in-memory stored
  procedure examples;
- shareable demo URLs with SQL and sample data encoded in the URL hash;
- a pre-compressed WASM companion with a streaming-decompression loader for
  lower static-host download cost, plus an uncompressed fallback.

```bash
make build-gh-pages-demo
make update-gh-pages
make push-gh-pages
```

To embed TinySQL as WASM in your own frontend instead of using the demo, see
the [Developer Integration Guide](./developer-integration.md).
