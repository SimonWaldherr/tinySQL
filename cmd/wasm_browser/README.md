# tinySQL WASM Browser (`wasm_browser`)

Part of [tinySQL](../../README.md). For the full local-first browser app and
GIS editor, see [`query_files_wasm`](../query_files_wasm/README.md).

Compiles tinySQL to WebAssembly and serves it with a browser UI. The SQL engine
runs client-side; no server is needed after the initial file download.

## Build

```bash
cd cmd/wasm_browser

# Build only (produces web/tinySQL.wasm, optional .gz, and web/wasm_exec.js)
./build.sh --build-only

# Build and start a local HTTP server on port 8080
./build.sh --serve

# Serve existing assets without rebuilding
./build.sh --skip-build --serve
```

Then open http://localhost:8080.

Manual equivalent:

```bash
cd cmd/wasm_browser
GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/
cd web && python3 -m http.server 8080
```

## UI features

- SQL editor with multi-statement support
- Result table with column headers
- Schema inspector (list tables, show columns)
- SQL editor state, selected demo query, and the database snapshot are restored
  from `localStorage`
- Export results as CSV or JSON
- CRS, WMS axis-order, OGC TileMatrix, WKB/EWKB, and
  GeoPackageBinary SQL functions from the generated function reference

The browser build excludes the path-based `sqliteimport` GeoPackage/MBTiles
reader. See the [geospatial standards guide](../../docs/geospatial-standards.md)
for supported formats and CRS profiles.

## JavaScript API

The module exposes `window.tinySQL`:

| Function | Description |
|----------|-------------|
| `tinySQL.open([dsn])` | Open an in-memory database (`mem://?tenant=name`) |
| `tinySQL.close()` | Close the current connection |
| `tinySQL.exec(sql)` | Execute a SQL statement |
| `tinySQL.query(sql)` | Execute a query and return rows |
| `tinySQL.exportDB()` | Return a base64-encoded GOB snapshot |
| `tinySQL.importDB(snapshot)` | Replace the current database from a snapshot |
| `tinySQL.listTables()` | Return table metadata |
| `tinySQL.describeTable(table)` | Return column definitions for a table |

## Notes

- The engine runs in memory. The demo persists a compact database snapshot to
  `localStorage` after successful mutations and restores it on reload. Clear the
  browser's site data for the demo origin to reset it.
- WASM files must be served over HTTP (not `file://`) due to browser security
  restrictions — use the built-in server or any static file host.
- Modern browsers load the generated `tinySQL.wasm.gz` companion and stream it
  through `DecompressionStream`; older browsers and hosts that already apply
  compression fall back to `tinySQL.wasm` automatically.
- The browser API executes directly against tinySQL's engine instead of through
  `database/sql`; this reduces bundle size and avoids connection-pool overhead.
- Repeated SQL text is served from a bounded compile cache, avoiding parser work
  on hot query paths.
- Set `window.tinySQLWasmDebug = true` before loading the module to enable
  diagnostic console logs; normal query paths stay quiet.
- For a Node.js variant see [`../wasm_node/`](../wasm_node/).

## Performance benchmark

Run the repeat-query benchmark against a built browser module:

```bash
node wasm_benchmark.js web/tinySQL.wasm 10000
```

The output reports elapsed time, throughput, and microseconds per query or
update as JSON. Pass another WASM file as the first argument to compare builds.

### Result transfer

Browser and Node builds share an adaptive result bridge: responses with at least
32 rows of JSON-compatible primitive values are serialized once and converted
into native JavaScript objects with `JSON.parse`. Smaller responses use direct
`syscall/js` conversion. The public response remains an object in both cases.
Special numeric values, float32, malformed UTF-8 and nil inner rows retain the
direct conversion path to preserve their existing semantics. `elapsed_ms` retains
its historical nanosecond units.

On an Apple M2 Max with Go 1.27.1 and Node 26.8.1, three alternating runs of 2,000
queries each gave these median wall-clock times (including result transfer):

| Query | Before | After |
| --- | ---: | ---: |
| Constant, one row | 41.74 µs | 42.14 µs |
| Filtered scan, 20 rows | 132.85 µs | 133.45 µs |
| Full result, 200 rows / 600 cells | 935.10 µs | 517.19 µs |

The large-result fixture takes about 45% less time; small-result differences are
within the observed measurement variation. The stripped Go WASM grew by 3,366
bytes (about 1,232 bytes with gzip). These measurements use Node's V8 runtime;
other browsers, result sizes and data types may have different tradeoffs.

To use the runtime support file matching a different Go toolchain:

```sh
WASM_EXEC_JS="$(go env GOROOT)/lib/wasm/wasm_exec.js" \
  node wasm_benchmark.js /path/to/tinySQL.wasm 2000
```

The benchmark verifies all 200 result rows and an empty result before timing.
Bridge edge-case tests execute inside WASM:

```sh
# Run from the repository root.
GOOS=js GOARCH=wasm go test \
  -exec="$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./internal/wasmbridge
```
