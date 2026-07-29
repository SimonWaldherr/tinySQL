# tinySQL WASM Browser (`wasm_browser`)

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
