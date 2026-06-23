# tinySQL WASM Browser (`wasm_browser`)

Compiles tinySQL to **WebAssembly** and serves it with a modern browser UI.
The entire SQL engine runs client-side — no server required after the initial
file download.

## Build

### All-in-one script

```bash
cd cmd/wasm_browser

# Build only (produces web/tinySQL.wasm, optional .gz, and web/wasm_exec.js)
./build.sh --build-only

# Build and start a local HTTP server on port 8080
./build.sh --serve

# Serve existing assets without rebuilding
./build.sh --skip-build --serve
```

Then open **http://localhost:8080** in your browser.

### Manual build

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

The WASM module exposes `window.tinySQL` functions callable from the browser
console or your own JavaScript:

| Function | Description |
|----------|-------------|
| `tinySQL.open([dsn])` | Open an in-memory database connection |
| `tinySQL.close()` | Close the current connection |
| `tinySQL.exec(sql)` | Execute a SQL statement |
| `tinySQL.query(sql)` | Execute a query and return rows |
| `tinySQL.exportDB()` | Return a base64-encoded GOB snapshot |
| `tinySQL.importDB(snapshot)` | Replace the current database from a snapshot |
| `tinySQL.listTables()` | Return table metadata |
| `tinySQL.describeTable(table)` | Return column definitions for a table |

## Notes

- The SQL engine still runs in memory. The browser demo persists a compact
  database snapshot to `localStorage` after successful mutations and restores
  it on reload.
- Clear the browser's site data for the demo origin to reset the persisted
  snapshot.
- WASM files must be served over HTTP (not `file://`) due to browser security
  restrictions — use the built-in server or any static file host.
- For a Node.js variant see [`../wasm_node/`](../wasm_node/).
