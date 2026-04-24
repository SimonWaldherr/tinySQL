# tinySQL WASM Browser (`wasm_browser`)

Compiles tinySQL to **WebAssembly** and serves it with a modern browser UI.
The entire SQL engine runs client-side — no server required after the initial
file download.

## Build

### All-in-one script

```bash
cd cmd/wasm_browser

# Build only (produces web/tinySQL.wasm + web/wasm_exec.js)
./build.sh --build-only

# Build and start a local HTTP server on port 8080
./build.sh --serve
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
- Query history stored in `localStorage`
- Export results as CSV or JSON

## JavaScript API

The WASM module exposes global functions callable from the browser console or
your own JavaScript:

| Function | Description |
|----------|-------------|
| `executeSQL(sql)` | Execute one or more SQL statements |
| `listTables()` | Return a JSON array of table names |
| `getSchema(table)` | Return column definitions for a table |
| `clearDatabase()` | Reset the in-memory database |

## Notes

- The database is **in-memory** and resets on page reload.
- WASM files must be served over HTTP (not `file://`) due to browser security
  restrictions — use the built-in server or any static file host.
- For a Node.js variant see [`../wasm_node/`](../wasm_node/).