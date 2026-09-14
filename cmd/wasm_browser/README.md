# tinySQL WASM Browser

Part of [tinySQL](../../README.md). For the fuller local-first browser app and
GIS editor, see [query_files_wasm](../query_files_wasm/README.md).

This command compiles tinySQL to WebAssembly and serves a small browser UI. The
engine runs in the browser after the initial asset download.

## Build and run

```bash
cd cmd/wasm_browser
./build.sh --build-only
./build.sh --serve                    # http://localhost:8080
./build.sh --skip-build --serve       # serve existing assets
```

The build creates web/tinySQL.wasm, an optional gzip companion, and
web/wasm_exec.js. To serve manually:

```bash
GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/
cd web && python3 -m http.server 8080
```

## Included UI

- multi-statement SQL editor and result table;
- schema inspector;
- CSV and JSON result export;
- restored editor state, selected demo, and database snapshot;
- generated SQL helpers for CRS, WMS axis order, OGC TileMatrix, WKB/EWKB, and
  GeoPackageBinary.

The browser build excludes the path-based sqliteimport GeoPackage and MBTiles
reader. See the [geospatial standards guide](../../docs/geospatial-standards.md)
for supported formats and profiles.

## JavaScript API

The module exposes window.tinySQL:

| Function | Purpose |
| --- | --- |
| tinySQL.open([dsn]) | Open an in-memory database |
| tinySQL.close() | Close the current connection |
| tinySQL.exec(sql) | Execute a statement |
| tinySQL.query(sql) | Execute a query and return rows |
| tinySQL.exportDB() | Return a base64 GOB snapshot |
| tinySQL.importDB(snapshot) | Replace the current database from a snapshot |
| tinySQL.listTables() | Return table metadata |
| tinySQL.describeTable(table) | Return column definitions |

## Notes

The database is in memory. The demo saves a compact snapshot to localStorage
after successful mutations and restores it on reload; clear site data for the
demo origin to reset it.

WASM assets must be served over HTTP rather than file URLs. Modern browsers
load the gzip companion through DecompressionStream when possible; other
browsers and already-compressing hosts fall back to the uncompressed module.

Browser and Node builds use an adaptive result bridge: larger JSON-compatible
primitive result sets cross the WASM boundary as one JSON payload, while small
or special values retain direct conversion. Callers receive the same JavaScript
response shape either way. Set window.tinySQLWasmDebug to true before loading
the module to enable diagnostic logs.

For Node.js, see [wasm_node](../wasm_node/README.md).
