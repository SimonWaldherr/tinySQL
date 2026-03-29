# query_files_wasm

Browser-based tinySQL playground (Go WASM + static HTML/JS UI).

## Build

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Generated artifacts:
- `query_files.wasm`
- `query_files.wasm.gz` (if `gzip` is available)
- `wasm_exec.js`

## Run locally

```bash
cd cmd/query_files_wasm
./build.sh --serve
# open http://localhost:8080
```

You can override the port:

```bash
PORT=8090 ./build.sh --serve
```

## UI capabilities

- import CSV/TSV/TXT, JSON/JSONL/NDJSON, XML, and Excel (`.xlsx`, `.xls`)
- execute single- and multi-statement SQL
- schema inspection and table removal
- query history in local storage
- export results as CSV, JSON, XML

## JS/WASM API

The WASM module exposes:
- `importFile(fileName, fileContent, tableName)`
- `executeQuery(sql)`
- `executeMulti(sql)`
- `listTables()`
- `getTableSchema(tableName)`
- `dropTable(tableName)`
- `clearDatabase()`
- `exportResults(format)`
