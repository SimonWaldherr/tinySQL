# query_files_wasm

Browser-based tinySQL playground (Go WASM plus static HTML/JS UI). This command
builds the static app published on GitHub Pages:

https://simonwaldherr.github.io/tinySQL/

The playground is local-first: the database runs in the browser, demo data can be
encoded into shareable URL hashes, and imported data stays in local storage
snapshots unless exported.

## Build

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Artifacts:

- `query_files.wasm`
- `query_files.wasm.gz` (if `gzip` is available)
- `wasm_exec.js`

Modern browsers load `query_files.wasm.gz` with streaming decompression. The
loader falls back to the uncompressed `.wasm` asset on older browsers or servers
that already apply HTTP compression.

## Run locally

```bash
cd cmd/query_files_wasm
./build.sh --serve          # open http://localhost:8080
PORT=8090 ./build.sh --serve
./build.sh --skip-build --serve   # serve existing artifacts
```

## gh-pages workflow

From the repository root:

```bash
make build-gh-pages-demo
make update-gh-pages
make push-gh-pages
```

`make update-gh-pages` builds the WASM artifacts, checks out the `gh-pages`
branch into `.gh-pages-worktree`, copies the static demo files, and commits when
anything changed. `make push-gh-pages` also pushes the branch.

## UI capabilities

- imports for CSV/TSV/TXT, JSON/JSONL/NDJSON, YAML, XML, Excel (`.xlsx`,
  `.xls`), GeoJSON, KML, OSM XML, and routing graph data (`.rg`,
  `.routinggraph`, `.graph.json`, ...)
- single- and multi-statement SQL execution, schema inspection, table removal
- query history, editor state, and database snapshot in local storage
- result filtering, sorting, table copy, VanillaGrid pivot view, and exports as
  CSV, TSV, Markdown, JSON, and XML
- intro page with guided recipes: file analytics, geodata, FTS/vector search,
  RAG context expansion, joins/reporting
- geodata examples: point extraction, distance matrices, radius filters,
  bounding boxes, zone membership, routing graph nodes, route edges
- search examples: `FTS_SEARCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`,
  `VEC_SEARCH`, `VEC_COSINE_SIMILARITY`, `RAG_CONTEXT_FROM`, hybrid retrieval
- analytics examples: `PIVOT`, `RETURNING`, `EXPLAIN`, SQLite-compatible
  `PRAGMA`, views, materialized views, `sys.*` metadata
- in-memory stored procedure demos via `CALL demo_table_summary()` and
  `CALL demo_release_features()`

## Shareable demos

The app reads URL hashes of the form `#demo=<base64url-json>`, where the decoded
payload carries SQL plus small sample tables:

```json
{
  "kind": "tinysql-demo",
  "version": 1,
  "id": "geo",
  "title": "Geodata lab",
  "query": "SELECT ...",
  "autoRun": true,
  "tables": [
    {
      "name": "places_geo",
      "fileName": "places.geojson",
      "content": "{\"type\":\"FeatureCollection\",...}"
    }
  ]
}
```

On load the app imports the encoded tables into the WASM database, sets the SQL
editor to the encoded query, and runs it when `autoRun` is true.

## JS/WASM API

- `importFile(fileName, fileContent, tableName)`
- `executeQuery(sql)`
- `executeMulti(sql)`
- `listTables()`
- `getTableSchema(tableName)`
- `dropTable(tableName)`
- `clearDatabase()`
- `exportDatabase()`
- `importDatabase(snapshot)`
- `exportResults(format)`
