# query_files_wasm

Browser-based tinySQL playground (Go WASM + static HTML/JS UI). This command
builds the static app published on GitHub Pages:

https://simonwaldherr.github.io/tinySQL/

The playground is intentionally local-first: the database runs in the browser,
demo data can be encoded into shareable URL hashes, and imported data is kept in
local storage snapshots unless the user exports it.

## Showcase features

- intro page with guided recipes for recent release features, file analytics,
  geodata, FTS/vector search, RAG context expansion, and joins/reporting
- shareable demo links using `#demo=<base64url-json>`; each link can carry SQL
  plus small sample tables
- imports for CSV/TSV/TXT, JSON/JSONL/NDJSON, YAML, XML, Excel, GeoJSON, KML,
  OSM XML, and routing graph data (`.rg`, `.routinggraph`, `.graph.json`, ...)
- geodata examples: point extraction, distance matrices, radius filters,
  bounding boxes, zone membership, routing graph nodes, and route edges
- search examples: `FTS_SEARCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`,
  `VEC_SEARCH`, `VEC_COSINE_SIMILARITY`, `RAG_CONTEXT_FROM`, and hybrid
  retrieval queries
- analytics examples for recent SQL features: `PIVOT`, `RETURNING`, `EXPLAIN`,
  SQLite-compatible `PRAGMA`, views, and materialized views
- in-memory stored procedure demos via `CALL demo_table_summary()` and
  `CALL demo_release_features()`
- query history, schema inspection, local database snapshot, result filtering,
  sorting, exports, and mobile-optimized layout

## Build

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Generated artifacts:
- `query_files.wasm`
- `query_files.wasm.gz` (if `gzip` is available)
- `wasm_exec.js`

Modern browsers load `query_files.wasm.gz` with streaming decompression when
available. The loader falls back to the uncompressed `.wasm` asset on older
browsers or servers that already apply HTTP compression.

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

To serve already built artifacts without rebuilding:

```bash
./build.sh --skip-build --serve
```

## gh-pages workflow

From the repository root:

```bash
make build-gh-pages-demo
make update-gh-pages
make push-gh-pages
```

`make update-gh-pages` builds the WASM artifacts, checks out the `gh-pages`
branch into `.gh-pages-worktree`, copies the static demo files, and commits the
branch when anything changed. `make push-gh-pages` also pushes the branch.

## UI capabilities

- import CSV/TSV/TXT, JSON/JSONL/NDJSON, YAML, XML, Excel (`.xlsx`, `.xls`),
  GeoJSON, KML, OSM XML, and routing graph files
- execute single- and multi-statement SQL
- schema inspection and table removal
- recent-feature recipes for RAG helpers, spatial SQL, materialized views,
  `PIVOT`, `RETURNING`, `EXPLAIN`, `PRAGMA`, and `sys.*` metadata
- query history, editor state, and database snapshot in local storage
- result filtering, sorting, table copy, VanillaGrid pivot view, and exports as
  CSV, TSV, Markdown, JSON, and XML

## Shareable demos

The app understands URL hashes with this shape:

```text
#demo=<base64url-json>
```

The decoded JSON payload has this structure:

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

On load, the app imports the encoded tables into the WASM database, sets the SQL
editor to the encoded query, and runs it when `autoRun` is true. This mirrors the
shareable-notebook pattern used by liveCalc while keeping the payload specific
to tinySQL demo recipes.

## JS/WASM API

The WASM module exposes:
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
