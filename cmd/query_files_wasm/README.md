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

`make build-gh-pages-demo` reuses the WASM build when neither its Go inputs nor
the Go WASM runtime changed, then validates the deployable assets. `make
update-gh-pages` reuses a clean `gh-pages` worktree, copies only changed files,
and commits only when generated content differs. It refuses to overwrite a
dirty worktree. `make push-gh-pages` also pushes the branch.

## UI capabilities

- imports for CSV/TSV/TXT, JSON/JSONL/NDJSON, YAML, XML, Excel (`.xlsx`,
  `.xls`), GeoJSON, KML, OSM XML, and routing graph data (`.rg`,
  `.routinggraph`, `.graph.json`, ...)
- single- and multi-statement SQL execution, schema inspection, table removal
- query history, editor state, and database snapshot in local storage
- WASM-side result paging, filtering, and sorting, so large result sets stay in
  Go memory instead of being copied wholesale into JavaScript; table copy,
  VanillaGrid pivot view, and exports as CSV, TSV, Markdown, JSON, and XML
- intro page with guided recipes: file analytics, geodata, FTS/vector search,
  RAG context expansion, joins/reporting
- geodata examples: point extraction, distance matrices, radius filters,
  bounding boxes, zone membership, routing graph nodes, route edges
- search examples: `FTS_SEARCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`,
  `VEC_SEARCH`, `VEC_COSINE_SIMILARITY`, `HYBRID_SEARCH`,
  `RAG_CONTEXT_FROM`, `RAG_SEARCH`,
  `CONTAINS_ALL`/`CONTAINS_ANY`/`CONTAINS_SCORE`, plus `?`/`_`
  single-character and `*`/`%` multi-character FTS wildcards
- recent vector/planning examples: `VEC_HAMMING_DISTANCE`, `VEC_CENTROID`,
  `ANALYZE`, and `sys.statistics`
- analytics examples: `PIVOT`, `RETURNING`, `EXPLAIN`, SQLite-compatible
  `PRAGMA`, views, materialized views, `sys.*` metadata
- in-memory stored procedure demos via `CALL demo_table_summary()` and
  `CALL demo_release_features()`

## Keyboard shortcuts

The editor supports:

| Shortcut | Action |
| --- | --- |
| `Ctrl`/`Cmd`+`Enter` | Run the selected SQL, or the full editor |
| `Ctrl`/`Cmd`+`Space` | Open autocomplete suggestions |
| `Ctrl`/`Cmd`+`Shift`+`F` | Format the query |
| `Tab` / `Shift`+`Tab` | Indent / unindent the selection |
| `↑` (empty editor) | Recall the last query from history |
| `(`, `'`, `"` | Auto-close the matching bracket/quote |

Also reachable from the **Tools** menu in the toolbar.

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
- `getResultPage(offset, limit, filterText, sortColumn, sortDirection)`
- `listTables()`
- `getTableSchema(tableName)`
- `dropTable(tableName)`
- `clearDatabase()`
- `exportDatabase()`
- `importDatabase(snapshot)`
- `exportResults(format)`

`executeMulti` recognizes statement separators only outside SQL strings, quoted
identifiers, and line/block comments, so scripts can safely contain semicolons
in those constructs.

Query execution returns only the first result page plus `totalRows`. Use
`getResultPage` for subsequent pages and WASM-side filtering/sorting.
`exportResults` still exports the complete unfiltered result.

## Recommended RAG workflow

Use a table with a stable primary key, a normalized text column, and a `VECTOR`
column. Generate the query embedding with the same model used at ingestion,
then call `HYBRID_SEARCH` so exact terms, wildcard patterns, and semantic
similarity contribute to one reciprocal-rank-fused result:

```sql
SELECT chunk_id, chunk_text, _vec_rank, _fts_rank, _rrf_rank, _rrf_score
FROM HYBRID_SEARCH(
  'rag_chunks',
  'embedding',
  'search_text',
  'auth?nticat* OR SSO',
  VEC_FROM_JSON('[0.12, -0.07, 0.31]'),
  20
)
ORDER BY _rrf_rank;
```

The full production-oriented schema, ingestion, chunking, tuning, evaluation,
and context-expansion recommendations are in
[`docs/rag-guide.md`](../../docs/rag-guide.md).
