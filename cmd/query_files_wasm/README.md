# query_files_wasm

Part of [TinySQL](../../README.md). The root guide covers the current GIS
functions; this guide covers the browser build and local-first UI.

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

When Binaryen is installed, release builds run one
`wasm-opt --all-features -Oz` pass. The result replaces the Go build only when
it is smaller and passes `WebAssembly.validate`; otherwise the script keeps the
original Go-generated module. The intentionally omitted `--converge` mode adds
substantial build time for negligible compressed-size savings.

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

## MBTiles demo (`tiles-demo.html`)

A second, focused page: a Leaflet map whose every tile is fetched with one SQL
query (`SELECT tile_data FROM tiles WHERE zoom_level = z AND tile_column = x
AND tile_row = TILE_FLIP_Y(y, z)`), run live by the same `query_files.wasm`
module through `window.executeQuery`. A sidebar logs each query with its
timing and whether its exact SQL text was already seen this session (and is
therefore eligible for the WASM module's compiled-query cache), shows the
tileset's MBTiles metadata, and lets you click the map to run `TILE_ZXY`,
`TILE_QUADKEY` and `TILE_BBOX` for that point.

A `settlements` table (procedurally named and placed points of interest, one
row per marker, geometry stored as the same GeoJSON `GEO_POINT` shape) sits
alongside `tiles` to demonstrate `GEO_DISTANCE`/`GEO_BEARING`/`GEO_MIDPOINT`
against real rows: clicking the map runs a `GEO_DISTANCE` + `ORDER BY ... LIMIT
1` nearest-settlement query, and clicking two markers runs one query
computing the distance, initial bearing and great-circle midpoint between
them (drawn on the map). See [`settlements.go`](../mbtilesdemo/settlements.go).

A `mapshaper_shapes` GeoJSON layer powers the **Mapshaper-style editing**
panel. It executes `ST_SIMPLIFY`, `ST_SMOOTH`, `ST_AFFINE`, and
`ST_REMOVE_HOLES`, `ST_CLEAN`, and `ST_SNAPTOGRID` against a line, a polygon
with a hole, and a multipolygon, then displays the result with its
`ST_ISVALID`, `ST_BBOX`, and `ST_CENTROID`. The dashed outline remains the
source geometry so the transformation is directly visible. You can also upload
any GeoJSON object, tune simplification, smoothing, affine, or snapping
parameters, and download the transformed result without sending the file
anywhere. See [`shapes.go`](../mbtilesdemo/shapes.go).

`tiles-demo-bavaria.html` is a separate real-data MapLibre demonstration. Its
monthly build publishes the sizeable snapshot into the `gh-pages` branch as a
same-origin asset; it must not fetch the GitHub Release download URL at runtime,
because the release redirect does not permit browser CORS access.

The tileset is generated, non-geographic art (value noise, quantized into flat
color bands) — see [`cmd/mbtilesdemo`](../mbtilesdemo) at the repo root. That
generator round-trips the tileset through tinySQL's real MBTiles pipeline
(`importer.ExportMBTiles` then `importer.ImportMBTiles`) and writes two
outputs consumed here: `demo.mbtiles` (a genuine spec-compliant file, offered
as a download on the page) and `tiles-demo-data.js` (the same tileset as a
tinySQL snapshot, loaded once via `importDatabase`). Regenerate both with:

```bash
go run -tags=sqliteimport ./cmd/mbtilesdemo
```

`tiles-demo.html`/`tiles-demo.js`/`tiles-demo-data.js`/`demo.mbtiles` are
included in `GH_PAGES_DEMO_FILES` in the repo root `Makefile`, so `make
update-gh-pages` picks up regenerated files the same way it does `app.js`.

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
  bounding boxes, zone membership, routing graph nodes, route edges,
  `GEO_DISSOLVE`/`GEO_CENTROID_AGG` region aggregates, indexed `GEO_SEARCH`
  bbox lookups, `GEO_INTERSECTS`/`GEO_DISJOINT`/`GEO_CLIP` geometry relations
  and clipping, `GEO_CONVEX_HULL`/`GEO_ENVELOPE`/`GEO_LINE_INTERPOLATE`
  construction, `GEO_BUFFER` service-area circles, and
  `GEO_BEARING`/`GEO_MIDPOINT`/`GEO_DESTINATION`/`GEO_WITHIN_POLYGON`/
  `GEO_POLYGON_AREA`/`GEO_LENGTH` measurement
- search examples: `FTS_SEARCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`,
  `VEC_SEARCH`, `VEC_COSINE_SIMILARITY`, `HYBRID_SEARCH`,
  `RAG_CONTEXT_FROM`, `RAG_SEARCH`,
  `CONTAINS_ALL`/`CONTAINS_ANY`/`CONTAINS_SCORE`, plus `?`/`_`
  single-character and `*`/`%` multi-character FTS wildcards
- recent vector/planning examples: `VEC_HAMMING_DISTANCE`, `VEC_CENTROID`,
  `ANALYZE`, and `sys.statistics`
- analytics examples: `PIVOT`, `RETURNING`, `EXPLAIN`, SQLite-compatible
  `PRAGMA`, views, materialized views, `sys.*` metadata, `EQUAL_INTERVAL`/
  `NATURAL_BREAKS` choropleth classification window functions
- developer UX examples: `BLOB_FROM_HEX`/`BLOB_FROM_BASE64` constructing
  storable blobs, read back with `BLOB_HEX`/`BLOB_LENGTH`
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
