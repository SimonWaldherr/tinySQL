# query_files_wasm

Part of [tinySQL](../../README.md). The root guide covers the current GIS
functions; this guide covers the browser build and local-first UI.

Browser-based tinySQL playground (Go WASM plus static HTML/JS UI). This command
builds the static app published on GitHub Pages:

https://simonwaldherr.github.io/tinySQL/

The playground is local-first: the database runs in a dedicated browser worker,
demo data can be encoded into shareable URL hashes, and imported data stays in
versioned local workspaces unless exported.

## Build

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Artifacts:

- `query_files.wasm`
- `query_files.wasm.gz` (if `gzip` is available)
- `wasm_exec.js`
- `wasm-worker.js` and `wasm-client.js` (the worker RPC runtime)
- `workspace-storage.js` (IndexedDB workspaces and recovery generations)

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
- query history and editor state in local storage; database snapshots in named,
  versioned workspaces with three retained recovery generations, size,
  modification time, and a one-time verified migration from the former
  `localStorage` snapshot. IndexedDB owns metadata and small payloads; on
  supported browsers, new raw binary snapshots at or above 8 MiB live in OPFS.
  Newer builds transfer snapshot bytes directly between WASM Worker, UI, and
  storage instead of Base64-encoding every autosave or restore
- a Worker-owned Go/WASM database and serialized Promise RPC, so imports,
  queries, result paging, exports, snapshots, and monitoring do not block the
  UI thread
- a `ResultStream`-backed path for single statements: rows arrive in bounded
  batches, the visible preview is capped at 10,000 rows / 16 MiB, and its
  Cancel button cancels the Go context between batches
- live runtime monitoring for active/total/failed/timed-out requests, request
  throughput and latency, peak concurrency, real Busy/Idle time, Go heap usage,
  table/row counts, query-cache occupancy, and per-user/session counters
- WASM-side result paging, filtering, and sorting, so large result sets stay in
  Go memory instead of being copied wholesale into JavaScript; table copy,
  a pivot-and-chart view (via VanillaGrid/D3), and file downloads as CSV, TSV,
  XLSX, JSON, XML, HTML, and Markdown
- a result code generator for Go structs, TypeScript interfaces, Python
  dataclasses, and SQL `CREATE TABLE` statements; inferred types are based on
  the visible result page and generated snippets can be copied or downloaded
- intro page with guided recipes: file analytics, geodata, FTS/vector search,
  RAG context expansion, joins/reporting
- geodata examples: point extraction, distance matrices, radius filters,
  bounding boxes, zone membership, routing graph nodes, route edges,
  EPSG/OGC CRS normalization, WMS 1.3 axis ordering, OGC TileMatrix
  bounds/positions, and GeoPackageBinary inspection,
  `GEO_DISSOLVE`/`GEO_CENTROID_AGG` region aggregates, indexed `GEO_SEARCH`
  bbox lookups, `GEO_INTERSECTS`/`GEO_DISJOINT`/`GEO_CLIP` geometry relations
  and clipping, `GEO_CONVEX_HULL`/`GEO_ENVELOPE`/`GEO_LINE_INTERPOLATE`
  construction, `GEO_BUFFER` service-area circles, and
  `GEO_BEARING`/`GEO_MIDPOINT`/`GEO_DESTINATION`/`GEO_WITHIN_POLYGON`/
  `GEO_POLYGON_AREA`/`GEO_LENGTH` measurement
- late-August OGC interoperability examples: WKT/EWKT and WKB/EWKB round
  trips, GeoPackageBinary inspection, geohash cells, `ST_TRANSFORM`,
  `ST_TOUCHES`/`ST_COVERS`/`ST_PERIMETER`, CRS normalization, WMS 1.3 axis
  ordering, and generic OGC TileMatrix bounds/positions
- search examples: `FTS_SEARCH`, `FTS_RANK`, `FTS_SNIPPET`, `BM25`,
  `VEC_SEARCH`, `VEC_COSINE_SIMILARITY`, `HYBRID_SEARCH`,
  `RAG_CONTEXT_FROM`, `RAG_SEARCH`,
  `CONTAINS_ALL`/`CONTAINS_ANY`/`CONTAINS_SCORE`, plus `?`/`_`
  single-character and `*`/`%` multi-character FTS wildcards
- pre-ranking tenant/ACL and spatial filtering through
  `VEC_SEARCH_FILTERED`, plus `RAG_WARM` cache preparation; the sample RAG
  corpus includes public/private rows and WGS84 geometry so the boundary is
  visible in the result
- route graph preparation and execution with `ROUTE_WARM`,
  `ROUTE_SHORTEST_PATH`, and `ROUTE_DISTANCE`
- recent vector/planning examples: `VEC_HAMMING_DISTANCE`, `VEC_CENTROID`,
  `ANALYZE`, and `sys.statistics`
- analytics examples: `PIVOT`, `RETURNING`, `EXPLAIN`, SQLite-compatible
  `PRAGMA`, views, materialized views, `sys.*` metadata, `EQUAL_INTERVAL`/
  `NATURAL_BREAKS` choropleth classification window functions
- developer UX examples: `BLOB_FROM_HEX`/`BLOB_FROM_BASE64` constructing
  storable blobs, read back with `BLOB_HEX`/`BLOB_LENGTH`
- reusable stored procedure demos with metadata, safe arguments, read-only
  scheduling, atomic writes, and runtime statistics:
  `CALL demo_table_summary()`, `CALL demo_runtime_status()`,
  `CALL demo_geo_distance(52.52, 13.405, 48.1372, 11.5755)`,
  `CALL demo_find_functions('RAG%')`,
  `CALL demo_log_event('browser', 'demo')`, and
  `CALL demo_release_features()`

Path-based OGC GeoPackage and MBTiles imports require the native
`sqliteimport` build and are therefore not part of the WASM upload path; their
SQL-level binary inspection helpers remain available. The profile matrix is
documented in the
[geospatial standards guide](../../docs/geospatial-standards.md).

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

The query studio does not expose the Go API on `window`: it owns the runtime in
`wasm-worker.js` and calls it asynchronously through `TinySQLWasmClient`.

```js
const engine = new TinySQLWasmClient();
await engine.init();
const result = await engine.executeQuery('SELECT 1 AS ready');
```

The client offers the methods below as Promise-returning functions. For queued
work it also accepts `engine.call(method, args, { signal })`; an `AbortSignal`
removes work that has not started yet. `executeQueryStream` additionally
forwards an active abort straight to a Go `Context`: its timer-scheduled
`ResultStream` batches yield back to the worker between chunks, so direct scans
can be cancelled while they are running. Blocking query shapes (for example
joins, `ORDER BY`, aggregates, `DISTINCT`, and CTEs) still materialize inside
the engine before their first row and may only acknowledge cancellation once
that phase yields; the normal timeout remains their guardrail.

- `importFile(fileName, fileContent, tableName)`
- `executeQuery(sql)`
- `executeQueryStream(sql, { signal })`
- `executeMulti(sql)`
- `getResultPage(offset, limit, filterText, sortColumn, sortDirection)`
- `listTables()`
- `getTableSchema(tableName)`
- `dropTable(tableName)`
- `clearDatabase()`
- `exportDatabase()` / `importDatabase(snapshot)` (legacy Base64-compatible)
- `exportDatabaseBytes()` / `importDatabaseBytes(snapshot)` (binary
  `Uint8Array`/`ArrayBuffer` workspace path)
- `validateDatabaseBytes(snapshot)` (loads a candidate in isolation without
  replacing the active database)
- `exportResults(format)`
- `getRuntimeStatus()`
- `setRuntimeIdentity(userId, sessionId)`

`executeMulti` recognizes statement separators only outside SQL strings, quoted
identifiers, and line/block comments, so scripts can safely contain semicolons
in those constructs. A script is limited to 50 statements, 30 seconds per
statement, and a 60-second total budget. Its final materialized result is
retained only up to the same 10,000-row / 16-MiB preview cap; a capped result
is marked preview-only and cannot be exported as if complete.

`executeQueryStream` is selected by the browser UI for one statement (a
trailing semicolon is fine). It emits live row chunks and keeps a complete
bounded result available to the existing pager and exporter. If the 10,000-row
or 16-MiB cap is reached, the result becomes an explicitly marked prefix and
full-result export is disabled; use `LIMIT` or refine the query instead.

The materialized query path returns only the first result page plus `totalRows`. Use
`getResultPage` for subsequent pages and WASM-side filtering/sorting.
`exportResults` still exports the complete unfiltered result unless the UI
explicitly marks it as a bounded preview.
XLSX and HTML downloads intentionally represent the currently visible result
page, matching the pivot-and-chart view; use CSV, JSON, or XML for a complete
unfiltered WASM-side export.
The result view reuses its filtered/sorted row index across page changes, and
workspace snapshot writes are coalesced and deferred to browser idle time.
`workspace-storage.js` uses IndexedDB as its portable binary backend, retains
the newest three immutable generations, and validates versions during recovery.
The modern validation pass is isolated from the active database; only the
selected valid generation is then imported. Binary snapshot ArrayBuffers move
between the worker and UI using transferable ownership, so autosave and restore
avoid Base64 expansion and its extra full-size text copies. `exportDatabase()`
and `importDatabase()` remain for compatibility with older demo assets and
shared snapshot code.
For raw binary snapshots of at least 8 MiB, supported browsers additionally
store the immutable payload in OPFS; IndexedDB still owns the generation
metadata and current pointer. The OPFS file is written and closed before that
pointer transaction, so an interrupted save can at most leave an unreachable
file, never replace the last recoverable generation. Small snapshots, legacy
records, and OPFS errors continue to use IndexedDB. Retention removes an OPFS
file only after its IndexedDB generation was committed as obsolete.

Browser file imports are capped at 64 MiB before the file is read, and the Go
importer enforces the same input budget. This bounds the local demo's most
expensive import path; binary snapshot imports are additionally capped at
256 MiB before their Go buffer is allocated. These are input limits, not a hard
total-heap limit for the in-memory database.

`getRuntimeStatus` returns one consistent JSON snapshot of request, operation,
database, cache, memory, user, and session metrics. The browser app assigns a
stable session ID for the tab and identifies itself as `local-browser`; a host
embedding the WASM module can provide its authenticated user and session with
`setRuntimeIdentity`. The standalone browser build still has one local database
instance per tab, so its observed-user count is normally one. The schema is
already multi-user-capable, but cross-user aggregation belongs in a shared host
or worker/service layer rather than pretending separate browser tabs share a
runtime.

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
