# query_files_wasm

Part of [tinySQL](../../README.md). This is the local-first browser playground
published at <https://simonwaldherr.github.io/tinySQL/>.

The Go/WASM engine runs in a dedicated browser worker. Imported data and
snapshots stay in the browser unless you export or deliberately share a URL
demo.

## Build and run

```bash
cd cmd/query_files_wasm
./build.sh --build-only
./build.sh --serve                    # http://localhost:8080
PORT=8090 ./build.sh --serve
./build.sh --skip-build --serve       # serve existing assets
```

The build produces the WASM module, its optional gzip companion, the Go runtime
loader, worker RPC files, and IndexedDB workspace support. Browsers fall back
to the uncompressed module when streaming decompression is unavailable.

To update GitHub Pages from the repository root:

```bash
make build-gh-pages-demo
make update-gh-pages
make push-gh-pages
```

The update target reuses a clean gh-pages worktree and refuses to overwrite a
dirty one.

## What the playground does

- imports CSV/TSV/TXT, JSON/JSONL/NDJSON, YAML, XML, Excel, GeoJSON, KML, OSM
  XML, and routing graph data;
- runs one or many SQL statements, browses schemas, drops tables, and retains
  query history;
- keeps named database workspaces in versioned IndexedDB generations, with
  browser OPFS used for larger supported snapshots;
- pages, filters, sorts, copies, and exports results as CSV, TSV, XLSX, JSON,
  XML, HTML, and Markdown;
- generates reusable Go, JavaScript, Python, and cURL query snippets from the
  current statement and result schema;
- includes GIS, routing, full-text, vector, hybrid-search, RAG, analytics,
  stored-procedure, SQL-planning, and text-to-columns examples.

Single-statement queries stream result batches when possible. The displayed
preview is limited to 10,000 rows or 16 MiB and is clearly marked as a prefix;
refine the query or add LIMIT before exporting a complete result. Blocking
shapes such as joins, ORDER BY, aggregates, DISTINCT, and CTEs materialize
before producing their first row.

Browser imports are limited to 64 MiB; snapshot imports are limited to 256 MiB.
Those limits protect the most expensive local paths, not the total memory used
by an in-memory database.

## Recent engine features in the demo

The **Text columns** starter recipe demonstrates `TEXT_TO_COLUMNS(text,
delimiter)` as a table function and `COLUMNS_TO_TEXT(delimiter, ...)` for
reassembling values. This makes pasted delimited values queryable without a
temporary import file. Empty fields remain empty and a NULL delimiter returns
NULL.

Recent columnar result and batched aggregate paths are used internally where a
query shape qualifies; the browser keeps its streaming/paged transport so it
does not materialize a second full copy of results. `IndexAdvisor` deliberately
remains a Go-host integration: it is opt-in, never observes ordinary queries,
and only creates an index when its host explicitly applies a recommendation.
See [columnar execution](../../docs/columnar-execution.md) and [automatic
indexes](../../docs/automatic-indexes.md) for the public APIs and guardrails.

Path-based GeoPackage and MBTiles import need a native sqliteimport build and
are therefore unavailable in this browser app. SQL-level binary inspection
helpers remain available. See the [geospatial standards guide](../../docs/geospatial-standards.md)
for supported profiles and formats.

## Map demos

tiles-demo.html runs live SQL tile lookups in the same WASM engine and includes
a small GeoJSON editing panel. The generated tiles are non-geographic visual
data; regenerate its MBTiles file and browser snapshot with:

```bash
go run -tags=sqliteimport ./cmd/mbtilesdemo
```

tiles-demo-bavaria.html is the separate MapLibre page for its published
same-origin Bavaria snapshot. The full app, map pages, and generator are
included in the gh-pages build inputs.

## Keyboard shortcuts

| Shortcut | Action |
| --- | --- |
| Ctrl/Cmd + Enter | Run the selection or full editor |
| Ctrl/Cmd + Space | Open autocomplete |
| Ctrl/Cmd + Shift + F | Format SQL |
| Tab / Shift + Tab | Indent / unindent |
| Up in an empty editor | Recall the latest query |

## Embed the worker client

The UI owns the Go runtime through TinySQLWasmClient rather than exposing the
Go API on window:

```js
const engine = new TinySQLWasmClient();
await engine.init();
const result = await engine.executeQuery("SELECT 1 AS ready");
```

| Need | Client method |
| --- | --- |
| Import or inspect schema | importFile, listTables, getTableSchema |
| Run SQL | executeQuery, executeQueryStream, executeMulti |
| Page a result | getResultPage |
| Change data | dropTable, clearDatabase |
| Back up or restore | exportDatabaseBytes, importDatabaseBytes, validateDatabaseBytes |
| Export query output | exportResults |
| Inspect local runtime state | getRuntimeStatus |
| Supply host identity | setRuntimeIdentity |

executeMulti recognizes statement separators outside strings, quoted identifiers,
and comments. It accepts at most 50 statements, with a 30-second budget per
statement and 60 seconds overall. An AbortSignal passed to executeQueryStream
cancels work not yet started and forwards cancellation to an active stream when
the engine can yield.

## RAG demo

Use a stable key, normalized text, and VECTOR column. Generate query embeddings
with the ingestion model, then combine lexical and vector ranks with
HYBRID_SEARCH:

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

For schema design, ingestion, tuning, and context expansion, read the
[RAG guide](../../docs/rag-guide.md).
