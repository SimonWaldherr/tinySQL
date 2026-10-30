# query_files_wasm

Part of [tinySQL](../../README.md). This is the local-first browser playground
published at <https://simonwaldherr.github.io/tinySQL/>.

The Go/WASM engine runs in a dedicated browser worker. Imported data and
snapshots stay in the browser unless you export or deliberately share a URL
demo.

Shared demos open in a new local workspace after the current one has been
saved. If browser workspace storage is unavailable, a shared link can still
start in a fresh temporary engine; opening one over an active temporary
database is disabled so its in-memory data is not lost.

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

To exercise workspace recovery against a real browser IndexedDB and compare
it with a committed baseline, run `./benchmark-workspace-recovery.sh HEAD`
from this directory and open the printed local URL. The test uses a temporary
database, five 8 MiB generations, and six timed recovery pairs; it reports
storage recovery time without the subsequent Go/WASM snapshot import. It also
checks concurrent saves, retention, and recovery from a rejected snapshot.
The optional Git reference selects the baseline, and `PORT` changes the local
server port. Stop the server with Ctrl-C.

Similarly, `./benchmark-workspace-save.sh HEAD` compares saving retained
generations and checks concurrent saves, retention, and OPFS rollback and
cleanup in a browser. Both scripts use isolated temporary databases and files.

## Tests and benchmarks

Run native Go, JavaScript, and Go/WASM tests from the repository root (Node.js
is required for the JavaScript and Go/WASM tests):

```bash
make test-query-files-wasm GO_TEST_FLAGS=-count=1
node cmd/query_files_wasm/result-view.bench.cjs
node cmd/query_files_wasm/export-xlsx.bench.cjs
node cmd/query_files_wasm/editor-highlight.bench.cjs
node cmd/query_files_wasm/editor-position.bench.cjs
node cmd/query_files_wasm/result-cells.bench.cjs
node cmd/query_files_wasm/workspace-panel.bench.cjs
node cmd/query_files_wasm/sql-mutation.bench.cjs
```

The JavaScript benchmarks measure local sort, result-cell formatting, XLSX
preparation, editor highlighting and positioning, workspace panel refreshes,
and SQL mutation detection without browser rendering or XLSX serialization.
Pass a prior `app.js` path as an argument to compare versions. To run the Go
filter, sorting, row-conversion, streaming and SQL splitting benchmarks in
Node's WASM runtime:

```bash
cd cmd/query_files_wasm
GOOS=js GOARCH=wasm go test -tags no_http \
  -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" \
  -run '^$' -bench '^Benchmark(ResultFirstPage|BuildResultRowIndexesFilteredSort|ResultSortColumnKeys|ResultFilterUppercaseColumns|ResultViewCaseInsensitiveFilter|ResultConversion|StreamChunkEmission|SplitStatements)$' -benchmem
```

`BenchmarkResultFirstPage` measures paging an already materialized result,
excluding SQL execution, JavaScript conversion, and rendering. Sizes above
10,000 rows are scaling controls beyond the default browser preview limit.

The shared CSV importer can also be measured as a complete WASM import from
the repository root:

```bash
GOOS=js GOARCH=wasm go test -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" \
  ./internal/importer -run '^$' -bench '^BenchmarkFuzzyImportCSV$' -benchmem
```

### Browser integration tests

Install the pinned Playwright dependencies and Chromium once (Node.js 20+ and
Go are required):

```bash
cd cmd/query_files_wasm
npm ci
npx playwright install chromium
cd ../..
make test-query-files-browser
```

On Linux, use `npx playwright install --with-deps chromium` to also install
Chromium's system libraries. CI runs these tests in its own job and uploads
the HTML report, failure screenshots and traces when a test fails.

The test server builds the `no_http` WASM module and matching Go runtime into
a temporary directory, serves the real application on `127.0.0.1:4173`, and
removes those assets when it stops. Set `TINYSQL_BROWSER_PORT` to use a different
port. Each test gets an isolated browser context; reloads within a test retain
its actual IndexedDB and OPFS data. Optional CDN libraries are blocked, so the
CSV/JSON import and normal table-view tests do not depend on external services.

The suite covers CSV/JSONL file uploads, autosave and reload, switching between
workspaces with the same table name, recovery from a corrupt generation,
blocked writes after unrecoverable snapshots, and a snapshot above the 8 MiB
OPFS threshold. It uses the storage API only to inspect saved generations and
seed corrupt input; imports, queries and workspace changes go through the UI
and the real WASM worker. Assertions wait for visible results or committed
snapshots rather than sleeping for a fixed autosave delay.

For debugging, run `npm run test:browser -- --headed` in this directory or
`npx playwright show-report` after a run.

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
