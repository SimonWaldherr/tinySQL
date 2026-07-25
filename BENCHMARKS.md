# Benchmarks: tinySQL vs. SQLite (modernc)

Head-to-head benchmarks between tinySQL's storage backends and
[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a pure-Go SQLite
driver already a direct dependency of this module — no CGo, so a fair comparison
target for a pure-Go embedded database.

Two suites, with different fairness properties:

- [`benchmarks/storage_benchmark_test.go`](benchmarks/storage_benchmark_test.go)
  and [`benchmarks/query_benchmark_test.go`](benchmarks/query_benchmark_test.go)
  drive tinySQL directly through `tinysql.ParseSQL`/`tinysql.Execute` and SQLite
  through `database/sql`. Right shape for comparing storage backends, but it
  skips the driver, placeholder-binding and row-scanning path the SQLite side
  pays.
- [`benchmarks/sqlite_parity_benchmark_test.go`](benchmarks/sqlite_parity_benchmark_test.go)
  puts *both* engines behind `database/sql` with bound parameters, one
  connection each, on matched schemas and matched durability. This is the suite
  that answers "can I replace SQLite with this?".

```sh
go test -run=none -bench=. -benchtime=100x ./benchmarks/...
```

## Machine / environment for the numbers below

- Intel Core i7-10850H @ 2.70GHz, Windows, `GOMAXPROCS` default (12),
  6 physical / 12 logical cores
- `go test -bench=. -benchtime=50x ./benchmarks/...`
- Microbenchmark ns/op from single runs on a shared dev machine. Treat trends
  (which backend is faster, by roughly what factor) as meaningful and exact
  ns/op values as approximate.

## Suite 1: storage backends (direct `Execute` vs. `database/sql`)

| Name | What it is |
|---|---|
| `tinySQL-Memory` | `ModeMemory`, no backing file |
| `tinySQL-Disk` | `ModeDisk`, one GOB file per table |
| `tinySQL-DiskGzip` | `ModeDisk` + `CompressFiles: true` |
| `tinySQL-Hybrid` | `ModeHybrid` (in-memory cache + disk-backed) |
| `tinySQL-Index` | `ModeIndex` |
| `tinySQL-Page` | Direct `pager.PageBackend` (B+Tree page store), bypassing SQL execution |
| `SQLite-modernc` | `database/sql` + `modernc.org/sqlite`, `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL` |

All tinySQL backends except `tinySQL-Page` go through the full
parse → plan → execute pipeline, so both sides pay their respective "real API"
overhead rather than a stripped-down internal fast path.

### BulkInsert — write N rows into a fresh table, one `INSERT` per row

| Backend | rows=10 | rows=100 | rows=1000 |
|---|---|---|---|
| tinySQL-Memory | 136 µs | 496 µs | 4.36 ms |
| tinySQL-Disk | 342 µs | 686 µs | 4.69 ms |
| tinySQL-DiskGzip | 355 µs | 666 µs | 5.09 ms |
| tinySQL-Hybrid | 334 µs | 733 µs | 5.12 ms |
| tinySQL-Index | 344 µs | 778 µs | 4.39 ms |
| tinySQL-Page | 2.65 ms | 8.97 ms | 69.8 ms |
| **SQLite-modernc** | **426 µs** | **765 µs** | **3.05 ms** |

tinySQL-Memory wins at 10 and 100 rows (SQLite pays fixed WAL/journal setup per
table-write cycle); SQLite pulls ahead at 1000 rows via its B-tree insert path.
`tinySQL-Page` is an outlier: each `SaveTable` call rewrites the whole table
instead of appending, so it does **not** represent an indexed insert path.

### FullScan — read all N rows back with `SELECT *`

| Backend | rows=10 | rows=100 | rows=1000 |
|---|---|---|---|
| tinySQL-Memory | 52 µs | 148 µs | 768 µs |
| tinySQL-Disk | 117 µs | 201 µs | 830 µs |
| tinySQL-DiskGzip | 129 µs | 209 µs | 859 µs |
| tinySQL-Hybrid | 137 µs | 172 µs | 654 µs |
| tinySQL-Index | 150 µs | 180 µs | 687 µs |
| tinySQL-Page | 84 µs | 130 µs | 444 µs |
| **SQLite-modernc** | 113 µs | 215 µs | 1.06 ms |

tinySQL wins scans across the board, by a wider margin as row count grows (2.4x
at 1000 rows, Memory vs. SQLite): tinySQL rows are native Go `[]any` slices with
no read-side marshaling, vs. SQLite's `Scan()` per column per row.

### RoundTrip, SingleInsert, MixedWorkload — ns/op

RoundTrip writes 100 rows then reads them all back per iteration; SingleInsert
does one `INSERT` per iteration (latency-sensitive path); MixedWorkload
interleaves a 10-row write with a full read.

| Backend | RoundTrip | SingleInsert | MixedWorkload |
|---|---|---|---|
| tinySQL-Memory | 466 µs | 74 µs | 115 µs |
| tinySQL-Disk | 689 µs | 186 µs | 224 µs |
| tinySQL-DiskGzip | 782 µs | 167 µs | 252 µs |
| tinySQL-Hybrid | 686 µs | 172 µs | 240 µs |
| tinySQL-Index | 952 µs | 166 µs | 235 µs |
| tinySQL-Page | 9.99 ms | 1.60 ms | 4.33 ms |
| **SQLite-modernc** | 906 µs | 339 µs | 483 µs |

Across RoundTrip, SingleInsert and MixedWorkload, tinySQL-Memory is 2x–4.6x
faster than SQLite, and the disk-backed tinySQL modes ~2x faster on the
per-statement paths (roughly on par for RoundTrip), because those backends
batch/checkpoint instead of fsyncing per statement.

### PointQuery — `SELECT name FROM t WHERE id = 500` on a 1000-row table

| Backend | ns/op | allocs/op |
|---|---|---|
| tinySQL-Memory | 123 µs | 61 |
| tinySQL-Disk | 211 µs | 63 |
| tinySQL-Page | 126 µs | 142 |
| **SQLite-modernc** | 140 µs | 20 |

**Caveat — not apples-to-apples.** SQLite's `t` has an `INTEGER PRIMARY KEY`
(a real B-tree index), so its point query is an O(log n) index seek. On the
tinySQL side a declared `PRIMARY KEY` does not create an index and this
benchmark issues no `CREATE INDEX`, so every tinySQL "point query" here is a
full linear scan with a predicate filter. tinySQL stays competitive at 1000 rows
only because a 1000-row linear scan is cheap in Go; the comparison will invert
at tens of thousands of rows. See "Next steps".

### Join — `SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id`

Data loaded once per sub-benchmark; only the join query is timed.
`customers=N,orders=M` means N rows in `customers`, M in `orders`.

| Backend | 10 cust / 50 orders | 50 cust / 500 orders | 100 cust / 2000 orders |
|---|---|---|---|
| tinySQL-Memory | 192 µs | 501 µs | 1.30 ms |
| tinySQL-Disk | 392 µs | 762 µs | 1.58 ms |
| **SQLite-modernc** | 246 µs | 695 µs | 2.18 ms |

tinySQL wins at every size and the gap widens (~1.7x at 2000 rows). Neither side
has an index on `orders.customer_id`, so both do a nested-loop join; tinySQL's
per-row cost stays low because it joins native Go values.

### Aggregate — `SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id`

| Backend | 10 cust / 50 orders | 50 cust / 500 orders | 100 cust / 2000 orders |
|---|---|---|---|
| tinySQL-Memory | 123 µs | 218 µs | 430 µs |
| tinySQL-Disk | 224 µs | 310 µs | 660 µs |
| **SQLite-modernc** | 200 µs | 522 µs | 1.60 ms |

`executeSimpleAggregateFastPath` (`internal/engine/exec.go`) accumulates
`SUM`/`AVG`/`MIN`/`MAX` directly off the raw `[]any` row during the single
group-by scan, as `COUNT` always did. Before that, those aggregates went through
the general row-map evaluator, which re-scans each group's buffered `Row` slice
once per aggregate expression: at 2000 orders tinySQL-Memory was
1.86 ms / 950 KB / 7377 allocs and lost to SQLite's 1.73 ms; it is now
430 µs / 85 KB / 912 allocs (~4.3x faster, ~11x less memory, ~8x fewer
allocations) and wins at every size, by ~3.7x at the largest. `SUM`/`AVG` still
fall back to `big.Rat` accumulation when they meet a `DECIMAL`/`MONEY` value, so
exact-decimal correctness is unchanged — only the all-numeric case got faster.

## Suite 2: parity — both engines through `database/sql`, durability matched

| Configuration | Durability |
|---|---|
| `tinySQL/mem` | none (in-memory) |
| `SQLite/mem` | none (`:memory:`) |
| `tinySQL/wal` | `ModeWAL`, log fsynced on every committed statement |
| `SQLite/wal-full` | `journal_mode=WAL`, `synchronous=FULL` — the honest counterpart to `tinySQL/wal` |
| `SQLite/wal-norm` | `journal_mode=WAL`, `synchronous=NORMAL` — what most applications actually run |

```sh
go test ./benchmarks/ -run='^$' -bench=Parity -benchtime=100x -count=2
```

Table `bench(id PK, name, score, bucket)` with 10,000 rows and an index on
`bucket`; ns/op, same machine as above, two runs shown as a range:

| Workload | tinySQL/mem | SQLite/mem | tinySQL/wal | SQLite/wal-full | SQLite/wal-norm |
|---|---|---|---|---|---|
| INSERT, autocommit (1 row) | **12–21 µs** | 13–15 µs | 1.19–1.21 ms | 1.18–1.22 ms | 0.09 ms |
| INSERT, 100 rows in one tx | 10.0–11.8 ms | **2.6–3.7 ms** | 8.2–12.9 ms | **3.1–3.5 ms** | 1.6–2.2 ms |
| UPDATE by primary key | 0.95–1.06 ms | **17 µs** | 4.2–6.5 ms | **1.25 ms** | 0.14–0.17 ms |
| Point lookup by primary key | 29–42 µs | **25–28 µs** | **30–41 µs** | 68–74 µs | 30–34 µs |
| Range scan (156 of 10,000 rows) | 317–439 µs | **234–260 µs** | 305–336 µs | 257–296 µs | 257–262 µs |
| `GROUP BY` + `COUNT`/`SUM`, 64 groups | **1.31 ms** | 7.0–7.3 ms | **1.34–1.95 ms** | 8.3–17.1 ms | 23–30 ms |
| 2-table JOIN + `GROUP BY` | 31–48 ms | **10.4–33.9 ms** | 48–68 ms | **10.4–12.4 ms** | 8.7–10.6 ms |

Vector k-NN, top-10 over 10,000 unit vectors of dimension 128, in-memory:

| Operation | ns/op |
|---|---|
| `tinySQL` `VEC_SEARCH(..., 'cosine')`, flat index, warmed | **0.44–0.46 ms** |
| `SQLite` + application-side scan (`SELECT` all vectors, rank in Go) | 18.2–19.3 ms |

These two are **not the same operation** and must not be read as "tinySQL's k-NN
beats SQLite's". Plain SQLite has no k-NN; the second row is what an application
without a vector extension has to do, and the ~40x gap is the cost of that
missing capability, not an engine-vs-engine result.

### What the parity numbers say

- **Durable single-statement writes are at parity.** `tinySQL/wal` and
  `SQLite/wal-full` land within a few percent of each other, both bounded by one
  fsync per statement rather than by engine work. `SQLite/wal-norm` is 13x
  faster than either — most SQLite deployments do not run `synchronous=FULL`.
- **Aggregation is tinySQL's strongest result** — 5x faster than SQLite in
  memory, 6–17x faster on the durable configuration.
- **UPDATE is the largest remaining gap.** Logging only the rows an UPDATE
  actually replaced, instead of the whole table, took `tinySQL/wal` from
  31–48 ms to 4.2–6.5 ms; what remains is engine-side, and in-memory UPDATE is
  still ~60x slower than SQLite. Two measured causes: `WHERE id = ?` scans every
  row instead of seeking the primary key, and the statement's rollback point
  deep-copies every cell of the table.
- **JOIN is 3–5x slower** than SQLite — the second gap worth closing.
- **Batched inserts are ~3x slower**: a transaction commit re-serializes each
  changed table rather than appending row deltas.

## Takeaways

- tinySQL wins read-heavy and low-latency-write workloads in-memory or with its
  lightweight disk backends — full scans, single inserts, joins, aggregates,
  mixed workloads — at the row counts tested (10–2000). That matches its design:
  embedded, allocation-light, single-process, without SQLite's
  transactional-durability machinery in the hot path.
- SQLite pulls ahead on large bulk inserts (1000+ rows in one loop), indexed
  point lookups, batched-transaction inserts and JOINs.
- `tinySQL-Page`, the direct B+Tree backend, is the slowest option for bulk
  writes: no incremental/append writes at the page level yet, so every
  `SaveTable` serializes the whole table. A known optimization target, not the
  page format's ceiling.
- `PointQuery` is currently unfair to SQLite in tinySQL's favor (index seek vs.
  full scan); an indexed point-query benchmark is the natural addition.

## Next steps

1. Close the UPDATE gap measured by the parity suite, in profile order:
   a. Seek the primary key for `WHERE pk = ?` in UPDATE/DELETE instead of
      scanning every row.
   b. Replace the whole-table rollback clone for an in-place UPDATE with an undo
      journal of the rows it replaced. A single-row UPDATE deep-copies every
      cell of the table to get a rollback point; that is ~1 ms of the ~1 ms
      in-memory figure. Copying only row headers is *not* a valid shortcut —
      `TestFullStatementSnapshotRestoresAllTablesAndDropsNewTables` documents
      that a snapshot also protects against in-place cell writes.
   c. Append row deltas at COMMIT instead of re-serializing each changed table,
      which is what makes batched inserts ~3x slower than SQLite.
2. Make the tinySQL side of `PointQuery` use an index. `CREATE INDEX` builds a
   real secondary index (`executeCreateIndex`,
   `internal/engine/exec_ddl_table.go`) and the planner seeks it for equality
   point/prefix predicates (`selectSecondaryIndex`, `internal/engine/exec.go`),
   but a declared `PRIMARY KEY` does not create one — so either index the PK
   column implicitly or add an indexed point-query benchmark comparing
   index-seek vs. index-seek.
3. Extend the row-count sweep beyond current sizes (10k/100k rows) to find where
   the bulk-insert crossover with SQLite happens.
4. Extend the raw-row aggregate fast path to multi-column `GROUP BY` (currently
   a single group-by column) and to `HAVING` clauses that only reference
   already-computed aggregates.

## Internal engine optimizations (not SQLite comparisons)

Before/after numbers for engine-internal work with no SQLite side to compare
against — `go test -bench=. -benchmem ./internal/engine/...` on the same machine.

### Vector search: HNSW allocation and build-time fix

Profiling `VEC_SEARCH(..., 'hnsw')`/`VEC_WARM(..., 'hnsw')` on a 12,000-row /
64-dim table (`BenchmarkVecSearchIndexModesSameTable` in
`internal/engine/vector_search_benchmark_test.go`) found two compounding issues
in the HNSW traversal (`internal/engine/vector_index.go`):

1. `searchLayer` allocated two fresh heaps plus a "touched nodes" slice per call
   (one call per graph layer) — tens of thousands of short-lived allocations per
   12k-row index build. Fixed by pooling the heaps and the visited-touch buffer
   across `searchLayer` calls within one traversal (`vecHNSWScratch` +
   `sync.Pool`).
2. Both heaps used `container/heap`'s `Push(h Interface, x any)`/`Pop() any`,
   boxing every `vecScoredRow`: one allocation per graph edge considered,
   dwarfing (1). Replaced with direct, non-interface sift-up/sift-down functions
   on the concrete slice types.

Separately, `vectorDotKernel`/`vectorL2SquaredKernel`/`vectorL1Kernel`
(`internal/engine/vector_math_amd64.go`) fell back to a portable scalar loop
below 128 dimensions. `BenchmarkVectorDotKernelBySize` showed the SSE2 kernel
winning at every size tested, including 16 dimensions, so the threshold went
away.

| Benchmark (12k rows, 64 dims, k=20, cosine) | before | after |
|---|---|---|
| HNSW index build (`VEC_WARM`) | 19.5 s | 7.5 s (2.6x) |
| HNSW query (`BenchmarkVecSearchCosineTopK_HNSWCached`) | 2.26 ms, 4336 allocs/op | ~1.0-1.2 ms, 78 allocs/op |
| IVF query | 382 µs, 133 allocs/op | ~220 µs, 77 allocs/op |
| Flat (exact) query | 970 µs, 415 allocs/op | ~580 µs, 128 allocs/op |

`TestVecSearchWithANNIndexModes` and
`TestVecSearchANNIndexInvalidatesOnTableVersion`
(`internal/engine/vector_test.go`) check HNSW/IVF results against exact search
and pass unchanged.

### Vector search: lock-free hot path and a contiguous column cache

**1. Lock-free hot path.** Every `VEC_SEARCH` call — any table size, any index
mode, even with the result cache and analytics both at their documented default
of *off* — took two `Lock()`/`Unlock()` round trips on one process-wide
`sync.Mutex` (`vecQueryCacheState` in `internal/engine/vector_query_cache.go`):
one for `vecQueryCacheEnabled`, one in `recordVecQuery`, which locked *before*
checking. The search itself is read-only and already parallelized across
workers, so this was pure serialization that worsened with concurrent callers.
Fixed by mirroring the two flags this path needs (`cacheEnabled`,
`analyticsEnabled`) into `atomic.Bool` fields written only by
`ConfigureVectorCache` inside its existing critical section. Any actual mutation
(`getVecQueryCache`, `putVecQueryCache`, `recordVecQuery`'s slow path) still
re-validates against the mutex-protected config, so a stale atomic read costs at
most one wasted lookup — never a wrong result.

A block profile is the clearest evidence: under a concurrent `VEC_SEARCH`
benchmark, `sync.(*Mutex).Lock` accounted for 0.10s of 5.89s of recorded
blocking time before the fix, attributed to
`vecQueryCacheEnabled`/`recordVecQuery`, and disappears afterward. Wall clock
(`BenchmarkVecSearchConcurrent`, `-count=5`, median) separates from noise only
once enough goroutines contend:

| `-cpu=N` (concurrent goroutines) | before (median ns/op) | after (median ns/op) |
|---|---|---|
| 1 | 15.8 µs | 15.5 µs (≈ noise) |
| 4 | 5.3 µs | 5.2 µs (≈ noise) |
| 8 | 5.8 µs | 4.5 µs (≈22% faster) |

With 12 logical cores, `-cpu=8` is the first setting where lock queuing shows up
in wall clock; the block profile is the more reliable signal at any concurrency.

**2. Contiguous column cache.** `VEC_SEARCH`'s per-`(tenant, table, column)`
cache (`vecSearchColumnCacheEntry` in `internal/engine/vector_search.go`) stored
each row's vector as its own heap-allocated `[]float64` — `N` scattered
allocations per `N`-row column, one pointer chase per step of what is otherwise a
sequential scan (flat, IVF and HNSW all walk the cache row by row), defeating
prefetching. Fixed by packing every valid row into one contiguous buffer at
cache-build time (`buildVecColumnCache`), each row's slice becoming a
`cap == len` view into it. Rows keep their true length rather than a fixed
stride, so mixed-length vectors (mid-embedding-migration tables) behave exactly
as before: a row whose length doesn't match the query is excluded at search time,
never truncated. `cache.vectors[i]` keeps its `[][]float64` type, so readers in
`vector_index.go` (IVF, HNSW) and `vector_warm.go` (`VEC_WARM` diagnostics)
needed no changes.

The effect scales with how much of a row's cost is "find the row" versus "read
the row": small for wide embeddings (768 dims = 6 KB/row), clearer for narrow
ones on a large corpus (64 dims = 512 bytes/row, so the fixed per-row overhead
recurs more often per byte scanned):

| Benchmark (`-count=3`, median) | before | after |
|---|---|---|
| 50k rows × 768 dims, `-cpu=1` | 19.9 ms | 20.1 ms (≈ noise) |
| 50k rows × 768 dims, `-cpu=4` | 12.1 ms | 11.5 ms (≈5% faster) |
| 200k rows × 64 dims, `-cpu=1` | 9.9 ms | 9.5 ms (≈5% faster) |
| 200k rows × 64 dims, `-cpu=4` | 5.0 ms | 4.4 ms (≈14% faster) |

Both row counts exceed `vecSearchParallelMinRows` (4096), so the `-cpu=4` numbers
include `VEC_SEARCH`'s pre-existing internal parallel-scan fan-out on both sides
— the lift from `-cpu=1` to `-cpu=4` is *not* purely the memory-layout fix; one
big buffer also contends shared last-level cache and memory bandwidth less than
thousands of scattered ones.

**Trade-off:** for columns holding a native `VECTOR` (`[]float64`) rather than a
JSON string, the old cache *aliased* each row at no extra memory cost; the new
one *copies* into the shared buffer, so a warmed cache holds roughly two live
copies of that column (the original in `table.Rows` plus the packed copy) while
both stay alive. JSON-string vector columns are unaffected — building the cache
already allocated one `[]float64` per row via `json.Unmarshal`. Memory for scan
speed, only for `VECTOR`-typed columns while their search cache is warm.

Correctness: `TestVecWarmMixedDimensionalityReported`,
`TestVecSearchNaNRowExcluded`, `TestVecSearchTopKWorkerPanicRecovered`
(constructs a cache entry literal, bypassing the builder), and the full
`TestVecSearchWithANNIndexModes`/`TestVecSearchANNIndexInvalidatesOnTableVersion`
suite pass unchanged. `TestVecSearchConcurrentWithCacheReconfiguration` (new)
races concurrent `VEC_SEARCH` callers against concurrent `ConfigureVectorCache`
calls as a guard for the lock-free fast path.

```sh
go test -run '^$' -bench 'BenchmarkVecSearchConcurrent|BenchmarkVecSearchFlatScanLargeEmbedding|BenchmarkVecSearchFlatScanManySmallEmbeddings' -benchmem -count=3 -cpu=1,4,8 ./internal/engine/...
```

### Row materialization (`rowsFromTable`): redundant per-row map check

`rowsFromTable` (`internal/engine/exec.go`) builds the `Row` (`map[string]any`)
for every row of every table in a `FROM` clause — the entry point behind scans,
joins, `GROUP BY` and `ORDER BY`. Each column is stored under a qualified key
(`alias.col`) and an unqualified one (`col`); the unqualified insert was guarded
by a map existence check on *every row* so a duplicate column name could not
clobber an earlier one. Real schemas essentially never have duplicates, so the
check computes once per query now instead of once per row: the fast path sets
both keys unconditionally, the slow path preserves the exact "first occurrence
wins" behavior (regression test `TestRowsFromTableDuplicateColumnNames` in
`internal/engine/rows_from_table_test.go`).

| Benchmark (20,000 rows) | before | after |
|---|---|---|
| `SELECT grp, sub, COUNT(*), AVG(val) ... GROUP BY grp, sub` | ~50 ms | ~40 ms (≈20%) |
| `SELECT * FROM t` | ~29 ms | ~27 ms |

```sh
go test -bench='BenchmarkGroupByTwoColumns|BenchmarkSelectStarFullScan' -benchmem ./internal/engine/...
```

### `GROUP BY` composite keys and `ORDER BY ... LIMIT` top-N heaps

`BenchmarkGroupByTwoColumns` (20,000 rows, 2 group-by columns, 50 distinct
groups) put 92% of all allocations in `writeFmtKeyPart`, the composite-key helper
used by every `GROUP BY` (`executeSimpleMultiGroupAggregate`,
`processAggregateQuery`), `PIVOT` (`processPivot`) and `DISTINCT`
(`distinctRows`) path in `internal/engine/exec.go`. Each built the key into a
`strings.Builder` reset per row via `keyBuf.Reset()` — which sets the internal
buffer to `nil`, discarding capacity — so every row allocated a fresh backing
array and then materialized the key as a `string` map key even when the group
already existed. Two fixes:

1. `writeFmtKeyPart` appends to a reused `[]byte` (`buf = buf[:0]` keeps the
   backing array), the standard `strconv.AppendInt`-style idiom, instead of
   writing through a `*strings.Builder`.
2. Group lookups use `groups[string(keyBuf)]` — the compiler elides the
   `[]byte`→`string` allocation when a map index expression is only read — and an
   owned string is materialized only for a row starting a **new** group.
   `processAggregateQuery`'s group map changed from `map[string][]Row` to
   `map[string]*[]Row` so appending to an existing group mutates through the held
   pointer instead of writing the map again (verified empirically that
   `m[string(b)] = v` on an existing key still allocates — reading is free,
   writing never is).

Per-row key allocation becomes per-*distinct-group* allocation; `PIVOT` and
`DISTINCT` get the same fix. Separately, `ORDER BY ... LIMIT N`'s top-N heap
(both the simple-select raw fast path and the general row-map path used after
`GROUP BY`) went through `container/heap`'s `Push`/`Fix`, boxing every candidate
row into an `any` — up to `N` allocations to fill the heap. Replaced with the
same direct sift-up/sift-down functions used for
`vecScoredHeap`/`vecMinScoredHeap`.

| Benchmark (20,000 rows, `-count=3`, median) | before | after |
|---|---|---|
| `... GROUP BY grp, sub` (2-col key, 50 groups) | 63,191 allocs, 1.40 MB, ~5.3 ms | 3,541 allocs, 0.29 MB, ~2.2 ms (**~18x fewer allocs, ~2.4x faster**) |
| `... GROUP BY grp HAVING COUNT(*) > 100` | 120,947 allocs, 15.4 MB | 80,947 allocs, 14.6 MB (allocs **-33%**; wall-clock unchanged) |
| `... GROUP BY grp ORDER BY a DESC LIMIT 10` | 120,864 allocs, 15.4 MB, ~21.9 ms | 80,853 allocs, 14.6 MB, ~20.9 ms (allocs **-33%**) |
| `... ORDER BY val DESC LIMIT 20` (raw fast path, no `GROUP BY`) | 70 allocs, 11.5 KB, ~5.5 ms | 49 allocs, 10.2 KB, ~4.2 ms (**-30% allocs, ~24% faster**) |
| `... ORDER BY val DESC` / `... ORDER BY grp, sub, val DESC` (no `LIMIT`) | unchanged | unchanged (no heap without `LIMIT`) |
| `GROUP BY grp` (single column, no `HAVING`) | unchanged | unchanged (already used a simpler key path) |

Allocation count and wall-clock time are different metrics here: the `HAVING` and
`GROUP BY + ORDER BY + LIMIT` queries lose a third of their allocations but their
latency is dominated by aggregate evaluation across `big.Rat`/`HAVING`
re-checks. Allocation count now scales with **distinct groups** instead of
**rows**, so the gap widens on tables with many rows and few groups.

Correctness: verified byte-for-byte identical output against the pre-optimization
implementation for `GROUP BY` key building, the `ORDER BY ... LIMIT` top-N heap,
and the whole-table-aggregate "one synthesized row over zero matching rows" edge
case forced through the general path via a `JOIN`. A heap-based top-N does not
guarantee insertion-order stability for ties on the sort column *alone* —
unchanged by this fix either way; add a secondary `ORDER BY` column for a fully
determined order, same as any SQL engine. `TestGroupByHaving`, `TestPivot*`,
`TestAggregateFastPath*`, `TestSelectOrderByLimitOffsetFastPath`,
`TestSelectStarFastPath*` and the whole `internal/engine` package pass unchanged.

```sh
go test -bench='BenchmarkGroupByTwoColumns|BenchmarkGroupByWithHaving|BenchmarkGroupByOrderByLimit|BenchmarkOrderByWithLimit' -benchmem -count=3 ./internal/engine/...
```

### RAG scalar-function path: constant folding, fused SIMD cosine, AVX2+FMA

The *scalar* RAG query shape — per-row
`VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[...]'))` in `WHERE`/`ORDER BY`,
optionally blended via `RAG_HYBRID_SCORE`/`RAG_RANK_SCORE` — was orders of
magnitude slower than the equivalent `VEC_SEARCH` call. On a 12,000-row / 64-dim
`ORDER BY sim DESC LIMIT 20` query, ~85% of CPU time sat in `encoding/json`: the
`VEC_FROM_JSON` literal was re-parsed per row. Five compounding fixes
(`internal/engine/const_fold.go`, `vector_functions.go`, `vector_math*.go/.s`,
`exec.go`):

1. **Parse-time constant folding** — `VEC_FROM_JSON`/`VEC_FROM_BYTES`/
   `VEC_NORMALIZE` calls with all-literal arguments are evaluated once at parse
   time and replaced with a vector literal (`foldConstFuncCall`). Invalid input
   stays unfolded so errors still surface at execution.
2. **Scalar vector functions use the SIMD kernels** — `VEC_DOT`,
   `VEC_L2_DISTANCE`, `VEC_MANHATTAN_DISTANCE`, `VEC_DISTANCE`, `VEC_NORM`,
   `VEC_NORMALIZE` and `cosineSimilarity` previously used naive Go loops.
3. **Fused cosine kernel** — cosine without cached norms needs dot(a,b),
   dot(a,a) and dot(b,b); `vectorCosineKernel` computes all three with the memory
   traffic of a single dot product.
4. **AVX2+FMA kernels with runtime dispatch** — 4-wide `VFMADD231PD` variants of
   the dot/L2/L1/cosine kernels, selected at startup via an in-repo CPUID check
   (no new dependency); SSE2 remains the fallback and the floor for short
   vectors.
5. **Raw fast path allocation removal** — `evalRawFuncCall` allocated an args
   slice, per-arg `Literal` wrappers, an escaping `FuncCall` copy and an empty
   `Row` map per row; these are now pooled/shared. Timestamp parsing
   (`RECENCY_SCORE`, `RAG_HYBRID_SCORE`) gained a fixed-layout fast path
   (`parseTimeFixedDigits`), ~15x cheaper than `time.Parse`.

| Benchmark (12k rows, 64 dims) | before | after |
|---|---|---|
| `ORDER BY VEC_COSINE_SIMILARITY(...) LIMIT 20` | 271 ms, 34 MB, 264k allocs | 2.7 ms, 109 KB, 12k allocs (**~100x**) |
| `RAG_RANK_SCORE(...) ORDER BY ... LIMIT 20` | 282 ms | 7.6 ms (**37x**) |
| Hybrid score + recency (`RAG_HYBRID_SCORE`) | 535 ms | 22.6 ms (**24x**) |
| `WHERE vector-cond AND scalar-cond` | ~29 ms | ~1.5 ms (**19x**) |
| `vectorDot`, 768 dims (AVX2+FMA vs SSE2) | 184 ns | 73 ns (2.5x) |
| `vectorL2Squared`, 768 dims | 227 ns | 83 ns (2.7x) |

Kernel parity across sizes (including AVX2 dispatch thresholds and odd tails) is
covered by `TestVecDotKernelMatchesUnrolledAcrossSizes` and
`TestVecCosineKernelMatchesUnrolledAcrossSizes`; folding semantics by
`TestVecFromJSONConstantFolding`/`TestVecFromJSONInvalidStillErrors`.

```sh
go test -bench='BenchmarkOrderByVectorLimit|BenchmarkRAGRankScore|BenchmarkHybridOrderBy|BenchmarkVectorDot768' -benchmem ./internal/engine/...
```

### Production hardening: raw-filter fallback, cache eviction, statement cache

Follow-up to the RAG scalar-function work, targeting long-running processes.
Wall-clock numbers carry thermal variance; the allocation numbers are the stable
signal.

1. **Function predicates no longer force the Row-map evaluator**
   (`internal/engine/exec.go`). WHERE clauses built from function-call
   comparisons — `VEC_COSINE_SIMILARITY(embedding, ...) > 0.5 AND
   RECENCY_SCORE(created_at, 30) > 0.2` — compiled to no raw filter at all,
   disqualifying the plan from the raw fast path; the general evaluator then
   built a `map[string]any` with two entries per column for every row, and that
   map traffic dominated. Now any predicate the specialized builders can't
   compile falls back to a raw `evalRawExpr` filter (`buildRawExprFilter`),
   keeping the scan on `[]any` rows. AND/OR keeps its cost model: a specialized
   side (column comparison) short-circuits before an expression side (vector
   distance), regardless of written order.
2. **Function names normalize to uppercase at parse time**
   (`internal/engine/parser.go`). Handler resolution tried an exact map hit then
   retried with `strings.ToUpper` — for lowercase-written SQL, an extra lookup
   plus a string allocation per call per row.
3. **Vector caches are bounded and purged on DROP TABLE**
   (`internal/engine/vector_search.go`, `vector_index.go`, `exec.go`). The column
   cache and the IVF/HNSW index caches each pin their `*storage.Table`, including
   all row data. Entries for dropped tables were never evicted, so a
   create/query/drop cycle leaked the entire table per iteration for the life of
   the process. DROP TABLE now purges all three caches eagerly
   (`purgeVectorCachesFor`), and hard caps (256 column caches, 64 per index kind)
   bound paths with no purge hook (renames, tenant removal). Regression test:
   `TestDropTablePurgesVectorCaches`.
4. **The `database/sql` driver caches parsed SELECT/EXPLAIN statements**
   (`internal/driver/driver.go`). Applications re-issue the same statement text
   on every call, and each call paid a full lex+parse. A bounded, process-wide
   cache (256 entries, statements ≤ 8 KB) returns the shared AST for repeated
   text — parsed statements are already safely re-executable (the public
   ParseSQL-once/Execute-many pattern). Oversized or unique statements (bulk
   INSERTs, inlined vector literals) are parsed directly and never stored.

| Benchmark (12k rows, 64 dims) | before | after |
|---|---|---|
| Hybrid score + recency (`RAG_HYBRID_SCORE`) | ~23 ms, 13.2 MB, 144k allocs | ~19 ms, **494 KB, 60k allocs** |
| `WHERE vector-cond AND scalar-cond` (vector written first) | 4.2 ms* | 2.2 ms (order-independent) |
| Repeated SELECT via `database/sql` | 137 allocs/query | 87 allocs/query (parse skipped) |

*The 4.2 ms "before" is what the naive fallback (evaluate both predicate sides in
written order) would cost; the shipped version restores cheap-side-first
ordering, so predicate order in SQL text no longer matters.

```sh
go test -bench='BenchmarkHybridOrderBy|BenchmarkWhereVectorAndSimpleCondition' -benchmem ./internal/engine/...
go test -bench='BenchmarkRepeatedSelectViaDriver' -benchmem ./internal/driver/...
```
