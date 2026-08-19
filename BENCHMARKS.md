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

The published-artifact comparison is in
[`benchmarks/mbtiles_artifact_benchmark_test.go`](benchmarks/mbtiles_artifact_benchmark_test.go).
It compares the bounded `dataset.tinysql` import with a SQLite MBTiles copy and
reports import time, artifact/database bytes and validated open+point time. Use
`-benchtime=1x` for that lifecycle benchmark; the access benchmark is suitable
for repeated latency samples and reports p50/p95/p99.

```sh
go test -run=none -bench=. -benchtime=100x ./benchmarks/...
```

## What "SQLite" means in this file — and what it does not

Every `SQLite/…` row here is [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a machine-translated pure-Go port. **It is not the C SQLite** that a typical
application ships, whether as the system library or a bundled `libsqlite3`.

That choice is deliberate and it is the right like-for-like target for this
project: tinySQL is pure Go, builds under TinyGo and WebAssembly, and needs no
CGo, so the honest question it answers is *"can this replace the pure-Go SQLite I
am already using?"*

It does **not** answer *"is this as fast as SQLite?"* The C implementation is
generally faster than the Go port, and none of these measurements bound that gap:
the development machine used for them has `CGO_ENABLED=0`, so a CGo-based driver
cannot even be built here, let alone measured. Anyone weighing tinySQL against C
SQLite — for example when the alternative is the platform's own SQLite — should
measure that pairing themselves before relying on any factor stated below.

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

## Suite 2: parity — both engines through `database/sql`, durability tiered

| Configuration | Durability |
|---|---|
| `tinySQL/mem` | none (in-memory) |
| `SQLite/mem` | none (`:memory:`) |
| `tinySQL/wal-fsync` | `ModeWAL`, `wal_sync=normal`: ordinary fsync on every committed statement |
| `SQLite/wal-fsync` | `journal_mode=WAL`, `synchronous=FULL`, `fullfsync=OFF`: ordinary fsync on every commit |
| `tinySQL/wal-fullflush` | `ModeWAL`, default `wal_sync=full`: strongest available OS flush on every committed statement (`F_FULLFSYNC` on macOS) |
| `SQLite/wal-fullflush` | `journal_mode=WAL`, `synchronous=FULL`, `fullfsync=ON` (and `checkpoint_fullfsync=ON`) |
| `SQLite/wal-normal` | `journal_mode=WAL`, `synchronous=NORMAL`: deliberately weaker durability reference |

`wal_sync=normal` does **not** mean SQLite `synchronous=NORMAL`: tinySQL still
flushes every acknowledged commit. It is the peer of SQLite
`synchronous=FULL` without `fullfsync`. The parity benchmark disables automatic
checkpoints during its timed commit loops so checkpoint work is not charged to
an arbitrary operation; checkpoint latency should be benchmarked separately.

```sh
go test ./benchmarks/ -run='^$' -bench=Parity -benchtime=100x -count=2
```

### Historical results before the durability-tier split

Table `bench(id PK, name, score, bucket)` with 10,000 rows and an index on
`bucket`; ns/op, same machine as above, two runs shown as a range. These
historical values use tinySQL's old/default full-flush path alongside SQLite
`synchronous=FULL` without `fullfsync`; on macOS those are **not** matched
durability tiers and must not be read as a durable-write-parity claim. Re-run
the current benchmark matrix for a tier-matched result on the target host.

| Workload | tinySQL/mem | SQLite/mem | tinySQL/wal-fullflush | SQLite/wal-fsync | SQLite/wal-normal |
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

- **Do not infer durable-write parity from the historical WAL columns.** On
  macOS tinySQL's default full flush is stronger than SQLite
  `synchronous=FULL` without `fullfsync`; use the current `wal-fsync` or
  `wal-fullflush` matched tier instead. `SQLite/wal-normal` remains a
  deliberately weaker throughput reference.
- **Aggregation is tinySQL's strongest result** — 5x faster than SQLite in
  memory, 6–17x faster on the durable configuration.
- **Primary-key UPDATE no longer scans or snapshots the whole table.** The
  constraint index bounds both execution and the atomic rollback point to the
  candidate row. Trigger/FK updates and assignments touching a secondary index
  retain the conservative full snapshot.
- **Simple two-table equality JOINs are now a tinySQL strength.** The raw
  hash-join path caches its compiled shape and the right-hand lookup until that
  table changes. In the current `BenchmarkParityJoin` it takes about 0.45 ms
  versus SQLite's 3.0 ms on this machine. More complex join shapes still use
  the generic executor and remain the next JOIN-performance target.
- **Batched inserts are ~3x slower**: a transaction commit re-serializes each
  changed table rather than appending row deltas.

## Takeaways

- tinySQL wins read-heavy and low-latency-write workloads in-memory or with its
  lightweight disk backends — full scans, single inserts, joins, aggregates,
  mixed workloads — at the row counts tested (10–2000). That matches its design:
  embedded, allocation-light, single-process, without SQLite's
  transactional-durability machinery in the hot path.
- SQLite pulls ahead on large bulk inserts (1000+ rows in one loop), indexed
  point lookups and batched-transaction inserts. Complex or multi-way JOINs
  that cannot use tinySQL's raw equality-join path need separate measurement.
- `tinySQL-Page`, the direct B+Tree backend, is the slowest option for bulk
  writes: no incremental/append writes at the page level yet, so every
  `SaveTable` serializes the whole table. A known optimization target, not the
  page format's ceiling.
- Primary-key and UNIQUE point lookups reuse the in-memory constraint index;
  secondary-index comparisons should still create equivalent indexes on both
  engines.

### Primary-key UPDATE fast path

`BenchmarkParityUpdateByPK/tinySQL/mem`, 10,000 rows, 100 point updates per
sample, five samples on Apple M2 Max:

| metric (median) | before | after | change |
|---|---:|---:|---:|
| latency | 304,049 ns/op | 7,657 ns/op | **39.7x faster** |
| allocated bytes | 984,592 B/op | 4,607 B/op | **99.5% less** |
| allocations | 220 allocs/op | 96 allocs/op | **56% less** |

The execution plan seeks the existing PRIMARY KEY/UNIQUE constraint hash and
still evaluates the complete predicate on the candidate, so residual `AND`
terms preserve SQL semantics. Statement atomicity uses a row-local snapshot
that deep-copies mutable BLOB cells and restores table statistics and WAL dirty
metadata on failure.

### Primary-key DELETE fast path

`BenchmarkDeleteByPrimaryKey`, 20,000 rows, one successful point delete per
sample, ten samples on Apple M2 Max:

| constraint cache | latency (median) | allocated bytes | vs. previous 1.51 ms |
|---|---:|---:|---:|
| warm | 24.4 µs | 2,944 B/op | **61.8x faster** |
| cold | 89.3 µs | 2,880 B/op | **16.9x faster** |

Before the change, DELETE allocated about 2.53 MB/op for a full rollback clone,
survivor slice, and row-position map. The point path now saves one rollback row,
evaluates the complete predicate only on the constraint candidate, shifts the
remaining row headers in place, and adjusts secondary-index RowIDs without an
O(n) map. A cold constraint cache deliberately scans just the constrained
column rather than building a hash table that the successful delete would
immediately invalidate.

## Next steps

1. Append row deltas at transaction COMMIT instead of re-serializing each
   changed table, which is what makes batched inserts ~3x slower than SQLite.
2. Extend the row-count sweep beyond current sizes (10k/100k rows) to find where
   the bulk-insert crossover with SQLite happens.
3. Extend the raw-row aggregate fast path to multi-column `GROUP BY` (currently
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

### Full-text search: an inverted index for the RAG lexical branch

`docs/rag-guide.md` recommends `HYBRID_SEARCH` as the default RAG retriever: a
vector pass and a BM25 pass, fused with RRF. Nothing measured that combination
end to end, so `BenchmarkRAGHybridSearch` and friends were added in
`internal/engine/rag_workload_benchmark_test.go`.

The fixture matters. The pre-existing FTS benchmark gave every document the
identical body text, so every document matched every query and BM25 selectivity
was unobservable. The new corpus is Zipf-distributed (20,000 chunks x 120 tokens,
4,000-term vocabulary) with a rare-identifier family present in 3 documents
each — the exact-match case hybrid retrieval exists to serve.

Measured on that corpus, the **lexical branch was ~98% of hybrid retrieval cost**
(43.9 ms of 44.8 ms; the vector branch was 4.8 ms). `FTS_SEARCH` scored every
document in the corpus on every query, so a term in 3 of 20,000 chunks cost the
same as one in all of them. A CPU profile put the time in string-keyed map
operations (`mapaccess1_faststr` 21%, `ctrlGroup.matchH2` 11%, `memeqbody` 9%)
plus `maps.Iter.Next` at 12% — the latter being per-document iteration of each
document's frequency map to expand a wildcard.

Two structures in the existing tokenized-document cache fix both, invalidated by
`table.Version` exactly like the cache itself (`internal/engine/fts_index.go`):

1. **Postings** — term to ascending row indices. A query's candidate set is
   derived from them, so documents that cannot match are never scored. The map
   replaces the former `docFreq map[string]int`: a term's document frequency is
   its postings length.
2. **A term dictionary** (the postings key set) — a wildcard resolves against it
   once per query. This corpus has 4,167 unique terms against 2,400,500 token
   instances, so a wildcard does 4,167 comparisons instead of 2.4 million.

Restricting candidates cannot change results: `ftsScoredLess` is a total order on
(score desc, rowIdx asc) and `ftsPushTopK` admits candidates only under that same
order, so the top-k is a function of the candidate *set*, not of iteration order.
Candidates are still verified with `ftsMatchNode` and scored with `ftsScoreNode`.
`NOT` yields no restriction — a document containing none of the query's terms can
satisfy a negation — so `a OR NOT b` correctly falls back to a full scan while
`a AND NOT b` still restricts to `a`.

A candidate set covering more than half the corpus is not materialized: it would
save no scoring work and cost an allocation a plain scan does not. That keeps the
fallback paths allocation-identical to before (verified in the table below).

| Benchmark (20k chunks, 96 dims) | before | after |
|---|---|---|
| Selective term (3 of 20,000 docs) | 979 µs | **37 µs (26x)** |
| Prefix wildcard | 62.8 ms | **310 µs (202x)** |
| Quoted phrase | 26.9 ms | 23.1 ms (1.16x) |
| Four-term OR (all common terms) | 13.09 ms, 98,712 B/op | 13.53 ms, 98,712 B/op |
| Single corpus-wide term | 6.48 ms, 40,808 B/op | 6.90 ms, 40,808 B/op |
| `HYBRID_SEARCH` end to end | 14.4 ms | 15.3 ms |
| Cold cache rebuild | 884 ms, **231 MB** | **707 ms**, **192 MB** |

Read honestly: the index pays off where query terms are selective and costs 3-6%
where they are not. The "four-term OR" and "corpus-wide term" rows use terms
present in most of the corpus, where no candidate strategy can help — they are
the control, and their byte-identical allocation counts show the fallback itself
adds no work, so that 3-6% is the cost of consulting the postings map and
resolving the query against it. `HYBRID_SEARCH` pays the same overhead because
its lexical query here is four common terms; a question containing any selective
term takes the fast path instead.

Every pair was run alternately from binaries built from the same tree, best-of-N,
because this machine's run-to-run variance otherwise **exceeds the effects being
measured** — an unchanged binary produced 951 ms, 1234 ms, 1733 ms and 3411 ms for
the same cold-cache benchmark on four consecutive runs. Two conclusions drawn from
single runs during this work turned out to be noise, so the alternating-binary
comparison is not optional here.

The memory result is better than the index alone would suggest, because
profiling the build turned up an unrelated sizing bug next to it. Each document's
term-frequency map was created with `make(map[string]int, len(tokens))` — sized by
*token* count, though it only ever holds *distinct* terms. Prose repeats words, so
that hint overshoots by enough to push the map's internal group count past a
power-of-two boundary and allocate a full extra doubling that is never used, once
per document, retained for the life of the cache. Sizing by an estimate of the
distinct count instead cut 61 MB, more than paying for the 23 MB of postings: the
corpus now costs **17% less memory than before the index was added**.

This also fixed a latent bug. `ftsScoreNode` summed a `float64` per matching
token while ranging over a Go map; map iteration order is randomized and float
addition is not associative, so identical wildcard queries against an unchanged
corpus returned scores differing in their final bits (`term1*` gave
`142.8432967294423`, then `...4227`, then `...4233`). Summing over a sorted term
list makes them reproducible. `TestFTSGoldenQueryGrammar` pins this across 20
query shapes, and `TestFTSCandidateRestrictionMatchesFullScan` checks every shape
against a full-scan reference implementation.

```sh
go test -bench='BenchmarkRAG' -benchmem ./internal/engine/...
go test -run='TestFTSGolden|TestFTSCandidate|TestFTSExpanded|TestFTSNegation' ./internal/engine/...
```

### RAG SELECT latency: bound IDF, a parallel BM25 scan, and a cached neighbor index

With the postings index in place, profiling a full RAG `SELECT` — the
`HYBRID_SEARCH` query from `docs/rag-guide.md`, including neighbor-chunk
expansion — showed the remaining time was not in retrieval logic at all. Three
findings, each fixed independently:

**1. IDF was recomputed per (term, document) pair.** `ftsScoreNode` obtained a
term's inverse document frequency through the `ftsIDFLookup` closure, which does
a postings map lookup and a `math.Log` on every call. IDF depends only on the
term and the corpus, so a four-term query over 20,000 documents evaluated 80,000
logarithms to produce four distinct values. `ftsBindIDF`
(`internal/engine/fts_index.go`) resolves them once into the query tree, turning
the inner loop into a multiply. The arithmetic deliberately keeps the original
operation order (divide, then scale by IDF) rather than the tidier
`weight*tf/denominator`: float multiplication is not associative, so reordering
would shift scores in their last bits and reorder near-ties.

**2. The BM25 scan was single-threaded** while the vector branch had been
parallel for some time (`vecSearchTopK`). That made the lexical pass the long
pole on a many-core machine whenever a query's terms were too common for the
postings index to narrow. `ftsScanTopK` now splits the scan across workers with
the same thresholds and per-worker-heap merge the vector path uses. Partitioning
cannot change the answer — `ftsScoredLess` is a total order on
(score desc, rowIdx asc) and every heap operation uses it — which
`TestFTSParallelScanMatchesSerialScan` checks against a forced single-worker scan
for every query shape.

**3. The neighbor-chunk expansion index was rebuilt on every query.**
`ragBuildContextIndex` scanned the whole source table per call, and resolved its
two column names through `strings.ToLower` plus a map lookup *per cell* — 40,000
of each for a 20,000-row corpus, before reading any data. The index is a pure
function of the table's contents and those two columns, so it is now cached like
the vector and document caches: keyed by (tenant, table, columns), validated
against `table.Version`, purged by `DROP TABLE`. Column positions are resolved
once. This is what made expansion nearly free.

Cumulative effect, measured against the pre-index baseline (minima of four
alternating rounds):

| Benchmark (20k chunks, 96 dims) | before | after | |
|---|---|---|---|
| `HYBRID_SEARCH` + expansion (the guide's query) | 19.02 ms, 2,350,936 B/op | **2.95 ms, 236,810 B/op** | 6.4x, 9.9x less memory |
| `HYBRID_SEARCH` | 11.88 ms | **3.11 ms** | 3.8x |
| BM25 branch (four-term OR) | 10.82 ms | **1.95 ms** | 5.6x |
| Single corpus-wide term | 3.89 ms | **0.95 ms** | 4.1x |
| Quoted phrase | 22.84 ms | **4.38 ms** | 5.2x |
| Selective term (3 of 20,000) | 712 µs | **23 µs** | 30x |
| Prefix wildcard | 53.12 ms | **212 µs** | 250x |
| `RAG_CONTEXT_FROM` top-k | 4.17 ms, 1,759,125 B/op | **1.04 ms, 350,731 B/op** | 4.0x, 5.0x less memory |

Expansion now costs so little that `HYBRID_SEARCH` with and without it measure
the same within noise. Per-query allocation rises 6-16% on the paths that scan in
parallel (per-worker heaps) and falls tenfold on the expansion path.

**One change was measured and rejected.** Running the vector and BM25 passes
concurrently in `RAG_SEARCH` looks like an obvious win, and it was implemented,
tested and benchmarked. But both passes now saturate every core internally, so
overlapping them mostly oversubscribes: repeated alternating measurements
disagreed about the sign of the effect (12% faster in one careful run, 32% slower
in the next, against a noise floor that reached 20x on this machine). It was
reverted rather than shipped — the concurrency, its panic handling and its
error-precedence reasoning are real complexity, and no demonstrable benefit paid
for them. A conditional version, overlapping only when the lexical side is
restricted enough to scan serially, is the shape worth revisiting.

```sh
go test -bench='BenchmarkRAG' -benchmem ./internal/engine/...
go test -run='TestFTSParallel|TestRAGContextIndex' ./internal/engine/...
```

### Documents as term ids in shared arenas

After the previous two rounds, profiling `HYBRID_SEARCH` still put **68% of its
time in `mapaccess1_faststr`** — at roughly 110 ns per lookup, far above the cost
of hashing a short string. The lookups were not slow because of hashing; they were
missing cache. Each document owned a `freq map[string]int`, so scoring a corpus
walked 20,000 independently allocated maps and every probe landed in cold memory.
String keys also meant hashing and comparing bytes for a value the corpus already
had an integer for.

Two changes, in `ftsCachedDoc` and `getFTSDocCache`:

- **Term ids.** Every corpus term gets a dense `int32`. Query terms resolve through
  the dictionary once per query, in the same pass that binds their IDF, so the
  per-document hot loop compares integers. A term absent from the corpus resolves
  to `-1`, which no document contains, so it simply never matches.
- **Shared arenas.** A document is now four offsets into per-entry arenas instead
  of two heap objects: its distinct terms and their frequencies as one ascending
  run (so a frequency lookup is a binary search over contiguous `int32`s), and its
  tokens in order for phrase matching. Scoring successive documents walks memory
  sequentially.

`ftsMatchNode` and `ftsScoreNode` were also fused into one tree walk
(`ftsEvalNode`): both walked the same tree performing the same lookups, so every
matching document paid twice. That is worth less than it sounds — `ftsMatchNode`'s
OR already short-circuited on the first common term, so only one lookup in five
was redundant for the four-term query — and it measured ~14% on a single-term
query, ~2% on hybrid.

The arenas' final size is unknown until every row is tokenized, so `append` grew
them by repeated doubling, churning about as many bytes again as it kept. After 64
documents the average length is extrapolated and the whole estimate reserved in
one step, which costs ~14% more retained memory in overshoot and saves ~37% of the
build's allocation.

Cumulative against the pre-index baseline (minima of four alternating rounds), and
against the previous commit:

| Benchmark (20k chunks, 96 dims) | pre-index | previous | now | total |
|---|---|---|---|---|
| `HYBRID_SEARCH` + expansion | 16.44 ms | 2.98 ms | **2.11 ms** | **7.8x** |
| `HYBRID_SEARCH` | 10.70 ms | 2.74 ms | **2.12 ms** | **5.0x** |
| BM25 branch (four-term OR) | 9.24 ms | 2.11 ms | **1.32 ms** | **7.0x** |
| Single corpus-wide term | 2.96 ms | 908 µs | **500 µs** | **5.9x** |
| Quoted phrase | 18.18 ms | 4.72 ms | **1.78 ms** | **10.2x** |
| Selective term (3 of 20,000) | 571 µs | 19.4 µs | **17.1 µs** | **33x** |
| Prefix wildcard | 43.76 ms | 239 µs | **195 µs** | **224x** |

Phrase matching gains most (2.7x on top of the previous round) because it compares
token ids instead of strings, over a contiguous run.

Memory, measured as retained heap for one 20k-chunk corpus
(`TestRAGCacheFootprint`), is the more important result for how large a corpus fits:

| | retained | per chunk |
|---|---|---|
| before | 139.7 MB | 6,986 B |
| after | **39.3 MB** | **1,963 B** |

That is **3.6x less**, against searched text of roughly 1.4 KB per chunk — the
cache now costs about 1.4x the text it indexes, where it used to cost 5x. Build
allocation also fell from 192 MB to 152 MB per rebuild.

Correctness rests on the same differential test, strengthened: its oracle no longer
reads the cache at all. `ftsDocStrings` re-tokenizes each document from the source
row text and scores it with the original string-keyed `ftsMatchNode`/`ftsScoreNode`,
so the reference implementation shares only `ftsTokenize` with the code under test,
and the comparison is still exact float equality.
`TestFTSPostingsMatchDocumentFrequency` additionally cross-checks three
representations against each other: the postings lists, the arena term runs, and
the raw text.

```sh
go test -bench='BenchmarkRAG' -benchmem ./internal/engine/...
go test -run='TestRAGCacheFootprint' -v ./internal/engine/...
```

### ORDER BY at scale: permutation sort instead of symMerge

Profiling `BenchmarkOrderByMultiColumnNoLimit` (20,000 rows, 3 sort keys) put
41% of CPU in `sort.symMerge` plus another 22% in `Swap`/`sort.swapRange`:
every full-result ORDER BY — the raw fast path (`exec_fastpath_select.go`),
the general Row-map path (`exec_sort.go`) and the window-partition sorts
(`eval_window.go`) — used `sort.Stable`, whose in-place merge does
O(n log² n) element moves. The elements are pointer-heavy structs (a raw-row
slice plus sort keys, 64 bytes), so the moves, not the comparisons, dominated.

Three coordinated changes, all preserving the exact output order:

1. **Original-position tie-breaks make the unstable sort stable.** Every
   comparator now falls back to the item's pre-sort position on key ties.
   All positions are distinct, so the comparator pins a total order — the
   pdqsort result is unique and equals what `sort.Stable` produced. The
   window paths and `exec_sort.go` keep sorting items directly (their
   elements are one map pointer plus keys); the raw fast path instead sorts
   a **permutation of `int32` positions** (`orderedRawRowsPermAsc`, same
   pattern as `orderedValuePermAsc`), because swapping 4 bytes beats
   swapping 64-byte structs. Top-N heaps in `exec_sort.go` gained the same
   tie-break, so a bounded `ORDER BY ... LIMIT` now retains exactly the rows
   a full stable sort plus truncation would.
2. **`orderCols`: ORDER BY terms that are plain column references** (the
   overwhelmingly common shape) resolve to raw column indexes once per plan
   (`simpleSelectOrderCols`). The scan loop reads sort keys straight from the
   raw row — no `evalRawExpr` map lookup per term per row.
3. **Multi-column sort keys pack into a chunked arena** (`reserveKeySlots`,
   4096 rows per chunk) instead of one `[]any` per row. First tried:
   comparing through `raw[col]` directly with no keys at all — allocations
   dropped 33% but wall clock got *worse*, because the comparator then chased
   pointers into row arrays scattered across the heap, while the old per-row
   key slices had been allocated back-to-back during the scan and were
   cache-dense. The arena keeps that density at ~5 allocations per query
   instead of one per row.

One attempt was measured and reverted: putting the tie-break index *into* the
item structs. It grew `floatOrderedRawRow` from 32 to 40 bytes, and the float
top-N heap's swap — three struct copies per sift level, now cache-line
straddling — went from 14% to 44% of the benchmark's CPU, a consistent ~45%
regression on `BenchmarkOrderByWithLimit` across every alternating round. The
float path therefore keeps its original 32-byte item and unspecified tie
order (documented above), with its comparator rewritten as direct float
comparisons that inline where `compareOrderedFloat` calls did not.

Minima of alternating before/after rounds (this machine's noise floor makes
single runs meaningless; see the FTS section):

| Benchmark (20,000 rows) | before | after | |
|---|---|---|---|
| `sortRows` (window partition sort helper) | 963 µs | 259 µs | **3.7x** |
| `ORDER BY val DESC` (single column, no LIMIT) | 32.5 ms | 16.2 ms | **2.0x** |
| `RANK() OVER (PARTITION BY ...)` (many partitions) | 65.2 ms | 51.1 ms | 1.28x |
| three shared `OVER` windows | 77.4 ms | 60.7 ms | 1.27x |
| `ORDER BY grp, sub, val DESC` (3 columns, no LIMIT) | 59.1 ms | 57.0 ms | parity; allocs **60,010 → 40,016** |
| `ORDER BY val DESC LIMIT 20` (float top-N) | 1.65 ms | 1.48 ms | parity |
| `SELECT *`, LIKE scan, hash join, GROUP BY | unchanged | unchanged | no collateral |

**The multi-column row is a measured non-result on wall clock and should be
read as such.** Five alternating isolated rounds gave before-minima of 59–92 ms
against after-minima of 57–89 ms — the two distributions overlap almost
entirely. An earlier round on a quieter machine looked like a 1.2x win
(46.6 → 38.4 ms); a later batched round looked like a 1% loss (69.3 → 70.0 ms).
Neither is a signal. The explanation that fits: with three keys, each
comparison performs three `compareOrderedValue` calls through `any`, so
comparator cost dwarfs the element-move cost that permutation sorting
removes — exactly the opposite of the single-key case, where moves dominate
and the win is a clean 2x. The **−33% allocation** figure is the reliable
result here (the key arena), and it is stable to the byte across every round.

Correctness: the full `internal/engine` suite passes; the ordered fast path,
window ordering and `applySortOrder*` produce bit-identical output to
`sort.Stable` by construction (total order via distinct tie-break positions).

```sh
go test -bench='BenchmarkOrderBy|BenchmarkSortRows|BenchmarkWindow' -benchmem ./internal/engine/...
```

### Scalar functions: fmt-free coercion, parse-time handler binding

`BenchmarkScalarInstrWhere` — `WHERE INSTR(note, '...') > 0` over 20,000 rows
— allocated 40,232 times per query, 94% of it in `fmt.Sprintf`: several
string functions (`REPLACE`, `INSTR`, `SPLIT_PART`, `CONCAT_WS`, `POSITION`,
`QUOTE`, hash/date/text functions, and the LIKE/REGEXP evaluators) coerced
*every* argument through `fmt.Sprintf("%v", v)` — including arguments that
already were strings, which fmt then copied byte-for-byte, once per argument
per row. Others took that path only for non-string values, still paying fmt's
reflection walk for every INT or FLOAT argument.

`valueText` (`coerce.go`) renders scalars byte-identically to
`fmt.Sprintf("%v", ...)` without entering fmt: strings pass through unboxed,
int/int64/float64/bool go through strconv, everything else (notably `[]byte`,
whose `%v` form is the decimal byte list some callers rely on) falls back to
fmt. All argument-coercion sites in the scalar functions now use it.

Separately, every function call resolved its handler through the
~300-entry registry map on every row (`mapaccess2_faststr` showed at ~5% of
scalar-function profiles). The parser now resolves it once
(`bindFuncHandler`) and stores it on the `FuncCall` node; evaluation calls it
directly, and the raw fast path skips `evalFuncCall`'s re-dispatch entirely.
The field is written only at parse time, before a statement can be shared, so
cached statements stay race-free; hand-built nodes (tests, PIVOT's synthesized
aggregate) keep the map fallback.

**The node stores a 1-based index into a flattened handler table, not the
`funcHandler` value itself, and that detail is load-bearing.** The first
version stored the func value and broke `ORDER BY COUNT(*)` at the *parser*
level: `orderByProjectionName` (`parse_select.go`) matches an ORDER BY
expression against the SELECT list with `reflect.DeepEqual`, and DeepEqual
reports two func values equal only when both are `nil`. Two structurally
identical `COUNT(*)` nodes therefore stopped comparing equal, and
`SELECT dept, COUNT(*) c FROM t GROUP BY dept ORDER BY COUNT(*) DESC` failed
with "ORDER BY expression must appear in the SELECT list or have an alias".
The whole `internal/engine` package passed — the regression only surfaced in
the root package's `TestFeatureSupport/GROUP_BY_ORDER_BY` and
`internal/testhelper`'s YAML examples, which is the argument for running
`go test ./...` and not just the package under change. Equal names map to
equal indexes, so DeepEqual behaves exactly as it did before the field
existed.

The index costs one bounds-checked slice load where the func value was a
direct call, and it is measurably slower than the (unusable) func-value
variant — so the dispatch change was measured three ways, against the
map-lookup baseline it replaces and against the variant it cannot use.
Minima of four alternating rounds:

| Benchmark | registry map per row | bound index (shipped) | func value (breaks ORDER BY) |
|---|---|---|---|
| `WHERE INSTR(note, ...) > 0` | 4.13 ms | **3.45 ms** | 3.94 ms |
| `SELECT CONCAT(grp, '-', sub)` | 13.85 ms | **13.22 ms** | not measured |
| `SELECT LENGTH(note)` | 9.30 ms | **8.91 ms** | 9.58 ms |

`boundFuncHandler` reads the table slice directly rather than through its
accessor: the accessor re-enters `getAllFunctions()`, whose `sync.Once` check
is not free at once-per-call-per-row. A non-zero index is itself proof the
table is built (only `bindFuncHandler` sets it, after resolving through the
index map), so the write to the table happens-before the write to
`handlerIdx`, which happens-before the node becoming reachable by a reader.

That ordering argument is **not** backed by a race-detector run here: this
machine builds with `CGO_ENABLED=0` and `-race` requires cgo, so the claim
rests on the happens-before chain above plus the package's existing
concurrency tests (`TestConcurrent*`, `TestParallel*`,
`TestVecSearchConcurrentWithCacheReconfiguration`) passing without the
detector. Anyone with a cgo-capable toolchain should re-run those under
`-race` to close that gap.

`TRIM`/`UPPER`/`LOWER`/`LEFT`/`RIGHT` additionally return the argument's
existing interface value when the operation changed nothing (nothing trimmed,
already upper/lower, length ≥ len) instead of re-boxing an identical string —
one allocation per row on those no-op-heavy workloads.

Minima of alternating rounds, 20,000 rows:

| Benchmark | before | after | |
|---|---|---|---|
| `WHERE INSTR(note, ...) > 0` | 14.7 ms, 1.0 MB, 40,232 allocs | 6.3 ms, 40 KB, **231 allocs** | **2.3x, −99.4% allocs** |
| `SELECT REPLACE(note, 'number', 'nr')` | 30.7 ms, 140,015 allocs | 18.6 ms, 80,011 | **1.65x** |
| `SELECT TRIM(note)` | 17.7 ms, 60,011 allocs | 12.8 ms, 40,012 | 1.38x |
| `SELECT LEFT(note, 8)` | 21.3 ms | 15.1 ms | 1.41x |
| `SELECT LENGTH(note)` | 17.6 ms | 13.1 ms | 1.35x |
| `SELECT UPPER(note)` | 24.7 ms | 19.3 ms | 1.28x |
| `SELECT SUBSTR(note, 6, 12)` | 19.8 ms | 16.9 ms | 1.17x |

What remains on these shapes is dominated by the public API's `Row`
(`map[string]any`) materialization per output row, not by the functions.

Correctness: exact-output parity is the design constraint — `valueText`
matches `%v` byte-for-byte for every type it special-cases, evaluation order
and NULL short-circuits are untouched (the handlers still evaluate their own
arguments), and the no-op box reuse only triggers when the argument already
is a string, so values and types are unchanged. The full `internal/engine`
suite passes.

```sh
go test -bench='BenchmarkScalar' -benchmem ./internal/engine/...
```

### RAG sort sites: the last `reflect.Swapper` instances, and a lesson about tie-breaks at small N

An earlier optimization round (see "`GROUP BY` composite keys and `ORDER BY
... LIMIT` top-N heaps" above) left a note that `rag_search.go`/`rag_context.go`
still had the `sort.Slice`/`sort.SliceStable`-over-pointer-containing-elements
pattern, unchecked. Four call sites fit it:

1. `ragSearchFuse`'s RRF-score ordering — `sort.SliceStable` over `[]string`
   (a string header holds a pointer, so it still misses `reflect.Swapper`'s
   non-pointer fast paths).
2. `ragExpandContextFrom`'s candidate ordering — `sort.Slice` over
   `[]*ragContextCandidate`, one pointer per element.
3. `ragFindContextRows`'s per-hit match ordering — `sort.SliceStable` over
   `[]ragContextRow`, a struct embedding an `any` (docID).
4. `ragSortContextIndex`'s per-document chunk ordering — the same
   `ragContextRow`, sorted once per document while building the neighbor-chunk
   index.

Each got a concrete `sort.Interface` type with a direct two-value `Swap`,
matching the pattern already applied to `orderedRawRowsAsc`/`orderedValueRowsAsc`
(`exec_fastpath_select.go`, `exec_sort.go`). Sites (1) and (2) additionally
converted from the stable variant to `sort.Sort`, because their comparators
already had a genuine strict total order to fall back on: `ragSearchFuse`
tags every fused row with a unique insertion sequence number
(`ragFusedRow.order`, "used as a deterministic sort tie-break" per its own
comment) precisely so ties resolve without needing scan-order preservation,
and `ragExpandContextFrom`'s candidates are deduplicated by
`(docID, chunkIndex)` before sorting, so its final `ragContextKeyLess` term
can never itself tie.

**Sites (3) and (4) are the interesting case: the same conversion was tried
and measurably regressed, then reverted.** The initial version added an
`idx []int32` permutation array (the same tie-break trick used for the ORDER
BY sorts) to justify `sort.Sort` there too. `BenchmarkRAGContextSingle` —
`RAG_CONTEXT('rag_chunks', 'doc_id', 'chunk_index', 'doc-500', 6, 2, 2)`,
whose `matches` slice is a handful of rows (`before=2, after=2` around one
center chunk) — got **slower in 5 of 5 alternating rounds** (worst case
1.29 ms → 2.97 ms). The idx slice is a real heap allocation on every call;
for a sort this small (tens of elements at most, `ragFindContextRows` and
each per-document group in `ragSortContextIndex` both stay in that range),
the difference between `sort.Sort`'s O(n log n) and `sort.Stable`'s
O(n log² n) is not measurable, so the allocation bought nothing and only
cost. The fix: drop the permutation array entirely, keep `sort.Stable` on a
plain `[]ragContextRow` with just a concrete `Swap` — reflect.Swapper is
still avoided (the actual established win), with zero extra allocation. A
`len(rows) <= 1` guard skips the call altogether for the common trivial case
(one hit, or a document with a single chunk).

Minima of five alternating rounds after the fix:

| Benchmark | before | after | |
|---|---|---|---|
| `RAG_CONTEXT(...)` (single lookup, small `matches`) | 614 µs, 169 allocs | 599 µs, **167 allocs** | parity-to-slightly-better, allocs down every round |
| `RAG_CONTEXT_FROM` top-k (12k rows, 64 dims) | 1.96 ms, 3092 allocs | 1.84 ms, **3024 allocs** | allocs down **~68** every round; wall clock at worst even |
| `HYBRID_SEARCH` end to end | 2.70 ms | 2.65 ms | noise-level — VEC_SEARCH/FTS_SEARCH dominate total cost |
| `HYBRID_SEARCH` + expansion | 2.81 ms | 2.25 ms | best-case ~20% faster; mixed across rounds, never worse |

Read honestly: the RRF-fusion and context-expansion sorts are a small slice of
total query time next to vector/lexical retrieval itself, so end-to-end wall
clock is mostly within this machine's noise floor. The allocation counts are
the trustworthy signal — they are deterministic, not subject to timing
variance, and they improved in every single round measured. The general
lesson, worth keeping for the next `reflect.Swapper` site found: **the
Stable→Sort-via-tie-break conversion pays for itself only at the row counts
the earlier ORDER BY work targeted (thousands+); below roughly a few hundred
elements, a bare concrete-Swap `sort.Stable` captures the whole win with no
allocation risk.**

Correctness: `TestRAGSearchHybridConcurrentDeterministic`,
`TestRAGContextFromMergesOverlappingHitProvenance`, and the full RAG/FTS test
set in `internal/engine` pass unchanged — the tie-break arguments above are
exactly what the tests would catch if wrong (score/order ties resolving
differently, or a changed neighbor-expansion rank).

```sh
go test -bench='BenchmarkRAGContextFromTopK|BenchmarkRAGContextSingle|BenchmarkRAGHybridSearch' -benchmem ./internal/engine/...
go test -run='TestRAG|TestFTSCandidate|TestFTSParallel' ./internal/engine/...
```

### Correctness found in passing: `compare()` had no `int64` case

Not a performance item, but found while re-reading `value_semantics.go` during
this round of work and worth recording here rather than in a silent commit:
`compare()` (the function every comparison operator, `ORDER BY`, `DISTINCT`
and `GROUP BY` equality ultimately goes through) had a type switch for
`*big.Rat`, `big.Rat`, `int`, `float64`, `string`, `bool`, `[]byte` — but not
`int64`, even though `compareInt` already handled a *right-hand* `int64` and
`numeric()` already listed `int64` as a recognized numeric kind. A left-hand
`int64` fell through to the function's final fallback, which only checks
text equality: two *different* `int64` values produced `("incomparable int64
and int64", error)` rather than an ordering.

That alone would just be a visible error. The actual failure mode is worse:
`compareForOrder`, the comparator behind every generic-path `ORDER BY`,
**swallows `compare()`'s error and returns `0`** ("equal") instead of
propagating it — so two different `int64` values silently sorted as ties,
producing a wrong, unsorted-looking result with nothing surfaced anywhere.
`int64` reaches raw rows from direct storage callers even though ordinary SQL
`INSERT` coercion normalizes it to `int` first (`coerceToInt`), so this was
reachable by exactly the callers the ORDER BY float fast path's own comment
already warns about ("direct storage callers can still provide … a different
Go type").

Fixed with a `compareInt64` mirroring `compareInt`'s structure (same-type
fast path, then `int`, then `numeric()` fallback), added as `case int64:` in
`compare()`'s switch. `value_semantics_test.go` (new) pins both the direct
`compare()` behavior and the `compareForOrder` silent-tie failure mode;
temporarily reverting the fix reproduces both failures, confirming the tests
are not vacuous.

```sh
go test -run='TestCompareInt64|TestCompareForOrderInt64' -v ./internal/engine/...
```

## Correctness audit: three silent-wrong-result JOIN bugs, and a family of missing type-switch cases

A follow-up pass — triggered by a multi-agent scan of subsystems not yet
touched by the perf work above, each finding independently re-verified
against the current source before being trusted — turned up bugs more
serious than anything fixed so far in this document: several produced wrong
query results silently, with no error, on ordinary queries. These are
listed in roughly descending order of how bad the failure mode is.

### JOIN: two tables sharing a column name could get the wrong answer, non-deterministically

`SELECT * FROM a LEFT JOIN b ON a.id = b.id` (or `RIGHT`/`FULL OUTER`), where
both `a` and `b` have a column named `id`, could return a *different, wrong*
answer for the bare `id` column on every single run of the identical query
— not a consistent bug, an actually non-deterministic one. Reproduced with
200 back-to-back identical queries against a 4-row table showing the
unqualified `id` flipping between a's real value and `NULL` run to run.

The root cause: `processNonAggregateQuery` and `processAggregateQuery`
(`exec_group.go`) expand `SELECT *` by `range`-ing over the row's map to
discover its keys — and Go deliberately randomizes map iteration order.
Whichever of `a.id`/`b.id` the range happened to visit *last* silently
overwrote the row's unqualified `id` entry, regardless of which one already
held the correct, already-resolved value from the join's own merge step.
Fix: only ever write the unqualified name from a qualified sibling when the
row doesn't already carry that unqualified key itself (it does, for any
column two joined tables share) — the existing value, which reflects the
join's own deterministic "right side wins on a name collision" rule
(`mergeRows`), is authoritative and must not be re-derived from a random
qualified key.

Two further, related bugs share the same "unmatched-side NULL-fill
overwrites a value that was already correct" shape, both narrower than the
one above but real:

- **`addRightNulls`** (`row_helpers.go`), called on *every* result row of
  `processLeftJoin`'s `>500`-row hash-join branch — not just unmatched ones
  — unconditionally nulled the right table's qualified columns
  (`"b.col"`). A `LEFT JOIN` on tables where either side exceeds 500 rows
  silently returned `NULL` for `b.col` even on rows that matched, wherever
  `b.col` was referenced by its qualified name (`SELECT b.name AS bname`).
  Fixed by adding the same existence check the function's unqualified branch
  already had.
- **`processRightJoin`/`processFullOuterJoin`**'s inline left-column nulling
  for an unmatched right row unconditionally nulled *every* key from a
  sample left row — including the shared unqualified name, blanking the
  right row's own real value for it. Deterministic (not random, since this
  loop iterates a fixed key list rather than a row map) but still wrong.
  Fixed with the same existence-check pattern, factored into a shared
  `addLeftNulls` helper.

Six regression tests (`join_ambiguous_column_test.go`) cover all three,
including 200-iteration repeats for the two non-deterministic cases; each
was confirmed to fail without its fix via a temporary revert before being
trusted. The full `internal/engine` suite passes unchanged.

```sh
go test -run='TestLeftJoinLarge|TestSelectStarLeftJoinAmbiguous|TestSelectStarGroupByJoinAmbiguous|TestRightJoinUnmatchedKeeps|TestFullOuterJoinUnmatchedRightKeeps' -v ./internal/engine/...
```

### `compare()`/`coerceToBool`/`truthy`: a family of missing type-switch cases

Beyond the `int64` gap fixed earlier in this document, the same audit found
several siblings with the identical shape — a value type this engine
genuinely produces, silently mishandled by a helper whose type switch never
learned about it:

| Function | Missing case | Consequence |
|---|---|---|
| `compare()` | `time.Time` | `NOW()`/`DATE_ADD` results fall to a text-equality fallback that is unreliable even for equality (monotonic clock reading); `ORDER BY` on such a column silently no-ops (`compareForOrder` swallows the error), `WHERE` raises "incomparable" |
| `truthy()` | `int64` | Any `int64` value — even a genuinely truthy one — evaluated as `false`; backs `WHERE`-clause bare-column truthiness |
| `coerceToBool()` | `int64` handling was **wrong**, not missing | `case int, int64: return x != 0` is an interface comparison in a shared-type-switch case (`x` keeps static type `any`), and different dynamic types are never interface-equal — `int64(0) != 0` evaluated to `true`. `CAST(int64_zero AS BOOL)` returned `true` |
| `numeric()`, `coerceToFloat()`, `coerceToInt()`, `coerceToBool()` | `*big.Rat`/`big.Rat` (this engine's DECIMAL type) | `CAST(decimal_expr AS FLOAT/INT/BOOL)` and `DATE_ADD`'s numeric-interval check rejected a genuine DECIMAL value outright |
| `inferType()` | `*big.Rat`/`big.Rat`, `time.Time` | `CREATE TABLE t AS SELECT price*qty AS total FROM orders` (a DECIMAL expression) or `... AS SELECT NOW() ...` silently created the new column as `JsonType` instead of `DecimalType`/`DateTimeType` |

**One fix was implemented, benchmarked, and reverted within the same
session** — a cautionary tale about this exact style of fix. Adding
`*big.Rat`/`big.Rat` to `numeric()` itself (not just the coercion functions)
looked consistent with the table above, and initially passed its own new
test. But `numeric()` is used elsewhere as a fast-path *discriminator*:
`exec_fastpath_aggregate.go` and `eval_aggregate.go`'s `SUM`/`AVG`/unary
`+`/`-` all check `numeric(v)` first and only fall back to
`storage.DecimalFromAny`'s exact `big.Rat` accumulation when `numeric()`
says no. Making `numeric()` accept `big.Rat` meant every one of those
callers took the fast (lossy) branch on the *first row of a group*, before
their own exact-decimal fallback ever ran — `SELECT SUM(decimal_col)`
silently became a float64 approximation instead of an exact fraction,
caught immediately by the pre-existing `TestAggregateFastPathDecimalSum`
(`expected *big.Rat sum for group 1, got float64 (30.299999999999997)`).
Reverted; `numeric()`'s doc comment now states explicitly why it must never
gain a `big.Rat` case, and a new `TestNumericExcludesBigRat` pins the
negative. The other four functions have no such fast-path role and keep
their `big.Rat` cases safely.

Regression tests for every row in the table (`value_semantics_types_test.go`),
each confirmed to fail without its fix via a temporary revert.

```sh
go test -run='TestCompareTime|TestTruthyInt64|TestCoerceToBoolInt64Zero|TestNumericExcludesBigRat|TestCoerceToFloatBigRat|TestCoerceToIntBigRat|TestCoerceToBoolBigRat|TestInferTypeBigRatAndTime' -v ./internal/engine/...
```

### Smaller performance fixes from the same audit

Once the correctness findings above were fixed, the remaining audit
findings were ordinary performance opportunities matching patterns already
established in this document — implemented, low-risk, each verified by the
existing or a new benchmark:

- **`writeFmtKeyPart` gained an `int64` case.** Every multi-column
  `GROUP BY`/`PIVOT`/`DISTINCT`/set-op key builder calls this function; an
  `int64`-typed grouping column previously fell to the `default` branch's
  full JSON marshal per row instead of a two-line integer append.
  `writeSingleGroupKey` (the single-column `GROUP BY` fast path, which
  already special-cased `int64` for exactly this reason) now simply
  delegates to it.
- **Hash-join build/probe loop stopped re-lowercasing a loop-invariant
  column name on every row.** `buildColumn`/`probeColumn` never change
  across a join; `getJoinKey` now takes the pre-lowered form instead of
  calling `strings.ToLower` per build row and per probe row in the
  `>500`-row path.
- **`DELETE` no longer rebuilds the row slice and secondary indexes when
  its predicate matched zero rows.** Both the raw-predicate fast path and
  the trigger/RETURNING path gated `t.Rows = kept` /
  `t.ReindexSecondaryIndexRows(oldToNew)` on `del > 0`, mirroring the
  pattern `UPDATE` already uses next to it. Measured on a 20,000-row table
  with one secondary index, four alternating rounds: **11.8–12.8 ms after
  vs. 13.7–18.5 ms before, allocations down ~750 every round.**
- **`INSERT`'s constraint check now visits only columns that can ever
  fail.** `validateRowConstraintsWith` used to walk every column of the
  table per row; both `executeInsertAllColumns` and
  `executeInsertSpecificColumns` now precompute the constrained-column
  index list once per statement (`constrainedColumnIndices`) instead of
  passing `nil` (all columns) on every row.
- **`applyColumnDefaults` is skipped entirely when no column has a
  default,** a schema-invariant fact now computed once per `INSERT`
  statement (`tableHasDefaults`) instead of scanning every column of every
  row to find out there was nothing to do.
- **`ARRAY_SORT` and the unary `+`/`-`/`NOT` join fast path** got the same
  two fixes already applied elsewhere in this document: a concrete
  `sort.Interface` (`arraySortAsc`) instead of `sort.Slice`'s
  `reflect.Swapper` fallback for `[]any`, and `evalJoinRawUnary` now calls
  a shared `applyUnaryOp(op, value)` directly instead of allocating a
  `&simpleSelectPlan{}`/`&Unary{}`/`&Literal{}` trio purely to reach
  `evalRawUnary`'s switch.
- **`buildRawFilterRegexp`** now calls the shared `compileCachedRegexp`
  instead of `regexp.Compile` directly — it was the one REGEXP/RLIKE/SIMILAR
  TO evaluation path in the engine that bypassed the process-wide compiled-
  pattern cache every other one already uses.
- **`applyDistinctOn`** (`SELECT DISTINCT ON`) and
  **`formatWindowPartitionKey`** (window `PARTITION BY` key, called once per
  partition column per output row) switched from `fmt.Sprintf`/`fmt.Fprintf`
  to `valueText`/`writeFmtKeyPart` — the same fmt-avoidance sweep applied
  earlier in this document, missed by it because these two call sites live
  outside `internal/engine`'s scalar-function files.

```sh
go test -bench='BenchmarkDeleteMatchesNothing' -benchmem ./internal/engine/...
```

### Deferred: three window-function complexity findings, not implemented this round

The same audit found three real algorithmic-complexity issues in window
function evaluation, deliberately left unfixed this round — each needs a
new per-(shape, partition) cache structure threaded through existing
memoization, which is a larger, riskier change than anything else in this
section, and deserves its own dedicated verification pass rather than
being rushed alongside everything above:

- **Partition build is O(N·K) instead of O(N)** for a query with K
  distinct partition values (`filterPartitionIndexed` re-scans all N rows
  from scratch for every newly-seen partition key).
- **`DENSE_RANK`/`RANK` are O(P²) per partition** (`evalDenseRankFunction`
  rescans from the partition's start on every output row instead of
  computing the whole partition's rank sequence in one forward pass).
- **`MOVING_SUM`/`MOVING_AVG` are O(N·window) instead of O(N) amortized**
  (`evalMovingAggregate` re-sums its entire window from scratch every row
  instead of maintaining a running sum across successive, mostly-overlapping
  windows).

All three would need a small per-(shape, partition) running-state cache,
similar in shape to the existing `windowPartitionEntry`/`natBreaksCache`
memoization already in `eval_window.go`/`eval_window_classify.go`.

## MBTiles tile serving: tinySQL vs SQLite

A tile server runs exactly one query per request:

```sql
SELECT tile_data FROM tiles
WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?
```

so that point lookup decides whether tinySQL can replace SQLite for MBTiles
serving. `benchmarks/tile_serving_benchmark_test.go` runs it through
`database/sql` on both engines with bound parameters and one connection, over a
full zoom-8 grid (65,536 tiles, 800-byte payloads), fetching random tiles to
defeat any single-tile cache.

The first measurement was not close: **4.64 ms for tinySQL against 13 µs for
SQLite**, growing linearly with the tileset while SQLite stayed flat. The
composite index existed and was never used.

The cause was not a missing planner rule but a safety check that cost more than
the work it guarded. SQL compares `1` and `1.0` as equal while the durable index
encoding tags integers and floats differently, so a numeric seek is only sound
when no row that compares equal is stored under the other tag.
`numericSecondaryIndexSeekSafe` established that by **scanning every row, once per
indexed column** — three full table scans per tile lookup, to decide whether a
seek was permitted. An indexed point lookup was therefore slower than the scan it
replaced.

Whether a column holds any `float64` is a property of the column, not of the
literal being sought. Cached per `(table, table.Version)` like every other cache
in the engine, it is computed once per table version, and an integer literal
against a float-free column is then decided without touching a row
(`internal/engine/index_seek_safety.go`). Float literals keep the original
per-value scan, because `-0.0` and `0.0` compare equal but encode differently, so
no column-level summary can prove a float seek sound.

| Tile lookup (65,536 tiles, 800 B payload) | before | after |
|---|---|---|
| tinySQL/mem | 4.64 ms | **8.7 µs (533x)** |
| SQLite/mem (`:memory:`) | 8.6 µs | 8.6 µs |
| SQLite/file (WAL, the real MBTiles case) | 40-65 µs | 40-65 µs |

Steady state over three alternating rounds, tinySQL vs SQLite/mem:
8.68/8.59, 12.63/12.91, 9.64/9.66 µs — **parity**, and 4-5x faster than
SQLite/file. At 4,096 tiles both are ~6-8 µs.

Read the two SQLite rows separately. `:memory:` is the fast bound, not how
MBTiles is deployed; a tileset on disk is the honest comparison, and tinySQL beats
it several times over once the tileset is resident.

Profiling what remains shows the lookup itself is no longer the cost: SQL lexing
(`ToUpper`, `tokenizeBlob`, `Fields`) and hex encode/decode of the BLOB payload
dominate. Those are shared with every other query shape rather than specific to
tiles, and are where further tile-serving work should go.

### `tinysqld -tiles`: caching the HTTP layer's own SQL, not just the index seek

The measurements above go through `database/sql` with a prepared statement and
bound `?` parameters, so the SQL is lexed and planned exactly once no matter how
many tiles are fetched. `cmd/tinysqld`'s `/tiles/` HTTP handler did not have
that luxury: tinySQL's public `Execute` takes no bind parameters, so each
request built a literal SQL string with `fmt.Sprintf` (tile coordinates already
validated and range-checked, so this carries no injection risk) and called
`tinysql.ParseSQL` fresh, every time — paying full lexing and parsing on every
tile a browser ever requested, including tiles it had already served.

A map viewport is a handful of tiles re-fetched by every client that pans
across it, so most of that parsing was repeated on SQL text the daemon had
already seen. `executeTileSQL` (`cmd/tinysqld/http.go`) runs the three
tile-serving queries — the tile lookup, its metadata row, and the observed
zoom range — through a bounded `tinysql.QueryCache` (`tinysql.Compile` /
`tinysql.ExecuteCompiled`) keyed by that exact literal SQL text instead of
`tinysql.ParseSQL`. A tile requested for the second time reuses its parsed
statement and the access-plan shape cached on that statement's AST node, the
same mechanism `Compile`/`ExecuteCompiled` document for any repeated query.

| Tile lookup, in-process (`cmd/tinysqld`) | before (`ParseSQL` per request) | after (`executeTileSQL`, warm) |
|---|---|---|
| latency | 3085 ns/op | **568 ns/op (5.4x)** |
| allocations | 65 allocs/op, 3568 B/op | 6 allocs/op, 736 B/op |

```sh
go test ./cmd/tinysqld -run '^$' -bench TileLookup -benchmem -count 3
```

See `BenchmarkTileLookupUncached`/`BenchmarkTileLookupCached` in
[cmd/tinysqld/tiles_benchmark_test.go](./cmd/tinysqld/tiles_benchmark_test.go).
The cache is bounded (LRU, 4,096 entries), so a crawler requesting an unbounded
stream of distinct tile coordinates evicts cold entries rather than growing
without limit; it does not change results, since a cached statement re-reads
the live table on every execution and only the parsed shape is memoized.

This benchmark was also what exposed the scale of the effect. Note the iteration
count: at `-benchtime=200x` tinySQL measured 15-19 µs against SQLite's 8-10 µs,
because statement-cache warm-up had not amortized. Only at 3000x does either
engine reach steady state, which is the regime a server runs in.

```sh
go test -run=none -bench='BenchmarkTileLookup' -benchtime=3000x ./benchmarks/...
```

## Spatial range queries: bounding boxes on a POI layer

Every viewport redraw and every "what is near me" is the same query:

```sql
SELECT id FROM poi WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
```

`selectSecondaryIndex` handled equality only, so this fell through to a table
scan even with an index on `(lat, lon)` — the index existed and no range
predicate could reach it. `EXPLAIN` said `TABLE SCAN` for a bounding box, a
one-sided `lat > ?`, and `GEO_DWITHIN` alike.

A range seek is possible because index entries are a byte-sorted array and the
numeric key encoding is order-preserving: integers are stored big-endian with the
sign bit flipped, float64 with its bits flipped for negatives, so comparing
encoded bytes compares the numbers. `LookupSecondaryIndexRange`
(`internal/storage/secondary_index_range.go`) walks that order; the planner picks
an equality prefix plus one bounded column, the standard B-tree access shape
(`internal/engine/plan_range_index.go`).

Three restrictions are correctness requirements rather than tuning choices:

- **Text and BLOB columns cannot range-seek.** Their keys are framed as
  tag + 4-byte length + payload, so byte order compares the *length* first:
  `'z'` sorts before `'aa'`. A walk would return the wrong rows, so these fall
  back to a scan.
- **A column mixing integers and floats cannot either.** The two carry different
  type tags, so every integer sorts before every float regardless of value. The
  cached column profile detects this and the planner falls back.
- **Signed zero moves the bound.** `-0.0` and `0.0` compare equal but `-0.0`
  encodes strictly below `+0.0`, so a walk bounded by `+0.0` would skip a stored
  `-0.0` on `v >= 0.0` and wrongly include it on `v < 0.0`. The bound is moved to
  the zero that brackets both. This was a real bug caught by the differential
  test, not a hypothetical.

Because the range constrains only one index column, the result is a superset and
the residual `WHERE` still runs — which is exactly what makes a two-dimensional
predicate work: the walk narrows to the latitude band, `lon` is filtered per row.
That is not an R-tree, and it is not presented as one.

50,000 POIs over a 10° square, a 0.05° viewport at a random position per
iteration, both engines through `database/sql` with the same B-tree index:

| | tinySQL | SQLite | |
|---|---|---|---|
| Viewport, no index | 11.08 ms | 14.07 ms | the previous behaviour |
| Viewport, `(lat, lon)` index | **235 µs** | 143 µs | **47x faster**; 1.6x behind SQLite |
| `cat = ?` + viewport, `(cat, lat)` index | **63 µs** | 125 µs | **2x faster than SQLite** |

The composite row is the more representative one — a map asks for *one layer* in
a viewport, not every POI — and there tinySQL wins. On the pure two-axis box
SQLite is still ahead because it can filter the second column inside the index,
while tinySQL fetches the row to evaluate `lon`. Evaluating trailing index
components during the walk is the obvious next step and is not implemented.

SQLite's R\*Tree module would be a stronger structure for genuine 2-D search;
this compares like-for-like B-tree indexes and does not claim parity with it.

```sh
go test -run=none -bench='BenchmarkViewport|BenchmarkCategoryInViewport' -benchtime=200x ./benchmarks/...
go test -run='TestRangeIndex' ./internal/engine/...
```

### Tile serving from disk: the case that actually ships

The tile numbers above put an in-memory tinySQL against an in-memory SQLite,
which is not how a tileset is deployed. A navigation device or tile server holds
tiles on disk, and for tilesets past RAM the answer is `ModePagedIndex`: an
immutable page store whose complete-composite-equality lookup — exactly a tile
lookup — resolves a B+Tree and materializes only the located row. It never decodes
the whole table, which the legacy `ModeIndex`/`ModeHybrid` GOB codec does, and
which is why the docs used to send multi-gigabyte tilesets to SQLite.

`BenchmarkTileLookupOnDisk` runs the same 65,536-tile fixture and the same
single-lookup query as the in-memory benchmark, against a 32 MiB page budget, so
the two are comparable:

| Tile lookup, 65,536 tiles, 800 B payload, on disk | ns/op across 3 rounds | B/op |
|---|---|---|
| tinySQL `paged_index` (read-only) | 59.8 / 57.3 / 60.6 µs | 15,432 |
| SQLite file (WAL, `synchronous=NORMAL`) | 38.5 / 109.0 / 112.4 µs | ~2,600 |

Read the spread, not just the minimum. tinySQL lands within 6% across rounds;
SQLite's file path varies by 3x. On minima SQLite is ahead (38.5 vs 57.3 µs); on
medians tinySQL is (58 vs 109 µs). Calling either "faster" from these numbers
would be overreaching — they are in the same range, with tinySQL the more
predictable of the two, which is what a p95 tile-latency budget cares about.

Where tinySQL clearly loses is allocation: **15.4 KB per lookup against SQLite's
~2.6 KB**. Profiling the query path attributes 33.3 MB of 39.8 MB — 84% — to
`Pager.readPageRaw`, which allocates a fresh page buffer on every buffer-pool
miss. With a working set well past `max_memory_bytes` most lookups miss, so this
is ~11 KB of garbage per tile.

Recycling those buffers is the obvious fix and is **not implemented**, for a
stated reason: a page buffer may only be reused once nothing decoded from it is
still referenced, and proving that across the row codec, B+Tree records and BLOB
materialization is an aliasing audit, not a patch. Getting it wrong corrupts data
silently, which is strictly worse than allocating. Until then, size the page
budget to the hot zoom levels rather than to the whole tileset.

One further limit: only *equality* predicates take the per-record paged path. A
range predicate against a `ModePagedIndex` table falls back to the full-table
compatibility path, so the range seeks documented above do not yet apply to it.
That matters for POI queries on a disk-backed database, not for tiles.

`cmd/tinysqld/tiles_paged_test.go` covers the deployment end to end: an artifact
built writable, reopened read-only, and served over `/tiles/{z}/{x}/{y}` with the
XYZ-to-TMS flip intact.

```sh
go test -run=none -bench='BenchmarkTileLookupOnDisk' -benchtime=3000x ./benchmarks/...
go test -run='TestTileEndpointOverPagedIndex' ./cmd/tinysqld/...
```

## MBTiles import: a tileset larger than memory

The section above establishes that *serving* a `ModePagedIndex` tileset larger
than memory already worked: a point lookup resolves a B+Tree and materializes
only the located row. *Building* that tileset in the first place did not have
the same property, for a reason specific to how the paged backend persists a
table.

`PagedIndexBackend.SaveTable` "replaces the entire table contents (drop +
recreate of the tree)" — its own doc comment. `ImportMBTiles` already streamed
its *source* scan in bounded batches, one SQLite row cursor at a time, but the
*destination* side had no equivalent: `db.Get` on a `ModePagedIndex` table
loads every row via `PageBackend.LoadTable`, and nothing durable happened
until that in-memory table was eventually saved — at which point SaveTable's
full rebuild needed the complete row set in memory anyway. A country-scale
`.mbtiles` runs to gigabytes of tile blobs; building one this way needed the
whole thing resident regardless of how the source was read.

`pager.PageBackend.AppendRows` (`internal/storage/pager/backend.go`) is the
missing primitive: it inserts new rows into an *existing* B+Tree in place —
using the same `BTree.Insert` SaveTable already calls, just without freeing
and rebuilding the tree first — and does the same for each declared unique
secondary index. `storage.DB.AppendRowsFast` computes each index's canonical
key per row and calls it; `internal/importer.insertTypedRows` calls that for
every batch when the destination is `ModePagedIndex`, falling back to the
original db.Get-and-append path for every other storage mode, unchanged. The
symmetric read-side gap — exporting a `ModePagedIndex` tileset back to
`.mbtiles` also called `db.Get` — is closed the same way, with
`PageBackend.ScanTableRows`/`DB.ScanRowsFast` streaming rows to the SQLite
writer one B+Tree leaf at a time instead of collecting them into a slice
first.

One correctness-adjacent fix was necessary to make batching itself safe:
`PageBackend.CommitTx` durably records a transaction but does not flush or
clean dirty pages — only `Checkpoint` does, and `PageBufferPool.evictOne` by
design never evicts a dirty page (it is the only copy of data the WAL has not
yet been checkpointed to). Without a checkpoint between batches, every batch's
newly dirty pages stayed pinned in the pool, so each subsequent page
allocation had to scan a *growing* list of unevictable pages before giving up
— `AppendRows`'s per-batch cost was accidentally `O(rows imported so far)`,
not `O(batch size)`. `AppendRows` now checkpoints once per call, tying
checkpoint frequency to the caller's own `BatchSize` instead of adding a
separate tuning knob.

A second, unrelated bug compounded this during development: `insertTypedRows`
called the shared `applyDefaults`, whose `CreateTable` heuristic ("enable it
when neither `CreateTable` nor `Truncate` is explicitly set") cannot
distinguish a caller's explicit `false` from an unset field. Every batch after
the first passes `CreateTable: false` to mean "the table already exists" —
`applyDefaults` flipped it back to `true` anyway, so `createTable`'s
`db.Put`-fails-then-`db.Get`-fallback fired before *every* batch, forcing a
full-table read before each one. Harmless waste for the in-memory default
(`db.Get` is a cached-pointer return there); silently catastrophic for
`ModePagedIndex`, where it turned into a real disk scan of everything
imported so far, repeated on every batch. `insertTypedRows` no longer calls
`applyDefaults`: every real caller (`ImportMBTiles`, `OpenMBTiles`,
`ImportOSM`, `ImportRoutingGraph`) already normalizes its own options once at
its own entry point before deriving a per-batch copy, which is what needs to
survive unchanged.

**Memory**, importing 500,000 tiles (~750 MB of tile payloads, 1500 B each)
into a database capped at 32 MB (`MaxMemoryBytes`):

| | before any of the above | after |
|---|---|---|
| peak heap during import | would need ~750 MB+ resident | **34.5 MB** |
| elapsed (500k tiles, seed + import) | did not finish in 5 minutes | 26.3 s |

**Speed and allocation**, `BenchmarkPagedIndexMBTilesImport{AppendRows,SaveTableRebuild}`,
50,000 tiles, `BatchSize` 1000, `-benchtime=1x`:

| | AppendRows (after) | SaveTable rebuild (equivalent to before) |
|---|---|---|
| elapsed | 3.20 s | 4.10 s |
| heap resident after import | 48.6 MB | 517 MB (10.6x) |
| peak RSS | 123.7 MB | 1032.3 MB (8.3x) |
| allocations | 5.08 M | 24.05 M (4.7x) |
| throughput | 15,614 rows/s | 12,204 rows/s |

SaveTable's memory cost is proportional to the *total* table size and grows
without bound as more of it is imported; AppendRows's is proportional to one
batch and stays flat regardless of how large the tileset gets. At 50,000 rows
the gap is already 8-10x; it widens with scale, which is the entire point for
a continent-sized `.mbtiles`.

```sh
go test ./benchmarks -run '^$' -bench BenchmarkPagedIndexMBTilesImport -benchmem -benchtime=1x
go test -tags=sqliteimport ./internal/importer/... -run 'TestImportMBTilesPagedIndex|TestExportMBTilesStreamsFromPagedIndex' -v
```

## MBTiles B+Tree split fix, and the extended read-path benchmarks

A real regional MBTiles import (158 MiB, 11,465 `images` rows, mixed BLOB
sizes with 409 in the 1.4–2.5 KiB range) failed against `ModePagedIndex` with:

```
insert row 10784: split right insert: btree page full: need 1569, have 1536 free
```

The B+Tree leaf split (`internal/storage/pager/btree.go`) picked its split
point by entry count (`len(merged) / 2`). With variable-size records, a count
midpoint can put several large records on one side even though both sides
together fit in two pages — the old page had already been proven to fit, but
its replacement, rebuilt with the wrong-side entries, had not. The fix,
`leafSplitIndex`/`internalSplitIndex`, chooses the split point by *encoded
byte footprint* instead, minimizing the size skew between the two output
pages subject to both fitting one page's capacity; `leafEntryNeedsOverflow`
centralizes the inline-vs-overflow decision as a pure function of page size,
key and value, so it can never depend on how full a particular page happens
to be right now. A second, related gap in the same code path is also fixed:
replacing the *only* entry in a single-entry leaf with a larger value used to
route through the same split code and fail outright — one entry has no
second side to split into — so an oversized replace or insert now first tries
a from-scratch compaction of the leaf's live entries (which also reclaims
dead space earlier in-place updates never freed) and only allocates a sibling
page if the live content genuinely does not fit one page.

This is a write-path algorithm fix, not a page-layout change — see
[the storage guide](./docs/storage-guide.md#btree-leafinternal-splits-are-byte-balanced-not-count-balanced)
for exactly what that does and does not mean for existing `paged_index`
artifacts. Regression coverage: `internal/storage/pager/btree_split_regression_test.go`
and `internal/engine/paged_index_mbtiles_regression_test.go`.

### Read-path benchmarks, extended: tinySQL vs. SQLite on the same shapes

`BenchmarkPagedIndexMBTilesAccess` (above) already isolates warm/cold and
compares against SQLite on a mixed-size corpus. Three further shapes, all in
`benchmarks/paged_index_mbtiles_read_path_benchmark_test.go`, each run
against both backends on the identical request corpus, row count and
artifact-building code path (same DDL, same z/x/y-and-tile_id addressing,
`db.SetMaxOpenConns(16)` on the SQLite side for the parallel case so it isn't
serialized behind a single connection):

**By size class** — a small inline payload, two right at the inline/overflow
boundary (1,569 B is the literal size from the fixed bug report above; 2,500 B
is the top of the reported critical band), and a large payload that always
overflows, each in its own fixed-size artifact per backend, so a regression
in one class — or a crossover point where SQLite overtakes tinySQL or vice
versa — cannot hide behind an average over the others
(`-bench BenchmarkPagedIndexMBTilesAccessBySize -benchtime=1000x`, this
machine):

| size class | tinySQL ns/op | tinySQL p50/p95/p99 (µs) | SQLite ns/op | SQLite p50/p95/p99 (µs) | tinySQL speedup |
|---|---|---|---|---|---|
| inline, 256 B | 3,935 | 3.00 / 8.46 / 19.08 | 16,763 | 11.67 / 58.33 / 110.0 | 4.3x |
| boundary, 1,569 B | 4,052 | 3.04 / 8.58 / 18.38 | 24,749 | 8.54 / 72.92 / 224.4 | 6.1x |
| boundary, 2,500 B | 7,468 | 3.33 / 10.38 / 49.88 | 31,041 | 10.42 / 100.1 / 334.2 | 4.2x |
| overflow, 50,000 B | 25,714 | 12.38 / 69.96 / 98.96 | 51,021 | 37.46 / 113.5 / 169.7 | 2.0x |

tinySQL stays faster across every class named in the bug report, including
the two boundary sizes that used to fail to import at all; the margin
narrows for the always-overflow class, where both backends pay for a second
page fetch (a B+Tree overflow chain vs. SQLite's own overflow pages) and the
per-lookup cost is dominated by copying the 50 KB payload rather than by
index-seek overhead.

**Concurrent readers** against one shared, already-open artifact — the shape
a tile server actually runs under (`-bench BenchmarkPagedIndexMBTilesAccessParallel
-benchtime=2000x`, same machine, `GOMAXPROCS=12`). `ns/op` in Go's parallel
mode is total wall time divided by the *total* op count across every
goroutine; the reported p50/p95/p99 are measured per call and rise with
contention even though `ns/op` looks flat:

| parallelism | tinySQL ns/op | tinySQL p50/p95/p99 (µs) | SQLite ns/op | SQLite p50/p95/p99 (µs) |
|---|---|---|---|---|
| 1 | 7,806 | 54.96 / 316.7 / 513.9 | 25,270 | 170.1 / 1,025 / 1,992 |
| 4 | 7,012 | 121.6 / 1,233 / 1,968 | 17,841 | 673.7 / 1,802 / 2,547 |
| 16 | 6,757 | 241.8 / 4,947 / 7,736 | 17,248 | 2,503 / 7,017 / 9,775 |

tinySQL's read path takes no locks across readers (an immutable page store),
so its `ns/op` stays essentially flat as parallelism rises; SQLite's improves
from parallelism 1 to 4 (connection-pool warm-up dominates at 1) but both its
`ns/op` and its tail latency grow again at 16, consistent with contention
inside `modernc.org/sqlite`'s own connection/lock handling under
`database/sql`.

**Open/reopen** — the one-time cost of opening a published artifact
read-only (catalog/schema decode plus the first page reads), separate from
any lookup that follows it (`-bench BenchmarkPagedIndexMBTilesOpenReopen -benchtime=200x`):

| artifact | tinySQL ns/op | SQLite ns/op | tinySQL speedup |
|---|---|---|---|
| 1,024 rows (6.8 / 3.4 MB) | 51,471 | 120,972 | 2.4x |
| 16,384 rows (108 / 54 MB) | 31,251 | 107,533 | 3.4x |

tinySQL's open cost does not grow with artifact size (it reads the
superblock and catalog, not the tables); SQLite's `Ping` here still opens the
file and validates its header, which is cheap but not row-count-independent
in the same way.

```sh
go test ./benchmarks -run '^$' -bench BenchmarkPagedIndexMBTilesAccessBySize -benchmem -benchtime=1000x
go test ./benchmarks -run '^$' -bench BenchmarkPagedIndexMBTilesAccessParallel -benchmem -benchtime=2000x
go test ./benchmarks -run '^$' -bench BenchmarkPagedIndexMBTilesOpenReopen -benchmem -benchtime=200x
go test ./internal/storage/pager/... -run 'TestBTreeExactBoundarySizesAllKeyOrders|TestBTreeReplaceDeleteInsertOverflowSequenceNoLeak|TestBTreeMultiLevelSplitInvariants|TestLeafEntryNeedsOverflowBoundary' -v
go test ./internal/engine/... -run TestPagedIndexRegionalMBTilesReplaceDeleteInsert -v
```

## Lexer: ASCII fast paths, scratch-buffer reuse, and zero-copy string/symbol tokens

Every prior round this session optimized the parser or the engine above it;
the lexer itself (`internal/engine/lexer.go`) had never been profiled. It
turned out to have the same shape of problem as everything else found this
session: correct code, but paying full Unicode-table and allocation costs on
the overwhelmingly common ASCII/no-escape case.

Profiling `BenchmarkLexerNextTokenOnly` (a new benchmark added this round
that drains a token stream without building any AST, isolating lexer cost
from parser recursion) with `-memprofilerate=1` showed allocation flat-cost
concentrated in three places: `upper()`'s `[]byte(s)` + `string(b)` pair on
every keyword/identifier, `tokenizeSymbol()`'s `string(rune)` conversion on
every single-character token (parens, commas, operators — the single most
frequent token shape in real SQL), and `tokenizeString()`/`tokenizeQuotedIdent()`
unconditionally building through a `strings.Builder` even when the literal
contains no escaped quote.

Fixes, in the order they were made:

- **`peek()`/`next()`/`skipWS()`/`tokenizeIdentOrKeyword()`**: added an ASCII
  byte-value fast path (`c < utf8.RuneSelf`) ahead of
  `utf8.DecodeRuneInString`/`unicode.IsSpace`/`unicode.IsLetter`, falling back
  to the general Unicode path only the moment a non-ASCII byte is seen.
  Verified against genuinely multi-byte identifiers (`héllo`, `café_table`,
  `日本語`, `naïve_col`) — a byte-wise fast path that mishandles a continuation
  byte would corrupt exactly this input, so `lexer_test.go` pins it directly
  end-to-end through `CREATE TABLE`/`INSERT`/`SELECT`.
- **`upperInto(s string) string`**: new method with a `scratch []byte` field
  on `*lexer`, reused (grown only when a longer candidate appears) across
  every token of one parsed statement — the same buffer-reuse pattern already
  used elsewhere in the engine (`writeFmtKeyPart`, `applyDistinctOn`). Cuts the
  has-lowercase-ASCII path from 2 allocations to 1. The original free-standing
  `upper()` function is kept unchanged for two cold call sites
  (`parse_column_type.go`, `parse_statement.go`) that don't need it.
- **`tokenizeSymbol()`**: single-character tokens now slice into a
  package-level precomputed 128-byte string (`asciiSingleChar`, built once at
  init so `asciiSingleChar[b] == byte(b)`) instead of converting a rune to a
  string — Go string slicing shares the backing array, so this is genuinely
  zero-allocation, unlike the next bullet's dead end. Two-character operators
  (`<=`, `<>`, `>=`, `!=`) return Go string constants directly instead of the
  old `string(a)+string(b)` concatenation.
- **`tokenizeString()`/`tokenizeQuotedIdent()`**: both now scan ahead
  byte-by-byte for the closing quote before building anything. If a plain
  close is found with no doubled-quote (`''`/`""`) escape on the way, the
  token value is the original string sliced directly
  (`lx.s[contentStart:i]`) — zero-copy, and skips the decode/re-encode
  `strings.Builder.WriteRune` does per byte even for plain ASCII. Only when a
  doubled-quote escape is actually present does it fall back to the original,
  unchanged `strings.Builder`-based path. A single-byte ASCII quote can never
  equal a continuation byte of a multi-byte UTF-8 sequence, so the byte-wise
  scan is safe on arbitrary Unicode content without decoding runes at all.

### A dead end worth recording: `string(byte(r))` does not avoid an allocation

The first attempt at `tokenizeSymbol()` used `string(byte(r))` for
single-character symbols, on the assumption that converting a `byte` (as
opposed to a `rune`) to a `string` has some compiler-recognized
single-byte-value fast path. It doesn't. A direct isolated benchmark of both
conversions came back identical: **`string(rune)` and `string(byte(r))` both
allocate ~23 ns/op, 1 alloc/op, 4 B/op** for a runtime (non-constant) value —
Go has no free byte→string conversion. This was caught not by a test failure
but by the A/B benchmark's allocation counts coming back *higher* than
before instead of lower on the first attempt, which prompted checking the
assumption directly rather than trusting it. The actual zero-allocation
technique is slicing into a precomputed string table (`asciiSingleChar`
above), confirmed by the same isolated benchmark at **1.5 ns/op, 0
allocs/op**. This mirrors the `numeric()`/`big.Rat` and small-*N*
permutation-sort reversals earlier in this document: another plausible Go
performance belief that turned out to be wrong under direct measurement.

### Results

New tests (`lexer_test.go`): Unicode identifiers (bare and end-to-end through
a real `CREATE TABLE`/`INSERT`/`SELECT`), every single- and double-character
symbol/operator plus boundary cases (`<==` → `<=`, `=`; `a<b` → `a`, `<`,
`b`), whitespace and both comment forms in combination, keyword
case-insensitivity across a scratch-buffer growth-then-shrink sequence, and
string/quoted-identifier literals covering both the zero-copy fast path and
the doubled-quote escape fallback (including a literal with multi-byte
content). All pass, and the full `internal/engine` suite (`go test
./internal/engine/... -count=1`) passes with no regressions.

Four alternating A/B rounds (`-benchtime=300x`, alternating before/after
binaries per round rather than running each once in sequence, per this
machine's documented 2–3x run-to-run noise) gave a consistent picture. On
this noisy machine, allocation counts are the trustworthy signal (they don't
vary round to round at all), so they're reported alongside the noisier
wall-clock minima across all 4 rounds:

| benchmark | allocs/op before → after | ns/op min before → min after |
|---|---|---|
| `BenchmarkLexerNextTokenOnly` (pure lexer) | 68 → 35 (−49%) | 11,860 → 9,845 |
| `BenchmarkParseBulkInsert` (200-row `VALUES`) | 5,425 → 3,222 (−41%) | 800,813 → 651,319 |
| `BenchmarkParseComplexSelect` (multi-join, window fn, `DATE_ADD`) | 147 → 114 (−22%) | 32,350 → 33,761 |
| `BenchmarkParseSimpleSelect` | 30 → 30 (unchanged — too few symbol/keyword tokens to move the needle) | 7,443 → 3,918 |

`ParseComplexSelect`'s minimum went the "wrong" way in this particular
4-round set (33,761 > 32,350) even though its allocation count dropped 22%
same as always — one "before" round happened to land unusually low (32,350
vs. that same round's own neighbors at 48–52k), which is exactly the kind of
single-round fluke this machine's documented 2–3x noise produces. This is
the reason allocation counts, not ns/op minima, are treated as the
trustworthy signal throughout this document: they are bit-for-bit identical
across every round, while wall-clock minima can and do flip sign on any
individual benchmark in any individual 4-round set. Averaged over multiple
independent 4-round sets taken during this work, `ParseComplexSelect` and
every other benchmark were consistently faster after, matching their
allocation-count improvement.

An adversarial-review workflow (4 independent lenses — fast-path/slow-path
correctness, Unicode byte-boundary safety, EOF/empty-input edge cases, test
coverage gaps — each finding then put through 3 independent refutation
attempts) was run against the diff before considering this done, the same
process used earlier in this document for the JOIN and date/time bugs. It
found one real bug, reported independently by all four lenses and confirmed
by all 12 refutation attempts:

### Found by adversarial review: the fast path silently dropped UTF-8 sanitization

The pre-diff `tokenizeString`/`tokenizeQuotedIdent` built every token through
`lx.next()` (which decodes via `utf8.DecodeRuneInString`) and
`strings.Builder.WriteRune`. Go's `utf8.DecodeRuneInString` returns
`utf8.RuneError` (U+FFFD) for any malformed byte, and `WriteRune` re-encodes
that as its 3-byte UTF-8 form — so the old lexer *always* silently replaced
invalid UTF-8 inside a string or quoted-identifier literal with the
replacement character, guaranteeing every token value was valid UTF-8. The
zero-copy fast path bypassed this entirely: it located the closing quote with
a raw byte scan and, when no `''`/`""` escape was present (the common case),
returned `lx.s[contentStart:i]` verbatim — including any malformed bytes,
unchanged. For example, lexing the 3-byte literal `'` + `0x80` + `'` returned
the raw invalid byte under the new code instead of the 3-byte U+FFFD
replacement the old code always produced. Not a panic or crash — `lx.pos`
bookkeeping was unaffected, since a literal ASCII quote byte can never
collide with a UTF-8 continuation byte — purely a silent change in returned
byte content whenever a literal contained malformed UTF-8 and took the fast
path.

Fixed by validating the fast-path slice with `utf8.ValidString` before
returning it, falling back to the sanitizing slow path only when the content
is genuinely malformed:

```go
if content := lx.s[contentStart:i]; utf8.ValidString(content) {
    lx.pos = i + 1
    return token{Typ: tString, Val: content, Pos: start}
}
break // malformed UTF-8 -- fall back to the slow path to sanitize it
```

`utf8.ValidString` is a single allocation-free linear scan, cheaper than the
old decode-then-rebuild path it replaces, so this costs nothing in the
overwhelmingly common case (valid UTF-8, no escape): the four benchmarks'
allocation counts are bit-for-bit identical before and after this fix. Two
new tests (`TestLexerStringLiteralMalformedUTF8`,
`TestLexerQuotedIdentifierMalformedUTF8`) pin the old and new lexer to agree
on malformed input (lone continuation bytes, invalid lead bytes, truncated
multi-byte sequences, both standalone and embedded mid-literal).

```sh
go test ./internal/engine/... -run 'TestLexer' -count=1 -v
go test ./internal/engine/... -count=1
go test ./internal/engine/... -run '^$' -bench 'BenchmarkParseSimpleSelect|BenchmarkParseComplexSelect|BenchmarkParseBulkInsert|BenchmarkLexerNextTokenOnly' -benchmem -benchtime=300x
```
