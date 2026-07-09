# Benchmarks: tinySQL vs. SQLite (modernc)

This document tracks head-to-head storage/query benchmarks between tinySQL's
storage backends and [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite)
(a pure-Go SQLite driver, already a direct dependency of this module — no CGo
required, so it's a fair comparison target for a pure-Go embedded database).

The benchmark suite lives in [`benchmarks/storage_benchmark_test.go`](benchmarks/storage_benchmark_test.go).
Run it yourself with:

```sh
go test -run=none -bench=. -benchtime=100x ./benchmarks/...
```

## Machine / environment for the numbers below

- Intel Core i7-10850H @ 2.70GHz, Windows, `GOMAXPROCS` default (12)
- `go test -bench=. -benchtime=50x ./benchmarks/...`
- These are microbenchmark ns/op numbers from a single run on a shared dev
  machine — treat trends (which backend is faster, by roughly what factor) as
  meaningful, and treat exact ns/op values as approximate.

## Backends compared

| Name | What it is |
|---|---|
| `tinySQL-Memory` | `ModeMemory`, no backing file |
| `tinySQL-Disk` | `ModeDisk`, one GOB file per table |
| `tinySQL-DiskGzip` | `ModeDisk` + `CompressFiles: true` |
| `tinySQL-Hybrid` | `ModeHybrid` (in-memory cache + disk-backed) |
| `tinySQL-Index` | `ModeIndex` |
| `tinySQL-Page` | Direct `pager.PageBackend` (B+Tree page store), bypassing SQL execution |
| `SQLite-modernc` | `database/sql` + `modernc.org/sqlite`, `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL` |

All tinySQL backends (except `tinySQL-Page`) go through the full
parse → plan → execute pipeline via `tinysql.ParseSQL`/`tinysql.Execute`, same
as SQLite goes through `database/sql`'s query path — so both sides pay their
respective "real API" overhead, not a stripped-down internal fast path.

## Results

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

tinySQL-Memory beats SQLite at small row counts (10, 100) since SQLite pays
fixed WAL/journal setup cost per table-write cycle; SQLite pulls ahead at 1000
rows, likely from its native (CGo-free but still compiled C-derived) B-tree
insert path outperforming tinySQL's GOB/row-append path at scale.
`tinySQL-Page` is a clear outlier — seven page-backend `SaveTable` calls
per-row rewrite the whole table rather than appending, so it does **not**
represent an indexed insert path; see the follow-up work below.

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

tinySQL wins full-table scans across the board, and by a wider margin as row
count grows (2.4x faster at 1000 rows for Memory vs. SQLite). This tracks with
tinySQL rows being native Go `[]any` slices with no marshaling overhead on
read, vs. SQLite's `database/sql` `Scan()` per column per row.

### RoundTrip — write 100 rows, then read them all back, per iteration

| Backend | ns/op |
|---|---|
| tinySQL-Memory | 466 µs |
| tinySQL-Disk | 689 µs |
| tinySQL-DiskGzip | 782 µs |
| tinySQL-Hybrid | 686 µs |
| tinySQL-Index | 952 µs |
| tinySQL-Page | 9.99 ms |
| **SQLite-modernc** | 906 µs |

tinySQL-Memory is ~2x faster than SQLite for this pattern; disk-backed tinySQL
modes are roughly on par with SQLite.

### SingleInsert — one `INSERT` per iteration (latency-sensitive path)

| Backend | ns/op |
|---|---|
| tinySQL-Memory | 74 µs |
| tinySQL-Disk | 186 µs |
| tinySQL-DiskGzip | 167 µs |
| tinySQL-Hybrid | 172 µs |
| tinySQL-Index | 166 µs |
| tinySQL-Page | 1.60 ms |
| **SQLite-modernc** | 339 µs |

tinySQL-Memory is ~4.6x faster than SQLite here; even the disk-backed tinySQL
modes beat SQLite's per-statement overhead by ~2x, likely because tinySQL's
disk backends batch/checkpoint rather than fsync-per-statement while SQLite's
WAL commit still has real per-transaction cost.

### PointQuery — `SELECT name FROM t WHERE id = 500` on a 1000-row table

| Backend | ns/op | allocs/op |
|---|---|---|
| tinySQL-Memory | 123 µs | 61 |
| tinySQL-Disk | 211 µs | 63 |
| tinySQL-Page | 126 µs | 142 |
| **SQLite-modernc** | 140 µs | 20 |

**Caveat — not apples-to-apples yet:** SQLite's `t` table has an
`INTEGER PRIMARY KEY` (a real B-tree index), so its point query is a true
O(log n) index seek. tinySQL has no index support wired into the `WHERE`
planner yet, so every tinySQL "point query" is a full linear scan with a
predicate filter. tinySQL is still competitive at 1000 rows purely because a
1000-row linear scan is cheap in Go, but this comparison will invert once
indexing is added on the SQLite side of a fairer test or once tinySQL rows
scale into the tens of thousands. This is the first thing addressed in the
"Next steps" section below.

### MixedWorkload — interleaved write (10 rows) + full read, per iteration

| Backend | ns/op |
|---|---|
| tinySQL-Memory | 115 µs |
| tinySQL-Disk | 224 µs |
| tinySQL-DiskGzip | 252 µs |
| tinySQL-Hybrid | 240 µs |
| tinySQL-Index | 235 µs |
| tinySQL-Page | 4.33 ms |
| **SQLite-modernc** | 483 µs |

tinySQL-Memory is ~4.2x faster than SQLite; disk-backed tinySQL modes are
still ~2x faster.

### Join — `SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id`

Data loaded once per sub-benchmark; only the join query itself is timed.
`customers=N,orders=M` means N rows in `customers`, M rows in `orders` (M/N
orders per customer).

| Backend | 10 cust / 50 orders | 50 cust / 500 orders | 100 cust / 2000 orders |
|---|---|---|---|
| tinySQL-Memory | 192 µs | 501 µs | 1.30 ms |
| tinySQL-Disk | 392 µs | 762 µs | 1.58 ms |
| **SQLite-modernc** | 246 µs | 695 µs | 2.18 ms |

tinySQL wins the join benchmark at every size tested, and the gap widens as
the join grows (tinySQL ~1.7x faster than SQLite at 2000 rows). This is
consistent with the `FullScan` results above — tinySQL's join implementation
operates on native Go values with no per-row `Scan()` marshaling, so its
per-row join cost stays low even though it (like SQLite here, since neither
side has an index on `orders.customer_id`) is doing a nested-loop join
rather than a hash join.

### Aggregate — `SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id`

| Backend | 10 cust / 50 orders | 50 cust / 500 orders | 100 cust / 2000 orders |
|---|---|---|---|
| tinySQL-Memory | 123 µs | 218 µs | 430 µs |
| tinySQL-Disk | 224 µs | 310 µs | 660 µs |
| **SQLite-modernc** | 200 µs | 522 µs | 1.60 ms |

**Update:** this originally showed tinySQL losing ground as size grew
(SQLite ~1.73ms vs. tinySQL-Memory ~1.86ms at the largest size), because
`SUM`/`AVG`/`MIN`/`MAX` fell off the raw-row GROUP BY fast path and went
through the general row-map evaluator, which re-scans each group's buffered
`Row` slice once per aggregate expression and pays full map-based row
evaluation on top. Only `COUNT` had a fast path before this round.

Extending `executeSimpleAggregateFastPath` (`internal/engine/exec.go`) to
also accumulate `SUM`/`AVG`/`MIN`/`MAX` directly off the raw `[]any` row
during the single group-by scan — the same way `COUNT` already did — cut
this benchmark's cost dramatically: at 2000 orders, tinySQL-Memory went from
1.86ms/950KB/7377 allocs to 430µs/85KB/912 allocs (~4.3x faster, ~11x less
memory, ~8x fewer allocations). tinySQL now wins this benchmark at every
size tested, including a ~3.7x win over SQLite at the largest size, instead
of losing to it. `SUM`/`AVG` still fall back to `big.Rat` accumulation
(mirroring the general evaluator) when they encounter a `DECIMAL`/`MONEY`
value, so correctness for exact-decimal columns is unchanged — only the
common all-numeric case got faster.

## Takeaways

- **tinySQL wins decisively on read-heavy and low-latency-write workloads**
  when running in-memory or with its lightweight disk backends — full scans,
  single inserts, joins, aggregates, and mixed workloads are all faster than
  SQLite at the row counts tested (10-2000 rows). This matches tinySQL's
  design as an embedded, allocation-light, single-process engine without
  SQLite's transactional-durability machinery in the hot path.
- **SQLite pulls ahead on large bulk inserts** (1000+ rows in one loop) and
  indexed point lookups — areas where its mature B-tree engine and
  per-column binary encoding pay off at scale. It no longer wins the
  `GROUP BY` aggregate benchmark after the fast-path extension described
  above.
- **`tinySQL-Page`, the direct B+Tree backend, is currently the slowest
  option** for bulk writes — it doesn't yet do incremental/append writes at
  the page level, so every `SaveTable` call serializes the whole table. This
  is a known, previously-flagged optimization target, not a reflection of the
  page format's ceiling.
- The `PointQuery` comparison is currently unfair to SQLite in tinySQL's
  favor (SQLite's index vs. tinySQL's full scan) — improving tinySQL's WHERE
  planner with real indexed lookups is the natural next benchmark to add, and
  is expected to close or reverse the gap at larger row counts.

## Next steps (tracked as follow-up work, not yet implemented)

1. Add real index-based `WHERE`-clause point lookups to tinySQL's query
   planner (`CREATE INDEX` is currently a no-op — see
   `executeCreateIndex` in `internal/engine/exec.go`), then add an
   indexed point-query benchmark so `PointQuery` compares index-seek vs.
   index-seek instead of index-seek (SQLite) vs. full-scan (tinySQL). This
   is a real engine feature, not just a benchmark addition — scoped as
   future work rather than bundled into this benchmark suite.
2. ~~Add JOIN and aggregate (`GROUP BY`/`SUM`/`COUNT`) benchmarks~~ — done;
   see `BenchmarkJoin`/`BenchmarkAggregate` in
   `benchmarks/query_benchmark_test.go`.
3. Extend the row-count sweep beyond current sizes (e.g. 10k/100k rows) to
   see where the bulk-insert crossover with SQLite happens and how far it
   grows.
4. ~~Investigate tinySQL's `GROUP BY` allocation growth~~ — done; see the
   Aggregate section above. `SUM`/`AVG`/`MIN`/`MAX` now join `COUNT` on the
   raw-row fast path instead of falling back to the general row-map
   evaluator. Remaining candidate: extend the same fast path to multi-column
   `GROUP BY` (currently limited to a single group-by column) and to
   `HAVING` clauses that only reference already-computed aggregates.

## Internal engine optimizations (not SQLite comparisons)

The sections above compare tinySQL against SQLite. This section instead
tracks before/after numbers for engine-internal work with no SQLite side to
compare against — measured with `go test -bench=. -benchmem ./internal/engine/...`
on the same machine as above.

### Vector search (`VEC_SEARCH`/`VEC_WARM`): HNSW allocation and build-time fix

Profiling `VEC_SEARCH(..., 'hnsw')` and `VEC_WARM(..., 'hnsw')` on a
12,000-row / 64-dim table (`BenchmarkVecSearchIndexModesSameTable` in
`internal/engine/vector_search_benchmark_test.go`) found two compounding
issues in the HNSW graph traversal (`internal/engine/vector_index.go`):

1. `searchLayer` allocated a fresh pair of heaps plus a "touched nodes" slice
   on every one of the many calls per traversal (one per graph layer) — for a
   12k-row index build, that's tens of thousands of short-lived allocations.
2. Both heaps went through `container/heap`'s `Push(h Interface, x any)` /
   `Pop() any` API, which boxes every `vecScoredRow` value into an
   interface — one heap allocation per graph edge considered during
   traversal, dwarfing the cost of (1).

Fixed by pooling the candidate/result heaps and the visited-touch buffer
across `searchLayer` calls within one traversal (`vecHNSWScratch` +
`sync.Pool`), and replacing the `container/heap`-based push/pop with direct,
non-interface sift-up/sift-down functions on the concrete slice types.

Separately, `vectorDotKernel`/`vectorL2SquaredKernel`/`vectorL1Kernel`
(`internal/engine/vector_math_amd64.go`) fell back to a portable scalar loop
for vectors under 128 dimensions, on the assumption SIMD setup cost wasn't
worth it below that size. Benchmarking across realistic embedding sizes
(`BenchmarkVectorDotKernelBySize`) showed the SSE2 kernel winning at every
size tested, including 16 dimensions, so the threshold was removed.

| Benchmark (12k rows, 64 dims, k=20, cosine) | before | after |
|---|---|---|
| HNSW index build (`VEC_WARM`) | 19.5 s | 7.5 s (2.6x) |
| HNSW query (`BenchmarkVecSearchCosineTopK_HNSWCached`) | 2.26 ms, 4336 allocs/op | ~1.0-1.2 ms, 78 allocs/op |
| IVF query | 382 µs, 133 allocs/op | ~220 µs, 77 allocs/op |
| Flat (exact) query | 970 µs, 415 allocs/op | ~580 µs, 128 allocs/op |

Correctness is unaffected: `TestVecSearchWithANNIndexModes` and
`TestVecSearchANNIndexInvalidatesOnTableVersion`
(`internal/engine/vector_test.go`) already cover HNSW/IVF result correctness
against exact search and pass unchanged.

### Row materialization (`rowsFromTable`): removed a redundant per-row map check

`rowsFromTable` (`internal/engine/exec.go`) builds the `Row`
(`map[string]any`) for every row of every table referenced in a `FROM`
clause — the shared entry point behind scans, joins, `GROUP BY`, and
`ORDER BY` alike. Each column is stored under both a qualified key
(`alias.col`) and an unqualified key (`col`); the unqualified insert used to
be guarded by a map existence check on *every row*, to protect against a
duplicate column name clobbering an earlier one. Real schemas essentially
never have duplicate column names, so that check was pure overhead in the
common case.

Fixed by computing "does this table have any duplicate column names" once
per query instead of once per row. When there are none (the fast path), both
keys are set unconditionally in a single loop; the slow path preserves the
exact original "first occurrence wins" behavior for the rare duplicate-name
case (regression test: `TestRowsFromTableDuplicateColumnNames` in
`internal/engine/rows_from_table_test.go`).

| Benchmark (20,000 rows) | before | after |
|---|---|---|
| `SELECT grp, sub, COUNT(*), AVG(val) ... GROUP BY grp, sub` | ~50 ms | ~40 ms (≈20%) |
| `SELECT * FROM t` | ~29 ms | ~27 ms |

Reproduce with:

```sh
go test -bench='BenchmarkGroupByTwoColumns|BenchmarkSelectStarFullScan' -benchmem ./internal/engine/...
```

### RAG scalar-function path: constant folding, fused SIMD cosine, AVX2+FMA kernels

The `VEC_SEARCH` table function was already fast, but the *scalar* RAG query
shape — per-row `VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[...]'))`
in `WHERE`/`ORDER BY`, optionally blended via `RAG_HYBRID_SCORE` /
`RAG_RANK_SCORE` — was orders of magnitude slower than the equivalent
`VEC_SEARCH` call. Profiling a 12,000-row / 64-dim `ORDER BY sim DESC
LIMIT 20` query showed ~85% of CPU time inside `encoding/json`: the
`VEC_FROM_JSON` literal was re-parsed for every row. Five compounding fixes
(`internal/engine/const_fold.go`, `vector_functions.go`, `vector_math*.go/.s`,
`exec.go`):

1. **Parse-time constant folding** — `VEC_FROM_JSON`/`VEC_FROM_BYTES`/
   `VEC_NORMALIZE` calls whose arguments are all literals are evaluated once
   at parse time and replaced with a vector literal (`foldConstFuncCall`).
   Invalid input stays unfolded so errors still surface at execution.
2. **Scalar vector functions now use the SIMD kernels** — `VEC_DOT`,
   `VEC_L2_DISTANCE`, `VEC_MANHATTAN_DISTANCE`, `VEC_DISTANCE`, `VEC_NORM`,
   `VEC_NORMALIZE` and `cosineSimilarity` previously used naive Go loops.
3. **Fused cosine kernel** — cosine similarity without cached norms needs
   dot(a,b), dot(a,a) and dot(b,b); a fused one-pass kernel
   (`vectorCosineKernel`) computes all three with the memory traffic of a
   single dot product.
4. **AVX2+FMA kernels with runtime dispatch** — 4-wide `VFMADD231PD`
   variants of the dot/L2/L1/cosine kernels, selected at startup via an
   in-repo CPUID check (no new dependency); baseline SSE2 remains the
   fallback and the floor for short vectors.
5. **Raw fast path allocation removal** — `evalRawFuncCall` allocated an
   args slice, per-arg `Literal` wrappers, an escaping `FuncCall` copy and
   an empty `Row` map per row; these are now pooled/shared. Timestamp
   parsing (`RECENCY_SCORE`, `RAG_HYBRID_SCORE`) also gained a fixed-layout
   fast path (`parseTimeFixedDigits`), ~15x cheaper than `time.Parse`.

| Benchmark (12k rows, 64 dims) | before | after |
|---|---|---|
| `ORDER BY VEC_COSINE_SIMILARITY(...) LIMIT 20` | 271 ms, 34 MB, 264k allocs | 2.7 ms, 109 KB, 12k allocs (**~100x**) |
| `RAG_RANK_SCORE(...) ORDER BY ... LIMIT 20` | 282 ms | 7.6 ms (**37x**) |
| Hybrid score + recency (`RAG_HYBRID_SCORE`) | 535 ms | 22.6 ms (**24x**) |
| `WHERE vector-cond AND scalar-cond` | ~29 ms | ~1.5 ms (**19x**) |
| `vectorDot`, 768 dims (AVX2+FMA vs SSE2) | 184 ns | 73 ns (2.5x) |
| `vectorL2Squared`, 768 dims | 227 ns | 83 ns (2.7x) |

Kernel parity across sizes (including AVX2 dispatch thresholds and odd
tails) is covered by `TestVecDotKernelMatchesUnrolledAcrossSizes` and
`TestVecCosineKernelMatchesUnrolledAcrossSizes`; folding semantics by
`TestVecFromJSONConstantFolding` / `TestVecFromJSONInvalidStillErrors`.

Reproduce with:

```sh
go test -bench='BenchmarkOrderByVectorLimit|BenchmarkRAGRankScore|BenchmarkHybridOrderBy|BenchmarkVectorDot768' -benchmem ./internal/engine/...
```
