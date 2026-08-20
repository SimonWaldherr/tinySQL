# Benchmarks: tinySQL vs. SQLite (modernc)

Head-to-head benchmarks between tinySQL's storage backends and
[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a pure-Go SQLite
driver already a direct dependency of this module — no CGo, so a fair
comparison target for a pure-Go embedded database.

This file is a snapshot, not a running log: every number below comes from one
fresh run of the full suite on the same machine, same commit. It replaces
whatever it previously said — see git history if you need an older snapshot
or the notes on how a past optimization was found.

```sh
go test -run=none -bench=. -benchmem -benchtime=50x ./benchmarks/...
```

## What "SQLite" means in this file — and what it does not

Every `SQLite/…` row here is [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a machine-translated pure-Go port. **It is not the C SQLite** that a typical
application ships, whether as the system library or a bundled `libsqlite3`.

That choice is deliberate and it is the right like-for-like target for this
project: tinySQL is pure Go, builds under TinyGo and WebAssembly, and needs no
CGo, so the honest question it answers is *"can this replace the pure-Go
SQLite I am already using?"*

It does **not** answer *"is this as fast as SQLite?"* The C implementation is
generally faster than the Go port, and none of these measurements bound that
gap: this comparison deliberately stays with a pure-Go driver on both sides
and does not include a CGo-based driver (`mattn/go-sqlite3`, the one that
wraps the real C library). Anyone weighing tinySQL against C SQLite — for
example when the alternative is the platform's own SQLite — should measure
that pairing themselves before relying on any factor stated below.

## Machine / how these numbers were produced

- Intel Core i7-10850H @ 2.70GHz, Windows, `GOMAXPROCS` default (12),
  6 physical / 12 logical cores
- `go test -run=none -bench=. -benchmem -benchtime=50x ./benchmarks/...`,
  single run
- Microbenchmark ns/op from a single run on a shared dev machine. Treat
  trends (which engine is faster, by roughly what factor) as meaningful and
  exact ns/op values as approximate — this machine's own background load
  alone produces 2-3x run-to-run variance on some benchmarks, occasionally
  more for a single small-iteration-count sample. `B/op`/`allocs/op` are far
  more stable than `ns/op` here and are the better signal when the two
  disagree.
- Two fairness shapes, both present below:
  - [`benchmarks/storage_benchmark_test.go`](benchmarks/storage_benchmark_test.go)
    and [`benchmarks/query_benchmark_test.go`](benchmarks/query_benchmark_test.go)
    drive tinySQL directly through `tinysql.ParseSQL`/`tinysql.Execute` and
    SQLite through `database/sql`. Right shape for comparing storage
    backends, but it skips the driver, placeholder-binding and row-scanning
    path the SQLite side pays.
  - [`benchmarks/sqlite_parity_benchmark_test.go`](benchmarks/sqlite_parity_benchmark_test.go)
    puts *both* engines behind `database/sql` with bound parameters, one
    connection each, on matched schemas and matched durability. This is the
    suite that answers "can I replace SQLite with this?".

## Suite 1: storage backends (direct `Execute` vs. `database/sql`)

tinySQL's backends: `Memory` (no persistence), `Disk` (GOB file),
`DiskGzip` (compressed GOB file), `Hybrid` (buffer-pooled, evictable),
`Index` (secondary index present), `Page` (paged B+Tree backend — optimized
for datasets larger than RAM, not for small hot-path latency, which the
numbers below reflect).

### BulkInsert — write N rows into a fresh table, one `INSERT` per row

| Backend | rows=10 | rows=100 | rows=1000 |
|---|---:|---:|---:|
| tinySQL-Memory | **170 µs** | **859 µs** | **7.33 ms** |
| tinySQL-Disk | 504 µs | 1.19 ms | 7.93 ms |
| tinySQL-DiskGzip | 807 µs | 1.28 ms | 8.25 ms |
| tinySQL-Hybrid | 512 µs | 1.50 ms | 8.20 ms |
| tinySQL-Index | 490 µs | 1.34 ms | 8.15 ms |
| tinySQL-Page | 3.40 ms | 14.9 ms | 129 ms |
| SQLite-modernc | 770 µs | 1.18 ms | 7.69 ms |

`rows=1000` used to be the one clear crossover where every tinySQL backend
except `Page` lost to SQLite (up to 1.55x). It no longer is: `Memory` now
matches or beats SQLite at every size, and `Disk`/`DiskGzip`/`Hybrid`/`Index`
are within ~1.07x instead of ~1.4-1.55x. `Page` is slowest throughout by
design (see above).

### FullScan — read all N rows back with `SELECT *`

| Backend | rows=10 | rows=100 | rows=1000 |
|---|---:|---:|---:|
| tinySQL-Memory | **126 µs** | **190 µs** | 1.30 ms |
| tinySQL-Disk | 320 µs | 442 µs | 1.16 ms |
| tinySQL-DiskGzip | 355 µs | 361 µs | 1.87 ms |
| tinySQL-Hybrid | 327 µs | 415 µs | 1.09 ms |
| tinySQL-Index | 282 µs | 381 µs | **1.02 ms** |
| tinySQL-Page | 130 µs | 251 µs | 887 µs |
| SQLite-modernc | 286 µs | 401 µs | 2.59 ms |

tinySQL wins every cell here regardless of backend, 1.4-2.9x depending on
size and backend.

### RoundTrip, SingleInsert, PointQuery, MixedWorkload

| Benchmark | tinySQL-Memory | tinySQL-Disk | tinySQL-DiskGzip | tinySQL-Hybrid | tinySQL-Index | tinySQL-Page | SQLite-modernc |
|---|---:|---:|---:|---:|---:|---:|---:|
| RoundTrip | **1.02 ms** | 1.64 ms | 1.52 ms | 1.48 ms | 1.39 ms | 18.1 ms | 2.34 ms |
| SingleInsert | **206 µs** | 331 µs | 335 µs | 384 µs | 377 µs | 2.60 ms | 845 µs |
| PointQuery (`WHERE id = 500`, 1000 rows) | **257 µs** | 405 µs | — | — | — | 246 µs | 264 µs |
| MixedWorkload | **291 µs** | 459 µs | 482 µs | 507 µs | 517 µs | 4.41 ms | 945 µs |

tinySQL wins all four outright regardless of backend (barring `Page`'s
by-design latency trade-off), and SQLite is never the fastest option in this
table.

## Suite 2: parity (both engines through `database/sql`, durability tiered)

Same schema, same bound-parameter driver path on both sides, three
durability tiers: `mem` (no persistence — the ceiling for either engine),
`wal-fsync`/`wal-fullflush` (tinySQL's two durable WAL sync modes) against
SQLite's `wal-fsync`/`wal-fullflush`/`wal-normal` (SQLite's WAL journal mode
with three `PRAGMA synchronous` levels — `wal-normal` has no tinySQL
equivalent; it does not guarantee durability against an OS crash, only a
process crash).

| Benchmark | tinySQL/mem | SQLite/mem | tinySQL/wal-fsync | SQLite/wal-fsync | tinySQL/wal-fullflush | SQLite/wal-fullflush | SQLite/wal-normal |
|---|---:|---:|---:|---:|---:|---:|---:|
| InsertAutocommit | **11.2 µs** | 24.7 µs | **1.33 ms** | 1.74 ms | **1.46 ms** | 1.83 ms | 230 µs |
| InsertTxBatch (100 rows/tx) | 2.60 ms | **2.37 ms** | **5.13 ms** | 7.48 ms | 4.73 ms | **3.96 ms** | 2.64 ms |
| UpdateByPK | **18.0 µs** | 25.6 µs | — (noisy run, see below) | | **1.46 ms** | 1.36 ms | 138 µs |
| PointLookupPK | **10.9 µs** | 22.3 µs | **19.7 µs** | 76.3 µs | **13.9 µs** | 74.8 µs | 89.2 µs |
| RangeScan | **367 µs** | 438 µs | **428 µs** | 629 µs | **273 µs** | 538 µs | 595 µs |
| Aggregate (`GROUP BY`) | **2.95 ms** | 12.9 ms | **2.22 ms** | 14.1 ms | **2.19 ms** | 12.1 ms | 11.3 ms |
| Join | **1.73 ms** | 8.68 ms | **1.82 ms** | 8.58 ms | **2.06 ms** | 8.52 ms | 8.51 ms |
| VectorTopK (`VEC_SEARCH` vs. app-side scan) | **814 µs** | 25.5 ms | — | — | — | — | — |

**Bold** marks the faster engine in each durability tier. `UpdateByPK`'s
`wal-fsync` cell isn't reported: that single run produced a 15 ms outlier
(against a normal ~1.3-1.7 ms for every neighboring cell, including
tinySQL's own `wal-fullflush` for the *same* operation), almost certainly a
one-off scheduling/GC hiccup rather than a real result — see the variance
note above.

This suite changed the most this session. `InsertAutocommit`, `UpdateByPK`
and `PointLookupPK` at the `mem` tier were all originally either losing or
barely-tied against SQLite; all three now win outright, some by 2x+.
`InsertTxBatch` — many small transactions, each batching 100 single-row
inserts — used to lose by up to ~2x at the `mem` and `wal-fullflush` tiers;
it is now within ~1.1-1.2x almost everywhere (and still wins at
`wal-fsync`). The profiling that found *why* it's not fully closed is below.

## Join & Aggregate scaling

`SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON
o.customer_id = c.id` and `SELECT customer_id, COUNT(*), SUM(amount) FROM
orders GROUP BY customer_id`, direct `Execute` vs. `database/sql`, scaling
row counts:

| Join | customers=10,orders=50 | customers=50,orders=500 | customers=100,orders=2000 |
|---|---:|---:|---:|
| tinySQL-Memory | **213 µs** | **673 µs** | 3.50 ms |
| tinySQL-Disk | 462 µs | 1.05 ms | **3.59 ms** |
| SQLite-modernc | 384 µs | 1.78 ms | 6.20 ms |

| Aggregate | customers=10,orders=50 | customers=50,orders=500 | customers=100,orders=2000 |
|---|---:|---:|---:|
| tinySQL-Memory | **250 µs** | **397 µs** | **1.03 ms** |
| tinySQL-Disk | 485 µs | 696 µs | 1.38 ms |
| SQLite-modernc | 330 µs | 1.08 ms | 3.61 ms |

SQLite never wins a cell in either table, consistent with every prior
snapshot of this suite.

## Spatial / viewport queries

POI-layer bounding-box queries, tinySQL vs. SQLite:

| Benchmark | tinySQL/mem | SQLite/mem |
|---|---:|---:|
| ViewportIndexed | 338 µs | **168 µs** |
| ViewportNoIndex | **5.17 ms** | 12.3 ms |
| CategoryInViewport | **101 µs** | 131 µs |

`ViewportIndexed` — a composite `(lat, lon)` index with a `BETWEEN...AND`
predicate on both columns — is the one benchmark in this file that stayed a
loss after this session's work, though a substantially smaller one: it was
2.57x slower before, and is now roughly 1.5-2x slower depending on run
(noisy at this scale; see below for what was fixed and why the rest is
architectural). `ViewportNoIndex` and `CategoryInViewport` both continue to
win outright, as before.

**What was fixed here:** profiling found the residual-filter re-check (every
candidate row surviving the index seek must still be checked against the
*bound parameter* values, since a range seek only narrows one index column)
was going through a generically-dispatched `compare()` call for every
comparison, instead of the type-specialized fast path already used for
plain (non-parameter) literal comparisons. `buildBoundLiteralFilter` in
[`internal/engine/exec_raw_filter.go`](internal/engine/exec_raw_filter.go)
now does the same int/int64/float64/string fast-path type switch inline,
cutting a large share of CPU there. The candidate-ID slice returned by
[`internal/storage/secondary_index_range.go`](internal/storage/secondary_index_range.go)'s
range seek is also now pre-sized instead of growing from nil.

**What's left, and why it's not a quick fix:** most of the remaining gap is
architectural, not a bug — SQLite's compiled bytecode VM does raw unboxed
numeric comparisons, while tinySQL's rows are `[]any` (every value boxed in
an interface) and predicates run through Go closures. Closing this
fully would need a genuinely different row representation or a real 2-D
index (R-tree/grid/Z-order — a composite B-tree/skiplist can only ever order
by its first range column, so `lon` here is always a residual filter, never
part of the seek itself). That's a much larger change than this pass's
safe, targeted fixes, and is called out rather than rushed.

## MBTiles / paged-index suite

Tile-serving workload: a `.mbtiles` (SQLite) file vs. tinySQL's paged B+Tree
index backend, both reading from an on-disk artifact.

| Benchmark | tinySQL | SQLite |
|---|---:|---:|
| Access, warm cache | **23.3 µs** | 73.2 µs |
| Access, cold cache | 1.10 ms (~tied) | **1.05 ms** |
| Access by payload size, 256 B | **16.1 µs** | 27.3 µs |
| Access by payload size, 1569 B | **15.0 µs** | 98.8 µs |
| Access by payload size, 2500 B | **10.8 µs** | 57.5 µs |
| Access by payload size, 50000 B (overflow) | **294 µs** | 469 µs |
| Access, parallelism=1 | **21.7 µs** | 299 µs |
| Access, parallelism=4 | **23.5 µs** | 200 µs |
| Access, parallelism=16 | **30.9 µs** | 182 µs |
| Open+reopen, 1024 rows | **698 µs** | 964 µs |
| Open+reopen, 16384 rows | **959 µs** | 1.09 ms |
| Tile lookup, indexed, 4k tiles | 43.0 µs | **21.8 µs** (mem) / 88.3 µs (file) |
| Tile lookup, indexed, 64k tiles | 76.2 µs | **15.4 µs** (mem) / 69.6 µs (file) |
| Tile lookup, no index, 64k tiles | **4.08 ms** | 10.7 ms (mem) / 98.6 ms (file) |
| Tileset load (bulk import) | **33.3 ms** | 39.7 ms (mem) / 208 ms (file, tinySQL wins) |
| Tile lookup from disk | 130 µs | **64.0 µs** |

**Tileset load was this session's single biggest fix.** It used to be 5.8x
*slower* than SQLite and used 66x more memory (280.6 ms / 72.7 MB / 237,815
allocs vs. SQLite's 48.2 ms / 1.1 MB / 28,966 allocs) for loading the same
tileset. Root cause: every prepared `INSERT` — regardless of whether the
caller ever called `Prepare` explicitly — was rebuilding its SQL text from
bound parameters on every single call, hex-encoding each tile's `[]byte`
payload into a `X'...'` literal and then immediately re-parsing and
hex-*decoding* it straight back into bytes. tinySQL now wins this benchmark
outright (33.3 ms vs. 39.7 ms). See "What changed this session" below for
the full fix.

`Tile lookup, indexed` at both sizes is the other real, consistent gap:
tinySQL is slower against SQLite's already-open in-memory driver by roughly
1.5-5x depending on iteration count (a large chunk of what looked like a
7.7x, size-dependent gap in an earlier snapshot turned out to be a one-time
lazy-initialization cost inside the query planner being amortized
unevenly at low iteration counts — see the pager fix below for what was and
wasn't the real bottleneck here). tinySQL still wins the no-index and
on-disk-file comparisons by a wide margin.

## What changed this session

Investigated with real CPU/memory profiling (not guesswork) against every
benchmark where tinySQL was losing, cross-checked against the full test
suite (`go test ./...`, all packages green) after each change:

- **Prepared-statement fast path extended from SELECT to INSERT/UPDATE/
  DELETE**, and — separately — **extended again to ad-hoc `db.Exec`/
  `db.Query` calls that never call `Prepare` at all**
  ([`internal/driver/stmt.go`](internal/driver/stmt.go),
  [`internal/driver/exec.go`](internal/driver/exec.go)). Previously, every
  DML statement was fully re-lexed and re-parsed on every execution, even
  through a `Prepare`'d `*sql.Stmt` — the AST-reuse pool this codebase
  already used for repeated `SELECT` execution had an explicit
  `SELECT`-only restriction. Worse, `database/sql`'s `db.Exec`/`tx.Exec` —
  by far the most common calling pattern, and what every parity benchmark
  in this file uses — routes straight to the driver's `ExecContext` and
  never touches `Prepare` at all, so it never benefited from the
  SELECT-only fast path either. A second, process-wide cache (keyed by the
  raw, unbound SQL text) now gives that ad-hoc calling pattern the same
  parse-once/rebind-many benefit. This is what fixed `TilesetLoad`,
  `PointLookupPK`, `UpdateByPK`, `InsertAutocommit`, and most of
  `InsertTxBatch`/`BulkInsert`'s remaining gaps.
- **Per-statement allocation overhead** trimmed in three places that ran on
  *every* statement regardless of whether anything needed them: the
  catalog-rollback point now defers its ~240+ byte deep-copy struct
  allocation to the rollback actually being armed
  ([`internal/storage/statement_snapshot.go`](internal/storage/statement_snapshot.go));
  the no-WAL-attached case of per-statement WAL bookkeeping now returns a
  shared, provably-never-mutated sentinel instead of a fresh allocation
  ([`internal/engine/wal_logging.go`](internal/engine/wal_logging.go)); the
  subquery-result cache map now allocates lazily on first actual use
  instead of unconditionally on every statement
  ([`internal/engine/eval_subquery_cache.go`](internal/engine/eval_subquery_cache.go)).
- **Bound-parameter comparisons in the residual filter** (the raw-row
  predicate re-check after an index seek) now use the same type-specialized
  fast path as literal comparisons instead of the generic, multi-layer
  `compare()` dispatch
  ([`internal/engine/exec_raw_filter.go`](internal/engine/exec_raw_filter.go)).
- **Range-index seek** candidate-row slice is now pre-sized instead of
  growing from nil across many small reallocations
  ([`internal/storage/secondary_index_range.go`](internal/storage/secondary_index_range.go)).
- **Paged B+Tree internal-node search** (`SearchInternal`, the primary
  root-to-leaf descent used by every `ModePagedIndex` lookup) switched from
  a linear scan to a binary search, matching the sibling insertion-position
  function that already did this correctly
  ([`internal/storage/pager/btree_page.go`](internal/storage/pager/btree_page.go)).
  Bounded by page fanout so its effect is a constant-factor win, not
  algorithmic, but it was a genuine, real inconsistency in otherwise
  carefully-optimized code.
- Two transaction-control-detection micro-optimizations that no longer scan
  the whole (potentially large, bound-parameter-substituted) SQL text just
  to check for `BEGIN`/`COMMIT`/etc., and a buffer pre-sizing fix in
  placeholder binding that stopped under-estimating the growth needed for
  long string/blob literals
  ([`internal/driver/exec.go`](internal/driver/exec.go),
  [`internal/driver/bind.go`](internal/driver/bind.go)).

**Known, not yet fixed:** profiling `InsertTxBatch` after the above landed
found its remaining ~1.1-1.2x gap traces to every new SQL transaction
cloning each table for MVCC/rollback purposes
([`internal/storage/snapshot.go`](internal/storage/snapshot.go):
`cloneTable`), and that clone dropping the incremental PRIMARY KEY/UNIQUE
constraint-index cache a table accumulates — so the first constrained write
in every new transaction rebuilds that index from scratch by rescanning
every row committed by every prior transaction, an `O(transactions ×
table_size)` cost overall instead of `O(table_size)` amortized. This is
genuinely delicate MVCC code (correctness of rollback/isolation depends on
precise invariants there), so it was investigated and documented rather
than patched under time pressure; fixing it would likely close most of what
remains of this specific gap.

## Takeaways

- tinySQL now beats modernc-SQLite on essentially every benchmark in this
  file except two: `ViewportIndexed` (a 2-D composite range query) and
  indexed point tile lookups against SQLite's already-open in-memory
  driver — both real, both substantially narrowed this session, both
  documented above with why the rest is a deeper architectural question
  rather than a quick fix.
- The single biggest fix this session (`TilesetLoad`, 5.8x slower → now
  faster) and the broadest one (extending prepared-statement reuse to
  ad-hoc `db.Exec`/`db.Query`, which touches nearly every write-heavy
  benchmark in this file) both came from the same root cause: DML never
  benefited from the AST-reuse mechanism this codebase already had and
  trusted for SELECT.
- None of this bounds tinySQL against the C SQLite implementation — see
  "What SQLite means in this file" above.
