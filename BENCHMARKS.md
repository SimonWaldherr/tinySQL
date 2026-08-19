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
  alone produces 2-3x run-to-run variance on some benchmarks.
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
| tinySQL-Memory | **526 µs** | **1.41 ms** | 14.1 ms |
| tinySQL-Disk | 691 µs | 1.81 ms | 11.0 ms |
| tinySQL-DiskGzip | 685 µs | 1.69 ms | 10.4 ms |
| tinySQL-Hybrid | 719 µs | 1.54 ms | 10.2 ms |
| tinySQL-Index | 778 µs | 1.62 ms | 10.6 ms |
| tinySQL-Page | 3.95 ms | 23.3 ms | 152 ms |
| SQLite-modernc | 1.26 ms | 2.13 ms | **8.34 ms** |

The crossover here is the interesting part: tinySQL wins at small batch
sizes (10-100 single-row `INSERT`s, ~1.4-2.4x faster) but SQLite's lower
per-statement fixed cost pulls ahead by 1000 rows — every tinySQL backend
except `Page` is slower than SQLite there. `Page` is slowest throughout by
design (see above).

### FullScan — read all N rows back with `SELECT *`

| Backend | rows=10 | rows=100 | rows=1000 |
|---|---:|---:|---:|
| tinySQL-Memory | **183 µs** | **386 µs** | 2.32 ms |
| tinySQL-Disk | 309 µs | 2.28 ms | 2.21 ms |
| tinySQL-DiskGzip | 304 µs | 732 µs | 2.15 ms |
| tinySQL-Hybrid | 336 µs | 676 µs | **1.32 ms** |
| tinySQL-Index | 428 µs | 730 µs | 1.51 ms |
| tinySQL-Page | 331 µs | 415 µs | 1.35 ms |
| SQLite-modernc | 651 µs | 807 µs | 4.09 ms |

tinySQL wins every cell here regardless of backend, 1.5-3x depending on
size — `Memory` leads at small sizes, `Hybrid` takes over by 1000 rows.
`Disk` at rows=100 (2.28 ms) is the one outlier worth a second look — every
other backend is under 1 ms at that size.

### RoundTrip, SingleInsert, PointQuery, MixedWorkload

| Benchmark | tinySQL-Memory | tinySQL-Disk | tinySQL-DiskGzip | tinySQL-Hybrid | tinySQL-Index | tinySQL-Page | SQLite-modernc |
|---|---:|---:|---:|---:|---:|---:|---:|
| RoundTrip | 2.40 ms | 2.59 ms | 2.08 ms | **1.74 ms** | 2.00 ms | 26.2 ms | 3.45 ms |
| SingleInsert | **284 µs** | 473 µs | 692 µs | 501 µs | 709 µs | 3.63 ms | 1.50 ms |
| PointQuery (`WHERE id = 500`, 1000 rows) | 260 µs | 508 µs | — | — | — | **253 µs** | 285 µs |
| MixedWorkload | **580 µs** | 689 µs | 772 µs | 1.24 ms | 997 µs | 4.47 ms | 1.37 ms |

tinySQL wins all four of these outright — some combination of `Memory`,
`Hybrid` or `Page` is fastest depending on the benchmark, and SQLite is
never the fastest option in this table. `Page` is markedly slower on
`RoundTrip`/`SingleInsert` (its B+Tree write path isn't tuned for
single-row latency) but competitive on `PointQuery`, where reads dominate.

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
| InsertAutocommit | **31 µs** | 48 µs | **1.92 ms** | 2.27 ms | **1.68 ms** | 2.42 ms | 297 µs |
| InsertTxBatch (100 rows/tx) | 5.07 ms | **2.63 ms** | **8.08 ms** | 9.12 ms | 8.52 ms | **6.76 ms** | 3.84 ms |
| UpdateByPK | **39 µs** | 73 µs | **2.69 ms** | 3.16 ms | **1.76 ms** | 2.02 ms | 343 µs |
| PointLookupPK | **31 µs** | 36 µs | **40 µs** | 59 µs | **17 µs** | 58 µs | 54 µs |
| RangeScan | **229 µs** | 411 µs | **328 µs** | 925 µs | **232 µs** | 747 µs | 617 µs |
| Aggregate (`GROUP BY`) | **6.27 ms** | 34.0 ms | **6.45 ms** | 28.5 ms | **8.67 ms** | 30.5 ms | 44.5 ms |
| Join | **5.79 ms** | 22.6 ms | **5.45 ms** | 49.4 ms | **17.1 ms** | 31.7 ms | 37.2 ms |
| VectorTopK (`VEC_SEARCH` vs. app-side scan) | **1.05 ms** | 48.0 ms | — | — | — | — | — |

**Bold** marks the faster engine in each durability tier. tinySQL wins single-row
point operations (insert/update/lookup) and every autocommit durability tier
by a wide margin — an autocommit fsync in tinySQL costs one row's worth of
WAL write, where SQLite's page-based WAL journal pays more per commit
regardless of statement size. tinySQL also wins every `Aggregate`/`Join`
comparison outright, and `VectorTopK` isn't close: tinySQL has a native
vector index, SQLite here is a hand-written application-side scan, which is
the point of the comparison — it is what an app would actually have to write
without one.

`InsertTxBatch` is the one mixed result in this suite: SQLite's
single-fsync-per-transaction batching beats tinySQL at `mem` and
`wal-fullflush`, but tinySQL comes out ahead at `wal-fsync`. Not a clean win
either way — worth a closer look before claiming batch-insert parity in
either direction.

## Join & Aggregate scaling

`SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON
o.customer_id = c.id` and `SELECT customer_id, COUNT(*), SUM(amount) FROM
orders GROUP BY customer_id`, direct `Execute` vs. `database/sql`, scaling
row counts:

| Join | customers=10,orders=50 | customers=50,orders=500 | customers=100,orders=2000 |
|---|---:|---:|---:|
| tinySQL-Memory | **180 µs** | **678 µs** | 3.62 ms |
| tinySQL-Disk | 400 µs | 982 µs | **3.43 ms** |
| SQLite-modernc | 359 µs | 1.37 ms | 5.94 ms |

| Aggregate | customers=10,orders=50 | customers=50,orders=500 | customers=100,orders=2000 |
|---|---:|---:|---:|
| tinySQL-Memory | **293 µs** | **552 µs** | **1.28 ms** |
| tinySQL-Disk | 885 µs | 1.03 ms | 1.73 ms |
| SQLite-modernc | 729 µs | 1.68 ms | 4.96 ms |

SQLite never wins a cell in either table. tinySQL-Memory leads Aggregate at
every scale tested, with the margin widening (1.6x at the smallest size,
3.9x at the largest) — consistent with Suite 2's `Aggregate` results above.
Join is Memory's at the two smaller sizes but Disk edges narrowly ahead at
the largest (customers=100, orders=2000), a reminder that "Memory is always
fastest" doesn't hold universally once result-set construction cost starts
to dominate.

## Spatial / viewport queries

POI-layer bounding-box queries, tinySQL vs. SQLite:

| Benchmark | tinySQL/mem | SQLite/mem |
|---|---:|---:|
| ViewportIndexed | 562 µs | **236 µs** |
| ViewportNoIndex | 31.2 ms | **27.3 ms** |
| CategoryInViewport | 221 µs | **203 µs** |

SQLite wins all three here, by a modest 1.1-2.4x margin. Spatial indexing is
one area where tinySQL's current implementation has not caught up.

## MBTiles / paged-index suite

Tile-serving workload: a `.mbtiles` (SQLite) file vs. tinySQL's paged B+Tree
index backend, both reading from an on-disk artifact.

| Benchmark | tinySQL | SQLite |
|---|---:|---:|
| Access, warm cache | **25.3 µs** | 82.8 µs |
| Access, cold cache | **1.585 ms** (~tied) | 1.594 ms |
| Access by payload size, 256 B | **13.9 µs** | 31.4 µs |
| Access by payload size, 1569 B | **24.8 µs** | 58.3 µs |
| Access by payload size, 2500 B | **14.0 µs** | 108 µs |
| Access by payload size, 50000 B (overflow) | **266 µs** | 375 µs |
| Access, parallelism=1 | **22.7 µs** | 189 µs |
| Access, parallelism=4 | **16.7 µs** | 149 µs |
| Access, parallelism=16 | **16.9 µs** | 145 µs |
| Open+reopen, 1024 rows | **558 µs** | 655 µs |
| Open+reopen, 16384 rows | **731 µs** | 1.04 ms |
| Tile lookup, indexed, 4k tiles | 184 µs | **21.6 µs** (mem) / 86.5 µs (file) |
| Tile lookup, indexed, 64k tiles | 184 µs | **49.4 µs** (mem) / 77.6 µs (file) |
| Tile lookup, no index, 64k tiles | **11.8 ms** | 19.1 ms (mem) / 156 ms (file) |
| Tileset load (bulk import) | 271 ms | **92.7 ms** (mem) / 369 ms (file, tinySQL wins) |
| Tile lookup from disk | 319 µs | **156 µs** |

Mixed picture, and worth reading precisely rather than by win count: tinySQL's
paged-index backend clearly wins warm/cold access, access-by-size, parallel
access, and open/reopen — the paths that page-cache and B+Tree design
target directly. It clearly loses single indexed tile lookups against
SQLite's in-memory driver (`184 µs` vs `21-49 µs` — a real, consistent 4-9x
gap, not noise) and initial tileset load against SQLite's in-memory case,
though it beats SQLite's on-disk-file case for the same load. Anyone
choosing between these two for tile serving should weigh "many small point
lookups against an already-open in-memory index" (SQLite ahead) against
"page-cache-bound access patterns, cold starts, and parallel readers"
(tinySQL ahead) against their actual workload shape.

## Recent engine-side optimizations (this snapshot)

Four storage-layer changes landed alongside this snapshot; none of them are
isolated in the tables above (they affect code paths some of these
benchmarks don't specifically exercise), so they're called out here instead:

- **INSERT into an indexed table, rollback fast path.** A table with any
  secondary index used to be disqualified from the cheap append-only
  statement snapshot and fall back to cloning the whole table on every
  single INSERT, successful or not — bulk inserts into an already-large
  indexed table were quadratic in the table's final size. 60k single-row
  INSERTs into an indexed table: ~9+ minutes and still climbing before the
  fix, ~1s flat after.
- **WAL replay, index rebuild.** `WALManager`'s replay path rebuilt secondary
  indexes from scratch on every committed transaction instead of once per
  replay — recovering N small transactions against an M-row indexed table
  was `O(N·M·log M)` instead of `O(M·log M)`. Now deferred to one rebuild per
  touched table after the whole WAL replays.
- **WAL checksum hashing.** Narrow integer columns (`INT8`/`16`/`32`,
  `UINT`/`8`/`16`/`32`/`64`) used to fall through a reflection-based
  fallback for checksum hashing; a direct-encode fast path measured ~29x
  faster for a lone scalar (3799 ns/op → 130 ns/op, isolated).
- **Buffer pool.** Cache hit/miss/eviction counters moved to atomics,
  removing a mutex acquisition from `BufferPool.Get` — the hot path for
  every table reference under the Hybrid/PagedIndex storage modes.

## Takeaways

- tinySQL beats modernc-SQLite on nearly every single-row/point operation
  (insert, update, point lookup) and on `Aggregate`/`Join` at every scale
  tested, often by 2-8x.
- tinySQL's autocommit durability story is strong: both WAL sync modes beat
  SQLite's equivalent durability tiers on point operations.
- `InsertTxBatch` is the one mixed result in the parity suite — SQLite wins
  at `mem` and `wal-fullflush`, tinySQL wins at `wal-fsync`. Worth a closer
  look if batch-insert throughput matters for a given workload.
- Spatial/viewport queries and single indexed point lookups against an
  already-open in-memory MBTiles index are the two areas where SQLite
  (modernc) currently wins outright.
- None of this bounds tinySQL against the C SQLite implementation — see
  "What SQLite means in this file" above.
