# Transaction twins: reuse row and index buffers (2026-11-11)

Since [BEGIN stopped copying the database](transaction-snapshot-performance.md),
a transaction copies a table only when it first writes it. That copy was still
proportional to the table: 24 bytes of row header per row, plus a full copy of
every secondary-index skip list the transaction changed. A transaction that
updated one row of a 100,000-row table allocated 2.6 MB; one that inserted a row
into an indexed table of that size allocated 3.5 MB.

The table a commit replaces is now recycled instead of discarded. Its row-header
array and its skip lists are brought up to date with the committed state and
kept as *twins*; the next transaction that writes the table is handed the twins
as its private copy. The cost of a transaction's first write is the number of
rows recent transactions changed, not the number of rows the table holds.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, in-memory database through
`database/sql`, one `BEGIN … COMMIT` per operation,
`benchmarks/tx_table_size_benchmark_test.go` and `BenchmarkParityInsertTxBatch`
with `-benchtime=3000x`. Before is `0da0fc8` in a clean worktree, after is this
change, same flags.

| Benchmark (tinySQL/mem) | Before | After | Bytes/op before → after |
|---|---:|---:|---:|
| UPDATE one row by PK, 1,000 rows | 18–22 µs | 14–15 µs | 64 KB → 37 KB |
| UPDATE one row by PK, 10,000 rows | 51–52 µs | 12–14 µs | 299 KB → 37 KB |
| UPDATE one row by PK, 100,000 rows | 305–346 µs | 12 µs | 2.6 MB → 38 KB |
| INSERT one row, 1,000 rows | 51–52 µs | 14 µs | 133 KB → 32 KB |
| INSERT one row, 10,000 rows | 92–99 µs | 13 µs | 440 KB → 32 KB |
| INSERT one row, 100,000 rows | 443–502 µs | 17–18 µs | 3.5 MB → 76 KB |
| 1,000 INSERTs in one transaction (growing table) | 678–769 µs | 268–279 µs | 4.2 MB → 367 KB |

SQLite on the same machine: 6–8 µs for the single-row transactions and about
0.46 ms for the 1,000-row batch. Read-only transactions (5.5–6.8 µs), autocommit
INSERT (1.3 µs), UPDATE by PK (2.0 µs) and point lookups (1.7 µs) are unchanged.

A confirmation run on the combined 2026-11-11 tree with GOMAXPROCS=12 and
3,000 operations measured UPDATE at 12.9 µs for both 1,000 and 100,000 rows,
allocating 37,030 and 37,812 bytes/op. INSERT measured 14.7 and 18.2 µs,
allocating 31,701 and 76,109 bytes/op. These are single-run checks of the
scaling and allocation claims, not additional timing ranges.

## How it works

* **Row twins** (`internal/storage/row_twin.go`). `ApplyWALChanges` calls
  `adoptTwins` when a transaction's private table replaces the live one. The
  replaced table's row-header array differs from the new one only where the
  transaction changed rows; `catchUpRows` replays those slots from the table's
  structural log (`structVersion` / `rowUpdateLog`, the record incremental
  vector indexes already rely on) and appends the inserted rows. The array is
  stored on the new live table; `cloneTableForSharedTx` takes it, after catching
  up with whatever autocommit statements did in between, instead of calling
  `cloneRowsSharedFor`. The two arrays swap roles at every commit.
* **Index twins** (`internal/storage/index_twin.go`). A transaction's copy of a
  skip list journals its inserts and removals (`SkipList.record`, at most 4,096
  operations). At commit the replaced table's list is brought up to date by
  replaying that journal and becomes the committed index's twin;
  `SecondaryIndex.borrow` hands it to the next transaction as a private list.
* **Unused copies go back.** A transaction that copies a table and then changes
  nothing (an UPDATE that matches no row, a rollback before any change) returns
  its row array and untouched skip lists to the source when it ends
  (`DB.returnTwins`), so it does not cost the next transaction a full copy.
* A skip list that every borrower has since copied is recognised as private
  again (`SkipList.sharers`) and is no longer copied a second time by its owner.

## When copying is required

Each buffer is recycled only when its own safety checks pass:

* The replaced table is still pinned by a stream or another transaction.
* The structural log cannot account for row changes, or the index journal is
  incomplete: DELETE, DDL, foreign-key cascades, WAL replay, or more than 4,096
  updated rows or index operations can require an ordinary copy.
* The relevant row array or skip list was replaced wholesale rather than
  edited in place (statement rollback, index rebuild, an append that outgrew
  its capacity). Address checks reject that buffer. Row and index eligibility
  are checked separately.
* Row arrays are excluded for BLOB, JSON, VECTOR and other reference-typed
  columns, whose cells a transaction must not share. Index lists can still be
  eligible. Buffers are not cached for tables with fewer than 256 rows.
* The transaction was rolled back after changing the table.

## Cost

Up to one extra row-header array (24 bytes per row) and one extra skip list per
secondary index for each qualifying table that transactions write to. Tables
written only by autocommit statements or never written keep no twin.

## Still proportional to something

* `BEGIN` is linear in the number of tables in the database: each table is
  pinned and recorded individually. See the 2026-11-12 follow-up below for the
  current cost.
* The PRIMARY KEY / UNIQUE overlay a transaction's copy borrows is folded into a
  flat map every `max(1024, rows/32)` new keys; that is the 76 KB/op of the
  100,000-row INSERT above.
* After a growth of the row array beyond its capacity, one transaction pays a
  full copy again.

## Reproduction and correctness

```sh
go test ./benchmarks/ -run '^$' -bench 'ParityTxSmall' -benchmem -benchtime=3000x
go test ./benchmarks/ -run '^$' -bench '^BenchmarkParityInsertTxBatch$/^tinySQL$/^mem$' -benchmem -benchtime=3000x

go test ./...
TINYSQL_VERIFY_ROW_TWINS=1 go test ./...   # compares every twin with its table at each hand-over
go test -race ./internal/driver ./internal/storage ./driver .
```

Tests in `internal/driver/tx_twin_test.go`:

* `TestTwinRecyclingMatchesModel`: random transactions (updates of indexed and
  unindexed columns, inserts, deletes, statements that fail and roll back,
  rollbacks, autocommit statements in between, a reader holding its snapshot
  across a commit, two writers of which the second must conflict) checked after
  every step against a plain map by full scan, primary-key lookup and
  secondary-index lookup. It fails if no twin was ever used, and it fails within
  a few rounds when `catchUpRows` is made to skip rows.
* `TestTwinRecyclingUnderConcurrency`: concurrent transfer transactions,
  autocommit updates and snapshot readers; every snapshot must see the same
  total. Run it with `-race`.
* `TestTwinRecyclingAfterJournalOverflow`: an indexed 5,000-row UPDATE exceeds
  both journals, then checks complete rows and index results before and after
  further small transactions. Incomplete journals must force ordinary copies.
* `TestSmallTxAllocationIndependentOfTableSize`: a one-row transaction on a
  60,000-row indexed table, and one that changes nothing, allocate less than a
  third of what the row headers alone would cost.

## Follow-up: histories, constraint overlays and BEGIN images

UPDATE history now uses append-only blocks of 64 deltas. A clone shares its
immutable prefixes with clipped slice capacities and allocates nothing; its
first append copies the open prefix into a private buffer. Small cloned tails
reserve geometrically up to 64 entries, while a new block reserves 64 directly.
A bulk write may grow its first partial block several times; subsequent blocks
have their full capacity. Searches, the 4,096-delta retention limit, structural
invalidation and row-twin catch-up retain their existing rules. Old snapshots
may retain shared blocks until released.

Constraint caches keep at most two read-only layers beneath their current
changes: a base map and a middle overlay. Small transaction deltas are copied;
larger ones are lent or merged, then flattened once their combined size passes
the existing threshold. This avoids copying a growing overlay on every borrow,
at the cost of another possible map lookup and retaining old keys until folding.
Multi-row buckets still require a private clone. Borrowing now checks every
column first, so a refused set cannot mark earlier columns as lent and cause
unnecessary copies on their next write.

The catalog image used to construct a transaction shadow is also its BEGIN
comparison image. The driver no longer serializes the same catalog a second
time. It remains immutable across private DDL/index/RBAC edits, is released on
promotion, and is not retained by general-purpose deep clones. Ordinary and
promoted databases continue to return current catalog snapshots.

### Allocation checks (2026-11-11)

The complete transaction comparison is against `629ee1f`, using source overlays
for the seven changed production files. Both driver test executables contain
the same fixture: 1,000 rows through `database/sql`, genuine score changes,
8 or 2,048 committed warm-up updates, then BEGIN/one-row UPDATE/COMMIT per
operation. Ingestion and warm-up are excluded. Go 1.27.3, darwin/arm64, Apple
M2 Max, GOMAXPROCS=2; medians of three runs of 16 operations. This is an
in-memory workload, including driver/query/transaction overhead, without durable
I/O or fsync.

| Warm-up updates | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| 8 | 10,412 → 10,408 | 136 → 125 |
| 2,048 | 53,372 → 10,304 | 136 → 125 |

Two isolated checks compare the initially uncommitted implementation with this
follow-up, holding the rest of the tree fixed: history cloning uses an overlay
of `table.go`, and refused borrowing an overlay of `exec_dml_insert.go`. They
use three runs of 100 operations. History clones with 1–4,096 retained deltas
fall from 1,024–2,816 allocated bytes to zero. Writers still allocate private
buffers; direct clone-plus-append results vary with the tail/block boundary.
The raw history fixture can grow past 4,096 to stress its storage, whereas a
Table resets its retained journal before that append.

Refused multi-bucket borrowing falls from 56 B / 2 allocations to zero. With
eight additional unique 4,096-key columns it falls from about 809 B to zero;
borrowing followed by writes falls from about 549 KB to zero. The old mixed-set
cost varies with map iteration order; these are fixture-specific allocation
volumes, not latency or peak-live-memory claims.

Tests cover clone/append/reset independence under concurrency, block boundaries,
history rollover, duplicate updates, expired histories, twin catch-up, long
constraint borrower chains, refusal without lender mutation, catalog isolation
and concurrent promotion. The root Go/race suites, focused checks after tail
capacity tuning, SQL-library compatibility, WASM and C/Python/Rust/Swift binding
tests pass.

```sh
go test ./internal/storage ./internal/engine ./internal/driver
go test -race ./internal/storage ./internal/driver \
  -run 'Test.*(History|Twin|Tx|Catalog|Snapshot)' -count=1
GOMAXPROCS=2 go test ./internal/driver -run '^$' \
  -bench '^BenchmarkTxWarmUpdateHistory$' -benchmem -benchtime=16x -count=3
GOMAXPROCS=2 go test ./internal/storage -run '^$' \
  -bench '^BenchmarkRowUpdateHistoryClone' -benchmem -benchtime=100x -count=3
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkConstraintBorrowRefusal$' -benchmem -benchtime=100x -count=3
```

## Follow-up: what BEGIN builds (2026-11-12)

After the changes above, most of what a one-row transaction still allocated
was `BEGIN` itself: about 70 of the 108 allocations of a read-only transaction.

* **The catalog image is cached while the catalog is at rest**
  (`CatalogManager.beginSnapshot`). Every `BEGIN` encoded and sorted the whole
  catalog to build the transaction's private copy. The image is now kept with
  the revision it was read at and reused until a mutation bumps the revision;
  every catalog mutator bumps it before releasing the write lock. Transactions
  still get a private catalog decoded from the image, so their DDL cannot reach
  it. Autocommit DML takes the catalog's write lock and therefore invalidates
  the image; a workload that alternates autocommit writes with transactions
  rebuilds it as before.
* **Snapshots are built in a bare database** (`newShadowDB`). `NewDB` creates an
  MVCC manager and two extension registries; the snapshot replaced the first
  with the live one and almost never touches the others, which are now created
  on first use.
* **The conflict base no longer allocates a record per table.** While a
  transaction pins a table, that table cannot change, so the base points at the
  live table itself. The Version is frozen into a stub only when the transaction
  gives up the pin, which is its first write to that table
  (`DB.recordBaseVersion`).
* **A shared table is pinned once, not twice.** The shadow used to count every
  shared table in its own pin map as well as in the live database's.
  `IsTablePinnedForStream` and `DetachAllPinnedForWrite` now consult the
  shadow's shared set directly.

Before is `a793de2` in a clean worktree, after is this change; same benchmark
binaries run alternately three times, `-benchtime=60000x` (5000x for 500
tables). The machine was under heavy unrelated load during these runs, so the
times are only good for the ratio between neighbouring rows; bytes and
allocations are exact.

| Benchmark (tinySQL/mem, 10,000 rows) | Before | After | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|
| Read-only transaction | 12.1–12.9 µs | 9.4–11.0 µs | 7.7 KB → 5.8 KB | 108 → 77 |
| UPDATE one row by PK | 16.7–18.9 µs | 13.2–17.4 µs | 11.5 KB → 10.0 KB | 136 → 105 |
| INSERT one row | 16.8–21.1 µs | 13.2–16.2 µs | 12.6 KB → 11.1 KB | 120 → 90 |
| Read-only transaction, 500 tables | 290–312 µs | 120–160 µs | 259 KB → 79 KB | 616 → 81 |

`BEGIN` still fills two maps and one pin count per table, so it remains linear
in the number of tables, at well under half the previous cost. What is left of
its fixed cost is decoding the private catalog (about 25 allocations).

Tests: `TestSharedBaseKeepsBeginVersionAfterPrivateCopy` and
`TestBeginCatalogImageIsReusedUntilCatalogChanges` in
`internal/storage/tx_share_test.go`; `TestBeginCatalogImageInvalidatesAfterRBACWrite`
also checks invalidation through the separate RBAC write lock and isolation of
the previous image. The existing pin, conflict and
catalog-visibility tests in `internal/storage` and `internal/driver` cover the
rest.
