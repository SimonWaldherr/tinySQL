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

* `BEGIN` costs about 0.25 µs and 0.5 KB per table in the database (133 µs for
  500 tables): each table is pinned and recorded individually.
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
