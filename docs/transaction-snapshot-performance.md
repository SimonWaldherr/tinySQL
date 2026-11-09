# Transaction snapshots: BEGIN no longer copies the database (2026-11-09)

`BEGIN` used to give every transaction a private copy of every table: the row
headers, the PRIMARY KEY / UNIQUE hash and every secondary index. A transaction
that read one row of a 100,000-row table therefore paid for copying 100,000
rows, and the cost grew with the database rather than with the work done. A
transaction that updated one row of that table cost about 1.8 ms; SQLite does
the same in about 7 µs.

`BEGIN` now shares the live tables and copies one only when the transaction
first writes it, and even then lends the table's indexes instead of duplicating
them.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, in-memory database through
`database/sql` (`MaxOpenConns(1)`), one `BEGIN … COMMIT` per operation. Before
is the tree at `bd52a27`, after is this change; both ran the same benchmarks
(`benchmarks/tx_table_size_benchmark_test.go`) with `-benchtime=500x`.

| Benchmark (tinySQL/mem) | Before | After | Bytes/op before → after |
|---|---:|---:|---:|
| Read-only transaction, 1,000 rows | 57.8 µs | 7.1 µs | 114 KB → 7.7 KB |
| Read-only transaction, 10,000 rows | 219 µs | 6.8 µs | 805 KB → 7.7 KB |
| Read-only transaction, 100,000 rows | 1,538 µs | 7.0 µs | 6.9 MB → 7.7 KB |
| UPDATE one row by PK, 1,000 rows | 58.8 µs | 23.3 µs | 123 KB → 43 KB |
| UPDATE one row by PK, 10,000 rows | 230 µs | 54.6 µs | 813 KB → 277 KB |
| UPDATE one row by PK, 100,000 rows | 1,785 µs | 350 µs | 6.9 MB → 2.6 MB |
| INSERT one row, 1,000 rows | 63.6 µs | 39.7 µs | 122 KB → 79 KB |
| INSERT one row, 10,000 rows | 278 µs | 101 µs | 811 KB → 385 KB |
| INSERT one row, 100,000 rows | 1,673 µs | 490 µs | 6.9 MB → 3.5 MB |
| Read-only transaction, 50 tables | 98 µs | 24.9 µs | 154 KB → 25.7 KB |
| Read-only transaction, 500 tables | 680 µs | 149 µs | 1.3 MB → 227 KB |
| 100 INSERTs in one transaction (table grows to 300k rows) | 2.54 ms | 1.18 ms | 10.8 MB → 4.2 MB |

SQLite on the same machine: 5–12 µs for every single-row transaction above,
independent of table size (a transaction of 100 INSERTs takes it about 0.5 ms). Autocommit statements are unchanged (INSERT 1.6 µs, UPDATE by PK
2.3–2.5 µs, point lookup 1.7–1.8 µs, within run-to-run noise).

Timings of microbenchmarks this short are dominated by the garbage collector:
the old `BEGIN` allocated 7 MB per operation against a heap of a few MB, which
kept a GC cycle running almost continuously. Cutting bytes allocated is what
moved the numbers; the byte counts are the more reliable column.

## How it works

* **Shared tables, pinned in the live database** (`internal/storage/tx_share.go`).
  `SnapshotForTxShared` / `SnapshotForReadTxShared` map each table of the
  transaction's shadow database to the live `*Table` and pin it with the
  copy-on-write protocol direct result streams already use
  (`PinTableForStream`, `DetachPinnedTableForWrite`). A live writer that is
  about to change a pinned table first moves to a private copy, so the
  transaction keeps reading its `BEGIN`-time rows no matter what other
  connections commit.
* **Copy on first write.** The shadow's first write to a table replaces it with
  a private copy (`privateCopyOfShared`) and drops the pin. A table the
  transaction never writes is never copied. `CollectWALChanges` skips tables
  that are still shared, which is exactly the set the transaction did not
  change; the driver's conflict check therefore still fires only for tables the
  transaction wrote.
* **Release on every exit.** `conn.clearTxState` runs the snapshot's release
  function, so commit, rollback, failed commit and abandoned connections all
  drop the pins. A leaked pin would only cost later writers one extra copy.
* **Default deny for statements the engine cannot classify.**
  `detachStreamSnapshotsForWrite` (internal/engine/stream_snapshot.go) names the
  statements that only touch one table (INSERT, UPDATE, DELETE, ALTER, CREATE/DROP
  INDEX) or no existing table's contents (CREATE/DROP of tables, views, jobs,
  triggers, users, roles, grants). `ANALYZE`, `REFRESH MATERIALIZED VIEW`,
  `CALL` and any statement type added later detach *every* pinned table first.
  Copying too much is safe; writing into a table a transaction still reads is not.
* **Lent indexes.** Copying a table no longer copies its indexes.
  * `SecondaryIndex.borrow` shares the skip list; the first side to mutate it
    copies it (`hydrateForWrite`). An UPDATE that changes no indexed column
    never copies it.
  * `constraintIndexEntry.borrow` gives the copy a read-only view of the lender's
    key map plus a private overlay (`base` + `rows`, with a tombstone for deleted
    keys). The lender copies its map before its own next write (`lent` / `own`).
    Overlays are carried forward while they are small and folded into a flat map
    once they exceed `max(1024, len(base)/32)` keys, so a chain of transactions
    does not grow a chain of overlays. Entries with multi-row buckets (non-unique
    foreign-key parents) are not lent because their bucket slices are appended
    to in place; they are copied as before.
* **Scalar-only tables skip the per-cell scan.** `cloneRowsSharedFor` copies the
  row headers with one `copy` when every column type is a plain scalar, instead
  of type-testing every cell of every row for BLOB/JSON/VECTOR values.
* **Cheaper conflict base.** The base of a transaction records only each table's
  name and Version (`versionStub`) instead of a full empty table with its maps,
  and the snapshot's tenant maps are sized once.
* `simpleColumnIndex` (used by every simple UPDATE/DELETE/SELECT plan) cuts all
  its qualified column keys from one backing string: 11 allocations → 4 per
  statement, UPDATE by PK 42 → 35 allocations.

Backends whose tables live in an evictable pool (`ModeIndex`, `ModeHybrid`,
`ModePagedIndex`) cannot be pinned and keep the previous eager copy.

## What is still proportional to table size

* The first write to a table copies its row-header slice (24 bytes per row, one
  `memmove`). For 100,000 rows that is 2.4 MB and now most of the remaining
  cost of a single-row write transaction.
* The first INSERT/DELETE (or UPDATE of an indexed column) into a table with a
  secondary index copies that index's skip list.
* After a transaction that wrote a table is *rolled back*, the first write to
  that live table copies its PRIMARY KEY map once, because the rolled-back copy
  might still have been reading it. A committed transaction replaces the table,
  so it does not pay this.
* Following up would need either append-only transactions to extend the live
  slice's spare capacity in place (with an ownership flag so two transactions
  never claim the same tail) or a chunked row store; neither is attempted here.

## Reproduction and correctness

```sh
go test ./benchmarks/ -run '^$' -bench 'ParityTx' -benchmem -benchtime=500x

go test ./...
go test -race ./internal/driver ./internal/storage ./driver .
```

`go test -bench` splits `-bench` on `/` into one unanchored regular expression
per level, even inside parentheses, so `…/rows=1000/…` also matches 10,000 and
100,000. Anchor with `$`.

Tests that pin the behaviour:

* `internal/storage/tx_share_test.go`: nothing is copied at `BEGIN`; the shadow's
  first write copies and unpins; a live writer is diverted from a pinned table;
  `CollectWALChanges` skips shared tables; release is idempotent and keeps other
  pins; `DetachAllPinnedForWrite`; copy-on-write secondary indexes in both
  directions.
* `internal/driver/tx_lazy_snapshot_test.go`: a transaction keeps its `BEGIN`
  snapshot while other connections commit to the same and to other tables;
  conflicts arise only for tables the transaction wrote; PRIMARY KEY and
  secondary-index lookups stay correct after commit and after rollback;
  `ANALYZE` in a transaction does not reach live tables; pins are released on
  every way a transaction can end. Disabling the detach hook makes four of these
  fail and hangs the rest of the run.
* `internal/engine/constraint_index_entry_test.go`: a differential test of lent
  entries (originals, borrowers, borrowers of borrowers, flat copies) against a
  plain-map model, plus the multi-bucket refusal and the overlay fold-over.
