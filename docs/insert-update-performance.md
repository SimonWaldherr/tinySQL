# INSERT and UPDATE allocation improvements (2026-09-09)

Batch INSERT now allocates row storage in blocks of up to 64 rows. Whole-table
updates in the simple UPDATE path use the same strategy after detecting an
actual change. Each row has a disjoint, capacity-limited slice; existing rows
remain unchanged for snapshots and rollback. Single-row INSERTs and selective
UPDATEs keep individual row allocations. Trigger, coercion, constraint, index,
RETURNING and WAL handling continue through the existing execution paths.

The original INSERT ... SELECT adapter normalized result column names once and
packed temporary expressions into blocks. It has since been replaced with a
[row cursor](delete-cte-materialization-performance.md) that reuses expression
slots while preserving normal INSERT validation. The historical adapter
measurements below describe the earlier implementation.

## Measurements

Apple M2 Max, Go 1.27.1, darwin/arm64, default GOMAXPROCS=12. Two 200 ms
samples before and after, representative ranges below. These are in-memory
microbenchmarks, not durable transaction throughput measurements.

| Benchmark | Before | After | Allocations before → after |
|---|---:|---:|---:|
| INSERT 1,000 two-column rows | 73–77 µs | 69–74 µs | 2,017 → 1,033 |
| UPDATE all 20,000 rows | 3.20–3.30 ms | 2.55–2.57 ms | 40,122 → 20,435 |
| UPDATE by primary key | 1.56–1.57 µs | 1.56–1.57 µs | 34 → 34 |
| Adapt 1,000 SELECT rows for INSERT | 72–77 µs | 30–32 µs | 4,001 → 34 |

The adapter measurement excludes SELECT execution and destination writes.
Batch INSERT has no indexes or constraints in the benchmark fixture and reuses
a parsed statement. Timing differences for INSERT are small enough to warrant
longer workload-specific measurements.

Fewer objects do not mean fewer allocated bytes: block rounding raises batch
INSERT from about 108 KB to 113 KB/op and whole-table UPDATE from 5.16 MB to
5.24 MB/op. A surviving row can retain the backing block and values belonging to
its former neighbours (up to 64 rows at a fixed schema width). Blocks are never
pooled or reused across statements; this preserves snapshot ownership. A workload
that retains very few rows from many batches, especially with large TEXT/BLOB
values, should account for that retention tradeoff.

## Reproduction and correctness

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkInsertBatchRows|BenchmarkUpdateAllRows|BenchmarkUpdateByPrimaryKey|BenchmarkInsertSelectAdapt)$' \
  -benchmem -benchtime=200ms -count=2

go test ./...
go test -race ./internal/engine \
  -run 'Test.*(Insert|Update|Constraint|Rollback|WAL|DMLPacked)' -cpu=1,36 -count=1
go vet ./internal/engine
```

Regression tests cross 64-row boundaries, cover both INSERT column forms and
INSERT SELECT, preserve old row slices across UPDATE, exercise append isolation,
and verify statement rollback and primary-key lookup. Existing INSERT, UPDATE,
constraint, trigger and WAL tests cover the shared mutation paths. The 36 setting
oversubscribes this 12-core host; it is not a measurement on 36 physical cores.

## Follow-up: reserve batch row headers (2026-09-09)

After commit `7b4b9e7`, INSERT without triggers or ON CONFLICT reserves space for
all incoming row headers once, instead of repeatedly growing and copying the
outer row slice. Single-row inserts retain the existing growth behavior.
Constraint validation and statement rollback still apply; the rollback regression
now checks that an existing row survives a failed 131-row batch as well.

Three fresh 300 ms samples on the same host, comparing against that commit:

| 1,000-row INSERT | Before reservation | After reservation |
|---|---:|---:|
| Time | 59.6–63.1 µs | 58.7–60.6 µs |
| Allocated bytes/op | 112,640 | 77,848 |
| Allocations/op | 1,033 | 1,023 |

This reduces allocated bytes by approximately 31%. The timing ranges overlap;
these samples do not establish a substantial latency improvement. Capacity is
reserved before row validation, so a failing large batch can allocate header
space before reporting its error. Conflict-skipping inserts are excluded to
avoid reserving for rows that may never be inserted.

## Indexed UPDATE and sparse defaults (2026-11-07)

Triggerless UPDATEs can use ordinary secondary-index equality and composite
prefix seeks when no foreign keys exist in the tenant. PRIMARY KEY/UNIQUE
constraint seeks retain priority. The candidate list also bounds the rollback
snapshot, so a point update no longer copies every row header in the table.
The general executor, including RETURNING, uses the same bounded candidates.
Its larger row-map/trigger state is kept in a separate function from the fast
UPDATE dispatcher.

Point-seek candidates are copied before execution because changing an indexed
column can remove entries from the searched index run. Prefix results already
own their row IDs. Both retain physical row order, and every candidate still
passes the full WHERE predicate, coercion, constraints, index maintenance and
WAL handling. Mixed numeric equality uses the SELECT planner's existing seek
safety rules; a compatibility check may require scanning column values.
Trigger and foreign-key paths retain their broader execution/snapshot rules.

`UPDATE ... RETURNING` now includes matching rows whose values stay unchanged.
Those matches count as updates without dirtying the row or advancing the table
version. A statement with both changed and unchanged matches returns both.

Multi-row INSERTs with an explicit column list prepare a compact default-column
list. Primitive default inputs and primitive coercion results can be reused
within the statement. BLOBs are still copied, and defaults parsed from JSON/vector
strings into containers are decoded per row. Unknown default types use the
original per-row path, preserving custom conversion calls and effects on other
defaults. Triggers also retain that path.
Default coercion stays after row-shape validation and cancellation checks, even
when an explicit value later overwrites the default. Single-row statements keep
their existing path; parameter-set APIs still execute separate statements.

### Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Comparisons against
`457f0ee` use the same benchmark code and an original-source overlay. Serialized
runs take three 300–500 ms samples. The fixtures reuse parsed statements and
in-memory storage; parsing, durable I/O and client/FFI transfer are excluded.

| Workload | Before | After | Allocated bytes/op before → after |
|---|---:|---:|---:|
| UPDATE one row by indexed text key in 20,000 rows | 185–192 µs | 1.98–2.05 µs | 485,428 → 2,183 |
| Same, RETURNING id and amount | 3.85–3.99 ms | 2.49–2.54 µs | 7,205,449 → 2,551 |
| INSERT 1,000 rows into 64 columns with one default | 203–223 µs | 187–208 µs | 1,074,089 → 1,074,122 |

The batch timing ranges overlap; a separate 500 ms comparison measured medians
of 261 µs before and 231 µs after. The default plan adds one small allocation
per statement; row-storage allocation is unchanged. The plain point UPDATE
also allocates five more small objects for planning while eliminating the large
snapshot allocation. These results apply to the measured shapes rather than
all UPDATEs or inserts. Full-table UPDATEs and default-free inserts retain their
existing execution strategies.

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkUpdateSecondaryIndex|BenchmarkBatchInsertSparseDefaults)$' \
  -benchmem -benchtime=500ms -count=3
go test ./internal/engine \
  -run 'TestUpdateSecondaryIndex|TestUpdateReturningIncludesUnchangedMatches|TestBatchDefaults' -count=1
make wasm-smoke
```

Regression tests exercise point/prefix key moves, residual NULL predicates,
empty seeks, duplicate-key rollback and retry, numeric representation mixing,
unchanged RETURNING matches, mutable defaults across 64-row blocks, custom
conversion effects and error precedence. The JavaScript smoke checks cover
indexed UPDATE, unchanged RETURNING, rollback and a 131-row defaulted INSERT in
the Browser/Node full and minimal profiles.

## Conflict updates and CHECK validation (2026-11-19)

On repeated imports, UPSERT now reuses the binding map for the current and
`excluded` rows. Triggers and `RETURNING` still receive independent row images.
A table owns the parsed checks and column bindings used to validate its rows.
Dropping the table releases that state. A rejected CREATE stores no CHECK
expression in a process-wide cache.

Median of five serial in-memory runs against `8e1d88e`, Apple M2 Max, Go 1.27.4,
`GOMAXPROCS=2`. Each UPSERT uses one parsed statement and 256 existing primary
keys. The CHECK benchmark measures one warm row with two checks.

| Operation | Time before → after | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| Update 256 conflicting rows | 194.0 → 115.5 µs | 332,122 → 70,689 | 2,570 → 798 |
| Skip 256 updates via WHERE | 150.2 → 78.5 µs | 313,694 → 52,132 | 2,093 → 305 |
| Ignore 256 conflicts | 35.9 → 32.3 µs | 33,828 → 15,377 | 777 → 265 |
| Validate two CHECKs on one row | 318.4 → 122.1 ns | 280 → 0 | 3 → 0 |

The existing driver benchmarks on 10,000 rows serve as controls. Primary-key
lookup used 25 allocations (previously 26) and took 1.63 µs (previously
1.77 µs). Grouped aggregation took 190 → 188 µs. Warm flat cosine top-10 over
128-dimensional vectors took 188 → 191 µs. A small write transaction varied
from 7.07 → 8.22 µs in the 200 ms samples. Four interleaved one-second samples
per version yielded a median of 7.50 → 7.49 µs, with 106 allocations in both.
These timings exclude durable I/O.

```sh
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench '^BenchmarkUpsertBatch$' -benchmem -benchtime=250ms -count=5
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench '^BenchmarkCheckConstraintRow$' -benchmem -benchtime=300ms -count=5
```

## What a multi-row DELETE costs (2026-11-23)

A DELETE holds the table's content lock exclusively, so every search waits for
it. The statement an importer issues for each replaced document is
`DELETE FROM chunks WHERE source_id = 'x' AND load_id <> 'y'` (about six rows),
now and then `... WHERE source_id IN (...)` for a batch. It used to cost the same
whatever the predicate matched, because the secondary indexes were rebuilt from
scratch: an old-to-new position map with one entry per surviving row, a copy of
every key and row ID list (`SkipList.All`), and a new skip list filled with one
insert per entry. That was about 300 bytes of garbage per row of the table and
per index, to remove six rows.

Two changes remove it:

- **In-place renumbering.** A DELETE that reports its removed positions
  (`Table.MarkRowsRemoved`) renumbers each index with
  `Table.RenumberSecondaryIndexRowsAfterRemoval`: one walk along the bottom level
  of the skip list drops the removed positions from the row ID lists, subtracts
  the number of removed positions below each survivor, and unlinks keys whose list
  became empty. Nothing is allocated. A list shared with another index (a
  transaction's copy of the table) is copied first, like for every write, and a
  parked index twin is forgotten. Statements that cannot report their rows
  (triggers, which also keep the position map; DELETE without WHERE; point deletes
  by PRIMARY KEY/UNIQUE value, which swap and pop) keep their old paths.
- **Candidate rows from an index** (`deleteCandidateRows`). When the predicate is
  an AND/OR/NOT combination of comparisons, IN lists and IS [NOT] NULL between
  columns and literals of the column's own type, and a top-level conjunct is
  `text_col = 'x'` or `text_col IN ('x', ...)` on the leading columns of a
  secondary index, the predicate is evaluated only on the rows the index names.
  Both conditions are needed for the result to be identical to the scan: the index
  must name every matching row (true for string equality, where the key is the
  string's bytes, and not for numbers, which SQL compares across int and float),
  and rows that were skipped must not have been able to fail (division by zero,
  incomparable types, a function error). Ordered comparisons of text, numeric
  equality, LIKE, subqueries and function calls therefore scan, as before.

Medians of three runs of 15 statements, in-memory (no durable I/O), engine only
(`Execute` on a parsed statement), GOMAXPROCS=2, i7-10850H, a 14-column table of
8-dimensional vectors, the table size kept constant (the removed rows are
re-inserted outside the timer, a garbage collection runs before each statement).
Indexes: 1 = (source_id, chunk_idx); 3 = that plus UNIQUE (id) and (load_id).
"Before" is the unmodified engine of `053fade` with the benchmark of `7f843cc`.
Time per DELETE, before → after:

| Rows | Indexes | one source (6 rows) | 100 sources (600 rows) | Bytes/op, one source |
| ---: | ---: | ---: | ---: | ---: |
| 100,000 | 0 | 6.5 → 5.8 ms | 8.8 → 9.0 ms | 4.8 → 4.8 MB |
| 100,000 | 1 | 67.6 → 3.4 ms | 59.7 → 5.9 ms | 28.0 → 4.8 MB |
| 100,000 | 3 | 126 → 6.1 ms | 145 → 11.6 ms | 52.2 → 4.8 MB |
| 400,000 | 0 | 22.1 → 22.0 ms | 33.2 → 30.3 ms | 19.2 → 19.2 MB |
| 400,000 | 1 | 265 → 12.6 ms | 318 → 19.3 ms | 112 → 19.2 MB |
| 400,000 | 3 | 556 → 23.4 ms | 580 → 42.0 ms | 209 → 19.2 MB |

At 400,000 rows the statements with an index are 14 to 24 times faster (265 →
12.6 ms is 21x, 556 → 23.4 ms is 24x, the 100-source batch 318 → 19.3 ms is 16x
and 580 → 42.0 ms is 14x). Allocations per statement at 400,000 rows and one
index fell from 2.4 million to 38. A table without an index cannot use candidates
and is unchanged.

A run at 1,000,000 rows (the size of a real corpus; opt in with
`TINYSQL_BENCH_ROWS=1000000`, it needs about 1.5 GB), same method, one source:
0 indexes 39.7 ms (the scan), 1 index 19.8 ms, 3 indexes 37 to 40 ms, each 48.8 MB
allocated; the 100-source batch 62, 33 and 60 ms. The cost does not scale as a
simple per-row constant: it is a scan or an index walk (cache-sensitive, and the
benchmark indexes are built in row order, which is the best case for the walk) plus
24 + 24 MB of row headers, so measure on a corpus of the real size before
quoting a figure.

The compacted row slice keeps about 3 percent spare capacity (`deleteSpareRows`),
so the INSERT of the next source's chunks appends in place instead of copying every
row header under the write lock. `BenchmarkReplaceSourceCycle` times the INSERT of
six rows together with the DELETE of the six old ones; before the spare capacity
it allocated 31 MB at 400,000 rows (one index) and took about 12 ms, after it 19.5
MB and about 9.5 ms (100,000 rows: 7.8 → 4.9 MB, 3.3 → 2.8 ms).

Worst case of index candidates: an indexed leading column that is the same in
every row (`source_kind = 'doc' AND load_id = 'nomatch'`, `BenchmarkDeleteLowSelectivity`).
The index names all N rows, the list is read and sorted, and the predicate then
runs on all of them. Measured against the scan of the same table: 100,000 rows
4.1 vs 3.7 to 4.3 ms, 400,000 rows 14 to 18 vs 17 to 19 ms, 1,000,000 rows 36 to 37
vs 39 to 40 ms, with 8 bytes per row more allocated (the candidate list). That is
about 10 percent slower and bounded, so there is no selectivity cut-off: one would
have to run the scan after reading the index, which costs more than it saves.

What is still O(N):

- the statement snapshot copies the row headers (24 bytes per row) and the
  survivors are copied into a new slice, because the snapshot aliases the old
  array and rollback needs it (about 19 MB at 400,000 rows, 48 bytes per row);
- each index is walked once from its first key to its last, which touches every
  key even when the removed rows are at the end (about 5 to 7 ms per index per
  400,000 keys; the unique index on `id` has one key per row, an index on a
  low-cardinality column has few);
- an index that has only its persisted entries (after a restart) is hydrated into
  a skip list by the first write, O(N log N), as before;
- the scan itself, for every predicate that cannot use candidates;
- inside a transaction (`BEGIN; INSERT; DELETE; COMMIT`) the renumbering ends the
  index's journal, so the commit cannot park an index twin and the next
  transaction's first write to that index copies the skip list, O(N) per index.
  The old rebuild replaced the list wholesale and had the same effect; the speed-up
  above is for autocommit statements and is not measured inside transactions;
- after a DELETE the constraint-index cache (PRIMARY KEY / UNIQUE point lookups) is
  dropped and rebuilt, O(N), on the next point operation, and the numeric column
  profile is rebuilt on the next numeric index seek of a SELECT or UPDATE, O(N x
  columns) per table version;
- the persistence of the change (gob save, WAL) is outside these benchmarks;
- the derived structures (full-text index, flat vector cache, HNSW graph) are
  maintained by their own code, see the retrieval roadmap.

Differential tests (`delete_index_diff_test.go`, `delete_index_candidates_test.go`)
compare tables with and without indexes statement by statement, inside
transactions with savepoints, across checkpoint round trips and disk reopen, and
check every index against one rebuilt from the rows. The benchmarks:

```sh
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench 'Delete(ReplacedSource|100Sources)$' -benchtime=15x -count=3
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench 'ReplaceSourceCycle|DeleteLowSelectivity' -benchtime=10x -count=3
```
