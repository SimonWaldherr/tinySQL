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
