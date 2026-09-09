# INSERT and UPDATE allocation improvements (2026-09-09)

Batch INSERT now allocates row storage in blocks of up to 64 rows. Whole-table
updates in the simple UPDATE path use the same strategy after detecting an
actual change. Each row has a disjoint, capacity-limited slice; existing rows
remain unchanged for snapshots and rollback. Single-row INSERTs and selective
UPDATEs keep individual row allocations. Trigger, coercion, constraint, index,
RETURNING and WAL handling continue through the existing execution paths.

INSERT ... SELECT normalizes result column names once and packs the temporary
expression slices and Literal objects into blocks. Every cell retains its own
Literal; selected values still pass through normal INSERT validation.

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
