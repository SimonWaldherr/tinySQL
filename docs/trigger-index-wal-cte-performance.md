# Trigger, range-index, WAL and recursive-CTE follow-up

Baseline: `1ae111c84628ec6e15cc3b66054a8d80ac21ff8f`.
The monthly comparison in `benchmarks/month-2026-08` is separate and unchanged.

## Implementation

- Trigger runners reuse their NEW/OLD binding object across rows of one
  statement. Calls are synchronous; nested DML creates its own runner. Row
  maps and RETURNING ownership follow the existing rules. Parsed programs and
  trigger caches are unchanged.
- Numeric secondary-index range seeks use bounded local scratch for the
  equality prefix, numeric bounds and combined seek key. Large prefixes grow
  normally. Both the persisted sorted array and mutable skip-list paths retain
  validation, table-order sorting and independently owned result row IDs.
  This changes temporary-key allocation, not index format or search complexity.
- AdvancedWAL computes the identical CRC32C byte stream with a contiguous
  header and scratch shared across header, row scalars and column descriptors.
  A CRC writer accepts string writes directly. Neither WAL format nor commit
  flushing, locking, transaction ownership or checkpoint behavior changes.
- Recursive CTEs prepare their positional column mapping once per execution
  and reuse it across frontiers. A changed recursive output schema rebuilds
  the mapping. Each output row still has its own map, with the same aliases,
  NULL handling and fallback lookup. The shared parsed AST stores no new
  mutable execution state. Non-recursive materialization is unchanged.

## Measurement

Apple M2 Max, Go 1.27.1 darwin/arm64, GOMAXPROCS=1, GOGC=100. Initial
three-sample measurements preceded production edits. Final comparison uses
prebuilt before/after engine and storage test binaries, identical benchmark
functions, eight alternating rounds and 300 ms per sample. Builds, tests and
analysis run outside timed rounds. Raw samples, initial measurements, binary
and source hashes are in [benchmarks/trigger-index-wal-cte](benchmarks/trigger-index-wal-cte).

Trigger measurements insert 100 rows, each firing an audit INSERT; tables are
cleared between operations to bound the working set. The benchmark calls the
INSERT handler directly, excluding top-level rollback snapshots and durability.
Index seeks use 20,000 prebuilt entries and return 1, 10 or 1,000 row IDs.
"warmfalse" means the sorted persisted representation in RAM, not cold disk I/O;
"warmtrue" means the mutable skip list has already been constructed.

WAL checksum benchmarks isolate serialization hashing; the LogInsert benchmark
includes BEGIN, INSERT, COMMIT and the unchanged default WALSyncFull flush on a
real temporary file. ModeWAL's indexed UPDATE benchmark repeatedly sets the
same value: after the first change it is predominantly a no-op control, not a
measure of sustained durable writes. The CTE chain generates 500 rows across
recursive steps. Non-recursive aggregate/projection benchmarks are controls.

B/op means cumulative allocations per operation, not retained heap or peak RSS.
Results are local measurements on a shared computer. Percentage changes use
paired geometric ratios with seeded 10,000-resample bootstrap 95% intervals;
latency and allocation values are medians.

## Results

| Workload | Before median | After median | Paired time change, 95% interval | Allocs/op |
|---|---:|---:|---:|---:|
| 100-row audit-trigger INSERT | 42.49 µs | 41.51 µs | -2.5% [-3.2, -1.8] | 310 → 210 |
| Sorted-array range, 1 result | 0.23 µs | 0.13 µs | -42.8% [-44.7, -40.7] | 8 → 1 |
| Mutable-index range, 1 result | 0.31 µs | 0.19 µs | -36.2% [-39.2, -32.9] | 8 → 1 |
| Sorted-array range, 10 results | 0.34 µs | 0.25 µs | -27.4% [-27.9, -26.8] | 8 → 1 |
| Scalar WAL checksum | 0.32 µs | 0.22 µs | -29.9% [-31.8, -27.9] | 8 → 2 |
| BLOB WAL checksum | 5.75 µs | 5.61 µs | -3.6% [-5.1, -2.3] | 8 → 2 |
| Recursive CTE, 500 rows | 518.05 µs | 500.67 µs | -5.2% [-7.7, -3.1] | 10,522 → 10,029 |
| WAL INSERT transaction with full sync | 4.334 ms | 4.435 ms | +7.1% [-3.1, +25.5] | 35 → 21 |

The trigger improvement is mainly fewer allocations (32.3%), with 1,584 fewer
B/op. Small range seeks save 88 B/op; at 1,000 returned rows their output
allocation/traversal dominates, so there is no clear latency improvement.
The scalar checksum saves 44 B/op (39.3%). Its CPU gain does **not** establish
faster durable commits: the full-sync INSERT median is higher, with substantial
variation and a paired interval spanning no change. Flush latency is unchanged
by the patch, and the measurements support only fewer commit-path allocations.

The recursive chain saves 493 allocations but only 3,824 B/op (0.38%); result
maps and recursive evaluation still dominate. Non-recursive CTE allocation
counts are unchanged. The filtered non-recursive control runs 1.7% faster in
these samples despite no change to its path, illustrating residual layout/load
variation; this is not attributed to the recursive optimization. The aggregate
CTE and no-op ModeWAL UPDATE controls have intervals spanning no change.

See the complete [engine comparison](benchmarks/trigger-index-wal-cte/engine-comparison.md)
and [storage comparison](benchmarks/trigger-index-wal-cte/storage-comparison.md)
for every measured workload. Ratios are paired geometric means, so they need
not equal ratios of the displayed rounded medians.

## Reproduction

Extract the baseline with `git archive` into a disposable directory. Copy
`internal/storage/range_seek_benchmark_test.go` from the final tree into that
baseline. Build each package's tests in both trees, naming the outputs
`engine-before.test`, `storage-before.test`, `engine-after.test`, and
`storage-after.test` in a temporary directory:

```sh
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/tinysql-perf/engine-before.test
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/storage -run '^$' -c -o /tmp/tinysql-perf/storage-before.test
```

Use the corresponding `after` names in the modified tree, then from that tree:

```sh
python3 -B docs/benchmarks/trigger-index-wal-cte/run.py /tmp/tinysql-perf /tmp/tinysql-perf-results
python3 -B docs/benchmarks/columnar/compare.py /tmp/tinysql-perf-results/engine-before.txt /tmp/tinysql-perf-results/engine-after.txt
python3 -B docs/benchmarks/columnar/compare.py /tmp/tinysql-perf-results/storage-before.txt /tmp/tinysql-perf-results/storage-after.txt
```

## Correctness

New checks exercise oversized index prefixes, sorted/owned results in both
representations, reuse of CTE mappings without sharing output maps or schema,
and byte-identical complete WAL checksums against the previous implementation.
Existing suites cover nested triggers, WHEN, RETURNING, rollback, CTE UNION and
limits, range-boundary/type safety, WAL legacy fixtures, corruption and recovery.

Validation passed with `go test ./...`, `go vet ./...`, and
`go test -race . ./internal/engine ./internal/storage ./internal/driver`.
