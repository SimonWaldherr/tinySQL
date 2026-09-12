# Columnar execution and compact results

tinySQL now has two independent additions: typed batches for eligible numeric
aggregates, and an opt-in column-oriented result API. The underlying tables and
persistence formats remain row-oriented. This change does not provide disk
projection pushdown, zone maps, compressed column segments, or an Arrow API.

## Execution

Direct-column `SUM` and `AVG`, optionally accompanied by `COUNT(column)` and
`COUNT(*)`, use query-local batches when the selected access path has at least
2,048 candidate rows. Batches contain up to 256 rows. Filters use the existing
predicate evaluator once per candidate, before aggregate columns are extracted.
Each distinct aggregate column is extracted once per batch into a typed
`float64` buffer and a NULL validity bitmap. Repeated references such as
`SUM(x), AVG(x), COUNT(x)` share that extraction and one running numeric
accumulator. Duplicate COUNT(*) projections also share their count.

Ungrouped queries keep a single accumulator. Grouped queries preserve the
existing typed grouping keys and first-encounter group order. HAVING, sorting,
and LIMIT/OFFSET reuse the existing aggregate finalizer. `EXPLAIN` displays
`BATCH AGGREGATE` with the batch size and the possibility of a scalar fallback.

Small candidate sets, COUNT-only queries, MIN/MAX, DISTINCT aggregates, and
expression arguments retain the existing scalar implementation. Runtime values
that need nonnumeric or exact decimal handling switch the current batch and
the rest of the query to scalar accumulation. No predicate is reevaluated.
Shared numeric states are copied to their individual projections before this
switch, so COUNT can diverge from SUM on nonnumeric values and every decimal
aggregate retains its complete numeric prefix. Otherwise the copies happen
once before result finalization, not once per row or batch.
SUM additions stay in the original order across batch boundaries, preserving
floating-point rounding; decimals keep rational promotion. No column cache
survives the query, so no invalidation or transaction-visible copy is added.

Batch scratch is bounded by referenced columns, independently of table size.
Grouping state and materialized results still grow with the number of groups
or output rows. This is not an out-of-core aggregation implementation.

## Compact result API

```go
result, err := tinysql.ExecSQLColumnar(ctx, db, "default",
    `SELECT id, amount FROM sales WHERE amount > 0`)
if err != nil {
    return err
}
for row := 0; row < result.RowCount; row++ {
    fmt.Println(result.Values[0][row], result.Values[1][row])
}
```

`ExecuteColumnar` accepts a parsed SELECT. `ColumnarResultSet` contains `Cols`,
`Values[column][row]`, and `RowCount`; cells use `any` and NULL is `nil`. Simple
unordered physical-table scans write column slices directly, avoiding Row maps.
Other SELECTs (including aggregates, joins, sorting, DISTINCT, CTEs, and alias
collisions) transpose the ordinary executor's final result. Those fallback
queries still incur their original intermediate allocations plus conversion.

Both APIs share statement authorization, locks, panic recovery, and auditing.
Non-SELECTs are rejected without executing them. Output slices belong to the
caller and are never reused by a later query. Nested cell values are not
deep-copied, matching the existing Row API. Results are fully materialized;
use `ExecuteStream` when the entire output should not remain resident.

## Measurement method

Baseline: commit `7f5bf487157fedab77f313595b8a1e1e5cf10796`, with only the new
benchmark fixture added. Its test binary was compiled and the
[initial baseline](benchmarks/columnar/before.txt) measured before implementation.
The same binary is retained for paired comparisons; no runtime toggle simulates
the old implementation.

Environment: Apple M2 Max, macOS arm64, Go 1.27.1, `GOMAXPROCS=1`, `GOGC=100`.
The [recorded environment](benchmarks/columnar/environment.json) includes binary
and harness SHA-256 hashes. Timings are single-process, warm in-memory query
execution. Fixtures, SQL parsing, and one cache warmup are excluded; conversion
from row storage and final result creation are included. No builds or test
suites run concurrently with the final measurements.

The main fixture has 131,072 rows and either 4 or 32 columns, 16 groups, and
an amount NULL every 19 rows. Extra columns contain shared immutable text;
this measures wide row layout, not unique large text/BLOB payloads. Additional
cases cover empty/128/2,048-row tables, 0%/1%/50% matches, one group per row,
MIN/MAX, ordinary projections, a point index lookup, and an indexed UPDATE.

The final comparison uses eight paired rounds, alternating which binary runs
first, with 300 ms per benchmark sample. Tables show median times and memory.
Time changes use the geometric mean of paired ratios, with a seeded 10,000-draw
paired bootstrap 95% interval. Intervals describe local timing variability;
they are not corrected for multiple comparisons and do not predict other
machines, production workloads, or disk performance.

## Stage 1 results: initial columnar implementation

See the [complete before/after table](benchmarks/columnar/comparison.md),
[paired baseline samples](benchmarks/columnar/paired-before.txt), and
[candidate samples](benchmarks/columnar/after.txt).

Selected medians from the paired runs (131,072 rows):

| Query | Row width | Before | After |
|---|---:|---:|---:|
| SUM / AVG / COUNT, two input columns | 4 | 4.268 ms | 1.437 ms |
| SUM / AVG / COUNT, two input columns | 32 | 4.426 ms | 1.979 ms |
| Filter matching 50%, then SUM / AVG / COUNT | 4 | 1.941 ms | 0.826 ms |
| GROUP BY, 16 groups | 4 | 4.558 ms | 3.271 ms |
| GROUP BY, 16 groups | 32 | 4.837 ms | 3.828 ms |
| Indexed point aggregate | 4 | 3.01 µs | 2.97 µs |
| Indexed point UPDATE (existing benchmark) | — | 2.22 µs | 2.19 µs |

The largest aggregate improvements are approximately 50–66% less execution
time. Few-group aggregation improves about 21–28%. One-group-per-row queries
remain dominated by group/result allocation and show no reliable improvement.
Wide scans with no matching rows also show no clear timing improvement.
Small queries and point-access controls retain their allocation counts; minor
timing shifts in unchanged paths are visible in the full table, including a
0.8% increase for the narrow MIN/MAX control.

This speed costs scratch allocation: the narrow multi-aggregate increases from
2,616 to 7,656 B/op (38 to 41 allocations); one SUM increases from 2,120 to
4,448 B/op. These allocations are bounded per query, not per input row. They
must not be confused with the result-memory savings of the separate API.

The batch-size experiment compared 256, 512, 1,024 and 2,048 rows with three
200 ms samples for each of six cases. 256 used the least scratch and had the
lowest median in both multi-aggregate and grouped scans; selective scans were
similar. This is a local engineering choice, not a universal optimum. Raw
files are `benchmarks/columnar/batch-size-*.txt`.

The [ablation](benchmarks/columnar/ablation.txt) also compares the original
per-row accumulator without the old empty-key grouping map against typed
batches. This separates batching's benefit from the simpler ungrouped state.
The initial ablation uses 512-row batches; the final stage-1 comparison uses 256.

For result layout, [API samples](benchmarks/columnar/output.txt) compare Row maps
and columns in the same candidate binary. These are an API-to-API comparison,
separate from the baseline commit comparison. Eight paired samples per query
run in fresh processes, alternating the API order each round. See the
[API comparison table](benchmarks/columnar/output-comparison.md), where
"Before" means maps and "After" means columns.

| Two-column result, 131,072 source rows of width 32 | Row maps | Column output |
|---|---:|---:|
| All rows: execution median | 25.830 ms | 3.716 ms |
| All rows: allocated bytes/op | 45,089,368 | 4,194,984 |
| All rows: allocations/op | 262,149 | 8 |
| Half the rows: execution median | 14.942 ms | 6.268 ms |
| Half the rows: allocated bytes/op | 24,796,376 | 11,079,080 |
| One percent of rows: execution median | 1.528 ms | 1.280 ms |

The full projection uses about 85% less time and 91% fewer allocated bytes.
Filtered output has less predictable cardinality and grows its column buffers,
so the half-table query saves about 55% of allocated bytes. Aggregate fallback
has no reliable timing improvement and adds 656 B/op for conversion; the batch
execution benefit is already available through the ordinary Row API.

The opt-in memory test runs one
format in each fresh process: `TotalAlloc` delta measures query allocations;
the post-GC heap delta while keeping the result alive measures retained output.
`/usr/bin/time -l` records peak process RSS separately, including the fixture,
runtime, and test harness.

Three fresh-process measurements per format gave these medians for the full
two-column projection:

| Memory measure | Row maps | Column output |
|---|---:|---:|
| Retained result heap after GC | 43.000 MiB | 4.000 MiB |
| Peak process RSS, including fixture | 137.609 MiB | 95.203 MiB |

The fixture itself occupies about 69.26 MiB of live Go heap before execution.
The result saves about 91% retained heap, while the complete process saves about
31% peak RSS. Raw counters are in `benchmarks/columnar/memory-{maps,columns}-*.txt`.
macOS resource counters required running these six local measurement processes
outside the filesystem sandbox; all timing comparisons used the same sandbox
settings for both variants.

## Stage 2: shared accumulators and result buffer growth

The follow-up optimizes the stage-1 implementation rather than using the old
row engine as its baseline. Its [initial measurements](benchmarks/columnar/stage2/initial-before.txt)
were recorded before the follow-up edits. Final paired runs use both binaries
with the same extended benchmark harness, eight alternating rounds, 300 ms per
sample, and the same Go/GOMAXPROCS/GOGC settings as stage 1. Builds, tests and
statistical analysis finish outside the measured intervals.

Repeated numeric aggregate projections now share accumulation as well as
extraction. The scalar fallback restores each projection's state first, as
described above. Column output now writes projected values directly into the
result, avoiding an intermediate value slice. When an output column is full,
its requested capacity grows geometrically, bounded by remaining candidates
and LIMIT. The allocator can round that request up. This reduces copying for
large filtered results without guessing selectivity or reevaluating predicates.

Geometric growth can retain more spare capacity at some cardinalities. The
suite therefore also includes uniformly distributed matches, roughly 1%
uniform matches, all rows matching a filter, and LIMIT/OFFSET. Allocated bytes
per operation measure transient allocation, not peak RSS or retained heap.

| Workload, 131,072 source rows | Stage 1 median | Stage 2 median | Paired time change (95% interval) |
|---|---:|---:|---:|
| SUM/AVG/COUNT, width 4 | 1.422 ms | 1.207 ms | -15.1% [-15.6, -14.6] |
| SUM/AVG/COUNT, width 32 | 1.939 ms | 1.737 ms | -10.6% [-12.1, -9.3] |
| All rows, column output | 3.966 ms | 3.083 ms | -22.7% [-23.9, -21.6] |
| Half the rows, column output | 6.393 ms | 3.219 ms | -49.4% [-50.3, -48.5] |
| All rows matching a filter, column output | 11.688 ms | 5.309 ms | -54.3% [-54.7, -53.9] |
| Uniformly distributed matches, column output | 1.885 ms | 1.812 ms | -3.9% [-4.3, -3.6] |

Half-table output allocation drops from 11,079,080 to 4,228,008 B/op
(-61.8%), with 46 versus 24 allocations. The all-matching filtered result
drops from 22,564,264 to 8,962,984 B/op (-60.3%). Single SUM, ordinary row
projections, index lookup and primary-key update controls remain approximately
unchanged. The table shows medians; percentage changes use paired geometric
ratios and therefore need not equal the ratio of the displayed medians.

There are measured tradeoffs: sparse column output is 1.4% slower and the
LIMIT/OFFSET case is 1.1% slower, while both allocate 99,240 rather than
118,184 B/op. The aggregate-fallback workload is 2.7% slower through the Row
API and 3.3% slower through the Column API, with 16 additional B/op for the
batch-operation metadata; it has no repeated numeric aggregates to share.
Uniform sparse matches allocate 30,208 more bytes (+2.6%) because of spare
buffer capacity. These results do not support a universal speedup. Stage 2
does not repeat the retained-heap or peak-RSS measurements from stage 1.

The [full stage-2 comparison](benchmarks/columnar/stage2/comparison.md) contains
both changed workloads and unchanged control paths. Its
[environment record](benchmarks/columnar/stage2/environment.json) identifies the
two binaries and benchmark files by SHA-256. A
[reverse patch](benchmarks/columnar/stage2/restore-stage1.patch) reconstructs the
stage-1 implementation from this tree without changing the benchmark harness.

To reproduce stage 2, apply that zero-context patch with
`git apply --unidiff-zero docs/benchmarks/columnar/stage2/restore-stage1.patch`
in a disposable copy of this tree and
compile its engine tests as `/tmp/tinysql-columnar-stage2-before.test`. Compile
the unchanged final tree as `/tmp/tinysql-columnar-stage2-after.test`, then run:

```sh
python3 docs/benchmarks/columnar/stage2/run.py /tmp/tinysql-columnar-stage2-before.test /tmp/tinysql-columnar-stage2-after.test /tmp/columnar-stage2
python3 docs/benchmarks/columnar/compare.py /tmp/columnar-stage2/before.txt /tmp/columnar-stage2/after.txt
```

Both `go test ./...` and `go vet ./...` pass. Race checks pass for the public
API, engine, storage and SQL driver with `go test -race . ./internal/engine
./internal/storage ./internal/driver`. Added differential tests cover COUNT
before SUM/AVG, repeated projections, late decimal/nonnumeric transitions, and
expression errors and pagination across output-buffer growth boundaries.

## Reproduction

Build the baseline in a separate checkout of the commit above after copying
`internal/engine/columnar_benchmark_test.go` from this checkout into it. Build
both binaries with the same Go compiler and settings:

```sh
# In the baseline checkout:
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/tinysql-columnar-before.test
# In a disposable stage-1 checkout (apply the reverse patch above to the final tree):
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/tinysql-columnar-after.test
python3 docs/benchmarks/columnar/run.py /tmp/tinysql-columnar-before.test /tmp/tinysql-columnar-after.test /tmp/columnar-results
python3 docs/benchmarks/columnar/compare.py /tmp/columnar-results/paired-before.txt /tmp/columnar-results/after.txt
python3 docs/benchmarks/columnar/run_output.py /tmp/tinysql-columnar-after.test /tmp/columnar-results
TINYSQL_COLUMNAR_MEMORY=maps GOMAXPROCS=1 GOGC=100 /usr/bin/time -l /tmp/tinysql-columnar-after.test -test.run '^TestColumnarMemory$' -test.v
TINYSQL_COLUMNAR_MEMORY=columns GOMAXPROCS=1 GOGC=100 /usr/bin/time -l /tmp/tinysql-columnar-after.test -test.run '^TestColumnarMemory$' -test.v
```

Correctness coverage compares batch results directly with the existing scalar
paths, including mixed numeric types, NULL groups, decimals before/after batch
boundaries, floating-point bit patterns, candidate row IDs, cancellation,
HAVING, sorting and pagination. Result API tests compare against Row output,
including fallbacks, errors, mutations, permissions, auditing and concurrent
readers/writers. The full root-module suite and targeted race tests pass.
