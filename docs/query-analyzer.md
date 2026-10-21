# Query analyzer

Use `EXPLAIN ANALYZE` to execute a query and find where its time goes:

```sql
EXPLAIN ANALYZE
SELECT category, SUM(amount) AS total
FROM orders
WHERE amount > 0
GROUP BY category
ORDER BY total DESC
LIMIT 20;
```

The result contains the estimated plan, measured execution stages, and a final
`ANALYZE` summary. It works through the Go API, `database/sql`, and SQL clients.
Plain `EXPLAIN` still returns only `step`, `operation`, and `detail` and does
not execute the query. `ANALYZE table` remains the separate statistics command.

## Reading the result

`EXPLAIN ANALYZE` adds these columns to the three existing columns:

| Column | Meaning |
| --- | --- |
| `parent_step` | Step number of the enclosing measured stage; NULL for the execution root. |
| `calls` | Number of completed invocations of this stage at this path. |
| `actual_rows` | Sum of rows produced by the stage across its calls. Grouping produces groups; join lookup produces distinct lookup keys. Source planning and type validation report zero; a cached scalar lookup reports one. |
| `total_ns` | Wall-clock nanoseconds, including measured child stages. |
| `self_ns` | Nanoseconds spent in this stage, excluding its measured children. |
| `percent` | Self time as a percentage of the root execution's total time. |
| `bottleneck` | True for the stage with the greatest self time; exactly one stage is selected. |

Plan-only rows and the final textual summary have NULL in these added columns.
Start with `bottleneck=true`, then inspect `self_ns`, `percent`, and its parent
path. Do not add inclusive `total_ns` values: a nested SELECT is already part
of its enclosing source/filter/projection stage. Self times add up to the root
total. `SELECT` self time includes dispatch, fast-path planning attempts, and
work outside separately measured stages.

The analyzer follows the executor actually selected. General execution exposes
source materialization, joins, WHERE, projection/grouping, DISTINCT, sorting,
set operations, and paging. Raw execution exposes its scan and sort stages,
join index lookup and probing, and scalar or batched aggregation. A fused loop
has a combined label such as `SCAN/FILTER/AGGREGATE` or `SCAN/FILTER/TOP-N`;
there are no per-row timers pretending to separate work within that loop.
Nested SELECTs, derived tables, and CTEs retain their parent relationship.
Repeated stages with the same parent, operation, and detail are aggregated;
`calls` can expose repeated execution without storing one trace per input row.
Distinct subqueries with the same description under the same parent share a
measurement row. This is a stage profile, not a unique AST-node trace.

## Measurement scope

Timing is enabled only for `EXPLAIN ANALYZE`. Ordinary queries allocate no
profile and read no profiling clock. Stage boundaries add a few conditional
checks, not checks inside row/comparison loops. Query-local collectors do not
modify cached plans, so concurrent executions of one statement remain separate.
With profiling disabled, three alternating 200 ms benchmark runs against the
previous executor showed unchanged allocation counts. Median COUNT(*) time
changed from 1.797 to 1.816 µs; a 20-row LIMIT/OFFSET query from 4.063 to
4.203 µs. A 20,000-row GROUP BY changed from 453.85 to 456.65 µs. This is a
small but measurable overhead for short queries, not a zero-cost claim.

The measured query runs once. Plan rendering happens afterwards so it cannot
warm an index or query plan before the measured execution. Materialized views
are described from catalog metadata during plan rendering; rendering never
refreshes their cached data.

Times include executor planning, data access, expression evaluation, and result
materialization. They exclude SQL parsing, waiting for the outer content lock,
permission checks, outer rollback snapshots, commit/WAL finalization, plan
rendering, and client transfer. They are elapsed times, not CPU samples, and
include scheduling delays and GC pauses. Run several times and distinguish a
cold cache from subsequent executions. Profiling itself adds some overhead,
especially for very small queries or many nested calls.

The existing summary's `allocations` and `allocated_bytes` are process-wide Go
runtime deltas, marked `allocation_scope=process`. They include profiler work
and can include unrelated goroutines; use isolated benchmarks for precise
allocation comparisons. Page reads and cache-hit counters are not measured and
are no longer printed as misleading zero values.

SELECT gets detailed stages. Other statements expose an `EXECUTE` total only,
including nested work. In particular, Go stored procedures can issue parallel
reads, which do not fit the synchronous stage accounting. `EXPLAIN ANALYZE`
on a write still performs the write under the normal authorization, rollback,
and durability rules. Failed or canceled statements return their normal error.

## Optimization driven by the analyzer

A reproducible workload sorts 20,000 DECIMAL prices:

```sql
EXPLAIN ANALYZE
SELECT id, amount FROM decimal_order ORDER BY amount, id;
```

The analyzer identified `SORT` as the dominant stage: about 87% of execution
time in isolated runs. Inspection of the decimal comparator found that it
copied the right-hand rational number before each comparison. `big.Rat.Cmp`
only reads its operands, so that copy was removed. Tests cover mixed numeric
inputs, exact large decimals, stable ties, bounded sorting, and preservation of
borrowed source values.

Measured with Go 1.27.3 on darwin/arm64, Apple M2 Max. Both binaries include the
analyzer; only the redundant decimal copy differs. Benchmark figures are
medians of three alternating 300 ms runs, `GOMAXPROCS=1`, `-cpu=1`, with no
concurrent builds or tests. Fixture construction and parsing are excluded.

| Measurement | Before | After |
| --- | ---: | ---: |
| Query time, without profiling | 39.69 ms | 30.86 ms |
| Allocated bytes per query | 42,096,969 | 37,355,501 |
| Allocations per query | 1,225,348 | 632,669 |
| Analyzer: SORT self time, observed range | 35.42–36.75 ms | 27.46–27.74 ms |
| Analyzer: total time, observed range | 40.48–42.07 ms | 32.02–32.34 ms |

That is about 22% less query time, 11% fewer allocated bytes, and 48% fewer
allocations for this workload. SORT remains the largest stage afterwards;
its absolute duration decreased. These results do not imply the same gain for
integer sorting or for other query shapes.

Reproduce the fixture, profile, and benchmark:

```sh
go test ./internal/engine -run '^TestAnalyzeDecimalOrder$' -v -count=1
GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkDecimalOrderAnalysis$' -benchmem -benchtime=300ms -count=3 -cpu=1

go test ./internal/engine ./internal/driver \
  -run 'Test(QueryProfile|ExplainAnalyze|SQLPreparedExplainAnalyze|DecimalComparison|DecimalOrder)' -count=1
```

## Exact comparison without temporary cross products

A second analyzer run still identified decimal sorting as the dominant stage.
The comparator now handles equal denominators directly and compares uint64
numerator/denominator magnitudes using exact 128-bit cross products. Sign and
zero handling happens first. Larger unequal fractions retain `big.Rat.Cmp`;
no floating-point conversion or mutation of borrowed inputs is involved.
The magnitude reader supports both 32-bit and 64-bit Go words.

The following comparison starts from the preceding copy-removal optimization,
with the analyzer enabled in both binaries but disabled during benchmarks.
Same machine and Go version; medians of three alternating 250 ms samples,
`GOMAXPROCS=1`, `-cpu=1`, no concurrent builds or tests. Host load varied in
the third sample, so treat the latency figures as local workload measurements.

| Measurement | Before | After |
| --- | ---: | ---: |
| Query time, 20,000 DECIMAL rows | 32.08 ms | 15.90 ms |
| Allocated bytes per query | 37,355,525 | 8,908,015 |
| Allocations per query | 632,669 | 40,013 |
| Small rational comparison | about 53 ns | about 12–13 ns |
| Allocations per small comparison | 2 | 0 |

That is a further 50% reduction in query time, 76% fewer allocated bytes and
94% fewer allocations for this fixture. A follow-up analyzer run measured
SORT at 10.87 ms (66.77% of execution), with result projection at 4.53 ms.

The range checks are not free: the arbitrary-precision controls with unequal
large numerators/denominators took roughly 5–8% longer (about 3–5 ns per
comparison), with unchanged allocations. Exact comparisons against `Rat.Cmp`
cover uint64 overflow boundaries, positive/negative values, zero values,
arbitrary-precision inputs, randomized fractions, and fuzz inputs.

```sh
GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkDecimalOrderAnalysis|BenchmarkDecimalComparison)$' \
  -benchmem -benchtime=250ms -count=3 -cpu=1
```
