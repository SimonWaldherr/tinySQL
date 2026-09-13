# SQL execution: August 13 versus September 13, 2026

Baseline: `185c8533d754d4337daa0ba90f2344918c62b988`, the last first-parent
commit on August 13 (22:53:03 +02:00). Current:
`1ae111c84628ec6e15cc3b66054a8d80ac21ff8f`. This measures all intervening
changes, not just the latest columnar work.

Both trees are extracted using `git archive` into separate temporary directories.
The same [harness](columnar_benchmark_test.go.txt) is copied into
`internal/engine/columnar_benchmark_test.go` in both. No historical production
code is modified. The existing Row API is used on both revisions; the new
Column API has no counterpart in the old revision.

## Results

| Query | August median | September median | Ratio of median times |
|---|---:|---:|---:|
| SUM, width 4 | 39.038 ms | 0.830 ms | 47.1× faster |
| SUM/AVG/COUNT, width 4 | 48.121 ms | 1.214 ms | 39.6× faster |
| SUM/AVG/COUNT, width 32 | 312.255 ms | 1.757 ms | 177.7× faster |
| GROUP BY, 16 groups, width 4 | 5.963 ms | 3.219 ms | 1.85× faster |
| GROUP BY, 16 groups, width 32 | 6.221 ms | 3.750 ms | 1.66× faster |
| Sparse Row projection, width 4 | 0.779 ms | 0.817 ms | 1.05× slower |
| Dense Row projection, width 4 | 12.152 ms | 12.094 ms | approximately unchanged |

For width-32 SUM/AVG/COUNT, allocation falls from 660,890,920 to 12,072 B/op,
and allocations from 524,637 to 97. These are cumulative allocations per query,
not a claim about process RAM. The indexed point aggregate falls from 40.914 ms
to 2.97 µs on this fixture; the percentage table rounds this extreme reduction
to -100.0%, but execution time is nonzero. This is a query-specific result, not
a general database speedup or a measurement of index construction.

The sparse width-4 Row projection regression is measurable: paired time ratio
+5.0%, 95% interval [+4.2%, +5.7%]. Dense projections and the wide sparse
projection have intervals spanning no change. GROUP BY with 16 groups is faster
but allocates more: 14,288 → 17,232 B/op at width 4, and 20,496 → 21,616 at
width 32. Large improvements therefore do not apply uniformly.

[Equivalent-result comparisons](comparison-equivalent.md) contain all 19
matching workloads, confidence intervals, byte counts and allocation counts.
[Cases with changed SUM semantics](comparison-semantic-change.md) separately
include the empty table, which rises from 1.28 to 1.79 µs (+41.4% paired), and
many-group queries, which show no clear timing improvement and about 2.1 MB
more allocation. Do not treat those five cases as equivalent-result speedups.
The [complete raw comparison](comparison-all.md) retains all rows for inspection.

## Method

Same Apple M2 Max, Go 1.27.1 darwin/arm64, GOMAXPROCS=1, GOGC=100.
Eight paired rounds alternate which binary runs first, with 300 ms per sample
and allocation reporting. Builds and result checking finish before measurement;
no other agent builds, tests or benchmark analysis run concurrently. The computer
is not dedicated or thermally controlled. The compiled binaries and harness are
identified by SHA-256 in [environment.json](environment.json).

Fixtures contain 131,072 rows at widths 4 and 32, plus empty/small inputs.
Values, NULL distribution and predicates are identical. Padding shares immutable
text; it is not a unique payload per cell. Setup, SQL parsing and index/cache
warmup are outside timing. This measures warm in-memory query execution, not
parsing, cold disk I/O, ingestion, throughput under concurrent load, or peak RSS.
B/op reports allocations during execution, not retained memory.

Before timing, the harness compares complete result multisets and column lists
using canonical JSON and SHA-256. Row order is ignored because these queries
have no ORDER BY. Nineteen cases match exactly. Five differ only because empty
or all-NULL SUM returns 0 in the old revision and NULL in the current revision:
`width4/filter0`, `width32/filter0`, `small0`, and both `group_many` cases.
The runner checks that converting only those NULL sums to zero accounts for
all differences. [semantic-differences.json](semantic-differences.json) records
the affected output counts. This is a compatibility caveat, not a performance
improvement; these five cases are separated from the equivalent-result table.
The check covers JSON-visible results, not every internal Go value type.

## Reproduction

Extract each named commit into a separate temporary directory using
`git archive COMMIT | tar -x -C DIRECTORY`. Copy the supplied harness to
`DIRECTORY/internal/engine/columnar_benchmark_test.go` in both trees, then
run from each tree (choose `before` or `after` for its binary):

```sh
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/tinysql-month-before.test
```

From the final repository, run:

```sh
python3 -B docs/benchmarks/month-2026-08/run.py /tmp/tinysql-month-before.test /tmp/tinysql-month-after.test /tmp/tinysql-month-results
python3 -B docs/benchmarks/columnar/compare.py /tmp/tinysql-month-results/paired-before.txt /tmp/tinysql-month-results/after.txt
```

The runner aborts on unexpected result differences or incomplete coverage.
Raw samples and correctness logs are included here. Comparison uses median
latencies/allocations and paired geometric mean time ratios, with seeded
10,000-resample bootstrap 95% intervals. The intervals describe run variability
on this machine, not portability to other data or hardware.
