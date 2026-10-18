# ORDER BY and aggregate allocation improvements

Measured on 2026-09-08 with Go 1.27.1, darwin/arm64, Apple M2 Max,
GOMAXPROCS=12. Baseline: commit `7606eb0`, compiled through a Go source overlay
with the same benchmark tests. Each comparison uses two 300 ms runs in separate
before/after test binaries, with fixture construction outside the timed section. Queries use in-memory tables; these numbers do not
measure file I/O or cold operating-system caches.

## Results

| Workload | Before | After | Allocated bytes before → after | Allocations before → after |
| --- | ---: | ---: | ---: | ---: |
| 20,000 rows, three-column ORDER BY, LIMIT 20 OFFSET 5 | 521–554 µs | 430 µs | 992,490 → 10,730 | 53 → 49 |
| 20,000 materialized rows, two-column ORDER BY, LIMIT 20 | 794–795 µs | 639–640 µs | 642,000 → 1,808 | 20,004 → 4 |
| 20,000 materialized rows, full two-column ORDER BY | 7.44–7.51 ms | 7.36–7.38 ms | 1,606,704 → 1,613,872 | 20,003 → 4 |
| 20,000 rows, two-column GROUP BY with COUNT and AVG | 1.087–1.089 ms | 0.992–0.993 ms | 288,745 → 288,776 | 3,543 → 3,543 |

A cold FTS control benchmark remained at about 10.3–10.4 ms before and after,
with unchanged allocation levels; this patch makes no cold-FTS speedup claim.
Timing varies between runs; allocated-byte and allocation-count reductions were
consistent in the initial and final comparisons.

The raw limited sort uses about 99% fewer allocated bytes and takes about 20%
less time. Full materialized sorting mainly benefits from fewer individual
allocations; total bytes are slightly higher because Go rounds a large backing
allocation differently from many small allocations. The materialized-row
benchmarks measure sorting only, excluding the cost of building those rows.

## Changes

- Multi-column bounded heaps return the discarded candidate's key slice, or the
  evicted root's key slice, for reuse. A retained row's keys are never overwritten.
  Key storage scales with LIMIT + OFFSET, with one additional candidate, instead
  of the number of scanned matches. Large retained sets use bounded allocation
  chunks. All candidate expressions still execute, preserving errors.
- Full sorting of materialized rows and window partitions packs keys into one
  backing allocation with disjoint, capacity-limited slices. Result maps remain
  independently owned. Input ordering and window row-index mappings retain their
  existing behavior.
- Materialized limited sorts no longer allocate an unused full-sort item slice
  alongside their heap, including the single-column path.
- Raw aggregate plans resolve direct column arguments once per execution.
  COUNT(column), SUM, AVG, MIN and MAX then read the stored column directly.
  Expressions and unresolved names retain the evaluator path; NULL handling,
  decimal arithmetic and range errors remain unchanged. Resolved positions live
  on the execution's table-bound plan, not a shared parsed SQL expression.

Raw top-N retains its existing tie behavior. Materialized top-N continues to
match stable full sorting followed by truncation. Regression tests compare
bounded and full results for NULLs, mixed sort directions, expressions,
LIMIT/OFFSET boundaries and retained sets crossing the key-arena chunk boundary.
Existing aggregate and window tests cover the other affected paths.

## Reproduction

### Large single-column result pages

Materialized one-column ORDER BY now uses the same strategy as multi-column
sorting: if LIMIT + OFFSET retains more than roughly 90% of the available
rows, sort once and truncate instead of building a heap and sorting it again.
The result keeps stable ordering for ties and NULLs. Smaller pages keep the
bounded heap. This limited-sort path leaves input rows in their original order.

A sorting-only benchmark with 20,000 rows and 19,950 retained rows measured
the following local medians with Go 1.27.2 on darwin/arm64, `-cpu=1`:

| Input | Before | After |
| --- | ---: | ---: |
| Already sorted | 46.54 ms | 1.37 ms |
| Reversed | 31.77 ms | 2.33 ms |
| Shuffled | 8.21 ms | 6.91 ms |

Both versions allocate four times per operation. The full sort needs about
8 KiB more for this page (about 1%). Repeated measurements under concurrent
CPU load varied substantially, so these figures describe this local workload
and do not establish a general query speedup. The benchmark excludes row-map
construction, SQL planning and storage I/O.

The baseline sorting kernel is from `736b9a2`; the comparison used a Go source
overlay with the same benchmark and other working-tree changes retained.

```sh
go test ./internal/engine -run 'TestSingleOrderLargePagesMatchStableReference|TestMaterializedSingleOrderLargePageSQL' -count=1
go test ./internal/engine -run '^$' -bench '^BenchmarkMaterializedSingleOrderLargePage$' -benchmem -benchtime=300ms -count=3 -cpu=1
```

### Other sort and aggregate workloads

```sh
go test ./internal/engine -run 'Test(Raw)?BoundedOrderKeysMatchFullSort' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkOrderByMultiColumnLimit|BenchmarkMaterializedOrderKeys|BenchmarkGroupByTwoColumns)$' -benchmem -benchtime=300ms -count=2
go test -race ./internal/engine -run 'Test.*(Order|Window|Aggregate|Rank|Lag|Lead)' -count=1
go test ./...
make test-sql-compat GO_TEST_FLAGS=-count=1
```

## GROUP BY and COUNT execution (2026-10-18)

Materialized aggregation now shares the typed group lookup and query-local
state arena used by raw aggregation. It no longer reserves a string-key map
for half the input rows or allocates every accumulator slice separately for
every group. This benefits grouped COUNT, SUM, AVG, MIN and MAX over subquery
and join results. Materialized and scalar raw grouping use a pair of strings
directly for two text grouping columns; other composite values and batch
grouping retain the existing framed encoding.

Direct-column COUNT queries with at least 2,048 candidate rows can also use
the batch kernel. Repeated COUNTs share extraction and validity masks, and an
ungrouped COUNT(column) counts set bits in those masks. Scratch memory scales
with the number of referenced columns and the fixed batch size, rather than
the table size.
COUNT(*) over an unfiltered physical table continues to read the table
cardinality directly. COUNT with an expression and COUNT(DISTINCT) keep their
separate execution paths.

COUNT(DISTINCT) uses exact integer keys while its non-NULL inputs are integers.
If another type occurs, it migrates once to the existing textual equivalence
rules. Mixed integer, REAL, decimal and text values therefore retain their
previous DISTINCT behavior. NULL handling, first-encounter group order,
integer overflow promotion and exact decimal SUM/AVG are unchanged.

The following allocation measurements used Go 1.27.2 on darwin/arm64
(Apple M2 Max). Fixture construction and SQL parsing are excluded. The
materialized cases use 20,000 input rows; DISTINCT uses 50,000 values.
Before values were recorded as the corresponding kernels from `736b9a2` were
replaced. Other working-tree changes were present, so these are development
measurements rather than comparisons of clean releases. The commands below
measure the current tree; the DISTINCT-set and COUNT-kernel benchmarks also
include the former textual or scalar implementation as a control.

| Workload | Bytes before → after | Allocations before → after |
| --- | ---: | ---: |
| Materialized COUNT/SUM, 50 text groups | 471,960 → 39,240 | 595 → 245 |
| Materialized COUNT/SUM, 20,000 text groups | 15,416,649 → 14,880,276 | 200,123 → 61,443 |
| DISTINCT integer set, 1,000 distinct values | 112,088 → 74,456 | 1,013 → 22 |
| DISTINCT integer set, 50,000 distinct values | 3,747,124 → 2,364,984 | 50,266 → 275 |
| Two-text-key lookup, 350 groups / 20,000 rows | 118,208 → 126,056 | 411 → 58 |
| Two-text-key lookup, 20,000 groups / 20,000 rows | 6,833,984 → 7,749,181 | 21,124 → 1,121 |

The text-pair map trades larger inline keys for fewer allocations: bytes can
increase by roughly 7–13% in these isolated lookup workloads. Its result
values and group states still have separate ownership. DISTINCT's integer
optimization reduces both allocated bytes and object count; mixed types pay
one migration rather than changing equality rules.

Timing was sensitive to concurrent system load. Same-process COUNT kernel
comparisons on 131,072 rows measured about 1.56 ms versus 0.75–0.77 ms for
COUNT(column), and 3.28–3.68 ms versus 1.04–1.07 ms for several COUNTs. These
compare the retained scalar implementation with the batch implementation;
they do not establish a general query speedup. Re-measure on the target
workload and machine before choosing latency budgets.

```sh
go test ./internal/engine -run 'TestCompositeGroupLookup|TestAggregateGroups|TestMaterializedGroupStore|TestAggregateCountBatches|TestDistinctCountInteger|TestCountDistinctIntegerSQL' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkMaterializedAggregate|BenchmarkAggregateCompositeLookup|BenchmarkDistinctIntegerSet|BenchmarkDistinctIntegerSQL|BenchmarkAggregateCountBatches|BenchmarkAggregateCountKernel)$' -benchmem -benchtime=300ms -count=3 -cpu=1 -p=1
```
