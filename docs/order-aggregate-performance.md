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

```sh
go test ./internal/engine -run 'Test(Raw)?BoundedOrderKeysMatchFullSort' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkOrderByMultiColumnLimit|BenchmarkMaterializedOrderKeys|BenchmarkGroupByTwoColumns)$' -benchmem -benchtime=300ms -count=2
go test -race ./internal/engine -run 'Test.*(Order|Window|Aggregate|Rank|Lag|Lead)' -count=1
go test ./...
make test-sql-compat GO_TEST_FLAGS=-count=1
```
