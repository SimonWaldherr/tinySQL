# CTE projections and indexed SELECT allocation (2026-09-09)

Simple materialized-CTE scans now accept direct projections qualified by the
CTE name or FROM alias. The qualifier is resolved once before scanning; the
consumer reads the materialized result directly, avoiding intermediate maps
with qualified keys for every source row. Output names, explicit aliases and
last-value behavior for duplicate output names remain intact.

The original optimization required direct column projections without sorting,
DISTINCT, aggregation, windows, joins or compound operations. Follow-ups added
[qualified WHERE binding](delete-cte-materialization-performance.md) and
[ordered direct-column consumers](cte-order-performance.md). Unknown qualifiers
and ambiguous dotted CTE output columns retain the general execution path.
CTE definitions are still materialized;
this change does not inline them or push predicates into their definitions.
Recursive CTE evaluation rules are unchanged.

Normal SELECT allocation estimates now use the number of index candidates
when available. An indexed lookup returning one candidate with LIMIT 10000
therefore reserves space for at most one result row, rather than 10000.

## Measured results

Apple M2 Max, Go 1.27.1, darwin/arm64, GOMAXPROCS=12; two 200 ms samples.
These are warm in-memory benchmarks with parsed statements reused.

| Query | Before | After | Bytes/op before → after |
|---|---:|---:|---:|
| CTE with 20,000 source rows, qualified projection, simple filter, LIMIT 20 | 9.36–9.43 ms | 2.29–2.31 ms | 23,853,532 → 6,891,956 |
| Indexed one-row SELECT with LIMIT 10000 | 12.87–12.90 µs | 0.99–1.01 µs | 82,984 → 1,072 |

CTE allocation counts fall from 140,027 to 40,053. Indexed SELECT allocation
counts stay at 14; it is the oversized backing array that disappears. The index
fixture stores actual ints in its INT column so index eligibility is exercised.
The CTE timing includes materializing its definition. These specific gains do
not imply the same improvement for unfiltered scans or arbitrary CTE queries.

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkCTEQualifiedProjection|BenchmarkSelectIndexedSmallResult)$' \
  -benchmem -benchtime=200ms -count=2
go test ./...
go test -race ./internal/engine \
  -run 'Test.*(CTE|Select|SecondaryIndex)' -cpu=1,36 -count=1
go vet ./internal/engine
```

Differential tests compare against derived-table execution for qualified aliases,
NULLs, duplicate output names, empty results, pagination, qualified-predicate
fallback and repeated execution of the same parsed query. Indexed SELECT tests
cover duplicates, missing keys, ordering, OFFSET and LIMIT 0. GOMAXPROCS=36
oversubscribes this 12-core host; it does not represent 36 physical cores.

## Follow-up: adaptive filtered SELECT buffer

For unordered SELECTs whose WHERE predicate still needs evaluation, LIMIT is
no longer treated as a predicted result count. The output slice starts with at
most 64 entries. If 64 results arrive within the first 128 candidates, it grows
once to the original bounded capacity estimate; otherwise normal slice growth
follows actual results. Evaluation order, error propagation, cancellation and
OFFSET/LIMIT behavior are unchanged.

On the same 20,000-row fixture, with LIMIT 10000 (two 200 ms samples):

| Result density | Bytes/op before → after | Allocations before → after | Time before → after |
|---|---:|---:|---:|
| One matching row | 82,856 → 1,448 | 7 → 7 | 116.6–116.8 µs → 118.9–160.6 µs |
| First 10,000 rows match | 3,442,540 → 3,443,048 | 20,005 → 20,006 | 1.00–1.02 ms → 1.05–1.08 ms |

The sparse case saves about 98% of allocated bytes. These samples establish
no latency gain; one sparse sample was notably slower. Dense queries pay one
small temporary buffer and showed several percent latency overhead. The
heuristic can overestimate later matches if only the early rows are dense;
it never reserves more than the original estimate plus the initial small buffer.
A uniformly small buffer was also measured and rejected because repeated growth
increased dense-query allocation to about 3.67 MB/op.

Reproduce with `-bench '^BenchmarkSelect(Sparse|Dense)LargeLimit$'`. Differential
coverage includes early/late dense matches, sparse/empty matches, limits around
the 64-row boundary, LIMIT 0 and OFFSET against derived-table execution.

## Aggregate consumers and bounded recursive alignment (2026-11-11)

Single-source CTE consumers can now aggregate immutable materialized rows
without first allocating a qualified map for every input row. The consumer
binds source names once, then uses the existing row WHERE and aggregate
evaluators for COUNT/SUM/AVG/MIN/MAX with direct or literal arguments and
direct GROUP BY columns. Filtering still completes before aggregation; sorting
and pagination retain their original output names and order.

Binding copies changed AST nodes and retains parameter pointers. Eligibility
is checked with the original names before resolving qualifiers, preserving
empty-result metadata and evaluation order for previously general aggregates.
A required-key pass precedes value evaluation so sparse maps retain qualified
diagnostics without replaying host Stringer or JSON callbacks after an error.
Direct borrowing is limited to callback-free storage values: built-in scalars,
BLOBs, vectors, times and decimals. Custom or nested composite source values
and callback-capable literals/parameters retain the general executor, whose
qualified row maps snapshot values before host callbacks can change later rows.
Computed arguments, HAVING, DISTINCT, windows, joins, dotted source columns
and other unsupported forms retain their existing paths. Producer results
remain materialized once; consumer limits and filters do not enter producers.

Bounded recursive UNION ALL now limits row alignment to the remaining compound
LIMIT/OFFSET work budget. The recursive SELECT and its column-count validation
still complete first, including errors in rows later discarded. This removes
unneeded alignment maps; recursive SELECT results themselves remain materialized.
UNION DISTINCT and unbounded recursion retain their existing behavior.

Allocation checks use the working tree immediately before these CTE changes as
the baseline, with source overlays for `exec_select.go` and `exec_cte.go`.
Earlier uncommitted WHERE/text/ordering changes are held constant. Both
executables contain identical fixtures. Go 1.27.3, darwin/arm64, Apple M2 Max,
GOMAXPROCS=2; three runs of three operations, with parsing and fixture setup
outside the measured region. Consumer fixtures use 20,000 pre-materialized
rows; the complete query includes producing its CTE.

| Fixture | Allocated bytes/op before → after | Allocations/op before → after |
| --- | ---: | ---: |
| COUNT(*) consumer | ~6.89 MB → ~1.2 KB | ~40,024 → 19 |
| SUM/AVG/MIN/MAX consumer | ~13.45 MB → ~2.7 KB | ~80,037 → ~45 |
| Filtered grouped consumer | ~13.55 MB → ~107 KB | 80,179 → 184 |
| Complete producer + grouped consumer | ~20.45 MB → ~7.01 MB | ~120,228 → ~40,232 |
| Recursive fan-out, LIMIT 8 OFFSET 2 | ~28.14 MB → ~21.26 MB | 160,092 → 120,110 |
| Unbounded fan-out, control | ~42.15 MB → ~42.15 MB | 240,092 → 240,092 |

These measure allocation volume, not peak live memory. COUNT still scans its
materialized input. Grouping, input values and aggregate results may allocate.
The recursive reduction saves 39,982 allocations from discarded row maps in
this fixture, without avoiding the producer's work.

Exploratory alternating timing runs showed lower times for the optimized
paths, but the unchanged expression-aggregate control ranged from about
3.8 to 6.1 ms across combined and isolated rounds. These records therefore make
no latency-speedup claim. Unsupported expression arguments are rejected before
allocating a CTE binding or formatting projection names.

Differential checks compare complete Row maps, column metadata and errors
against the qualified-source pipeline. Coverage includes empty/NULL/mixed
inputs, exact decimals, aliases, hidden/unknown names, fresh parameters,
callback order, sparse maps, pagination, cancellation, concurrent statement
reuse, source ownership, sibling reuse and recursive errors past the work
limit. Root-suite validation, targeted race checks, SQL-library compatibility,
go vet and full/minimal browser/Node WASM checks completed. The final root run
hit temporary ENOSPC errors in five persistence tests; all five passed on retry
after temporary benchmark binaries were removed. Other root tests passed.

```sh
go test ./internal/engine -run 'Test.*(CTEAggregate|CTEBounded)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^Benchmark(CTEAggregateConsumer|CTEAggregateQuery|RecursiveCTEBoundedFanout)$' \
  -benchmem -benchtime=3x -count=3
```
