# CTE projections and indexed SELECT allocation (2026-09-09)

Simple materialized-CTE scans now accept direct projections qualified by the
CTE name or FROM alias. The qualifier is resolved once before scanning; the
consumer reads the materialized result directly, avoiding intermediate maps
with qualified keys for every source row. Output names, explicit aliases and
last-value behavior for duplicate output names remain intact.

Eligibility still requires direct column projections, no sorting, DISTINCT,
aggregation, window functions, joins or compound operations. Qualified WHERE
references, unknown qualifiers and ambiguous dotted CTE output column names
retain the general execution path. CTE definitions are still materialized;
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
