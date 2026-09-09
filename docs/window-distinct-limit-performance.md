# Window functions, DISTINCT and LIMIT

Measured on 2026-09-08, Go 1.27.1, Apple M2 Max, darwin/arm64,
GOMAXPROCS=12. Two 200 ms runs per workload, compared with the workspace state
before these changes (including the preceding sort-key and aggregate work).
Fixtures and parsed SQL are outside the timed section. These are in-memory
execution benchmarks, not disk-I/O measurements.

| Workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| ROW_NUMBER, 5,000 rows, 50 text partitions, ordered | 9.49–9.51 ms | 3.27–3.45 ms | 5,975,809 → 5,923,102 | 36,179 → 36,138 |
| LAG + LEAD, same partitions | 10.31–10.46 ms | 4.33–4.37 ms | 6,015,910 → 5,963,216 | 41,180 → 41,139 |
| ROW_NUMBER OVER (), 5,000 rows | 2.30–2.31 ms | 2.11–2.15 ms | 5,310,035 → 5,120,665 | 34,787 → 34,763 |
| DISTINCT text, 20,000 rows, 50 values | 451–459 µs | 316–322 µs | 22,426 → 21,882 | 168 → 120 |
| DISTINCT text ORDER BY DESC LIMIT 10 OFFSET 5 | 7.54–7.87 ms | 322–323 µs | 20,930,812 → 22,522 | 120,103 → 123 |
| Filtered LIMIT 10 OFFSET 15000, 20,000 rows | 1.49–1.50 ms | 132–133 µs | 5,166,862 → 4,040 | 30,025 → 25 |

## Execution changes

**Window partitions:** A direct `PARTITION BY` column containing only strings
is indexed in one pass per shared OVER shape. Building each partition then
reads its row positions, rather than filtering the whole input again. Sorted
partitions remain shared across functions, so ROW_NUMBER, RANK, LAG, LEAD and
other window functions benefit. NULLs, mixed types, non-text values and computed
partition expressions retain the existing comparison-based path. The index is
execution-local and cannot survive data changes between queries.

`ROW_NUMBER() OVER ()` uses the filtered input position directly. Duplicate rows
receive separate numbers. Outer LIMIT/OFFSET still applies after numbering.

**DISTINCT:** Eligible ORDER BY terms that name projected outputs now sort after
raw-value deduplication. Only distinct rows become result maps, and LIMIT/OFFSET
applies after deduplication and ordering. Hidden ordering keys and colliding
projection names retain the general path. Ordered DISTINCT still scans all
candidates; early termination remains restricted to unordered DISTINCT.

For a single string projection, deduplication uses the original immutable string
as a map key. A separate set holds framed non-text keys, preserving type
boundaries even when text looks like an encoded NULL, integer or boolean.

**Filtered pagination:** Matching rows below OFFSET still evaluate their
projections, including errors, but no longer allocate discarded result maps.
Result capacity follows LIMIT rather than LIMIT + OFFSET. The filter still
scans candidates to locate the requested page; this is not an index seek.

## Validation and reproduction

Regression tests compare ordered DISTINCT and filtered pagination with execution
through a FROM subquery, preserving an independent general path. Window tests
compare indexed partitions with the existing filter/sort implementation, including
mixed numeric values and NULL fallback. Tests also cover duplicates, type-key
collisions, empty results, page boundaries and division errors in skipped rows.

```sh
go test ./internal/engine -run 'Test.*(Window|Distinct|Limit|Offset|Rank|Lag|Lead|Partition|RowNumber)' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkWindowRowNumberUnordered|BenchmarkWindowRowNumberManyPartitions|BenchmarkWindowLagLeadManyPartitions|BenchmarkDistinctOrderedLimit|BenchmarkSelectDistinctFewGroups|BenchmarkFilteredDeepOffset)$' -benchmem -benchtime=200ms -count=2
go test -race ./internal/engine -run 'Test.*(Window|Distinct|Limit|Offset|Rank|Lag|Lead|Partition|RowNumber)' -count=1
go test ./...
make test-sql-compat GO_TEST_FLAGS=-count=1
```
