# CTE row-alignment performance

CTE column aliasing and recursive row alignment now calculate normalized source
keys, target keys and qualified names once per result shape, outside the row
loop. Result maps are sized for their known column counts. This avoids repeated
string allocation without adding a result cache or changing recursive working
set semantics, NULL handling, aliases or source-row ownership.

The isolated `BenchmarkRecursiveCTEAlignment` uses 20,000 rows and three renamed
columns. On Apple M2 Max, Go 1.27.1, darwin/arm64, three-run medians were:

| Metric | Before | After |
| --- | ---: | ---: |
| Alignment time | 16.45 ms | 6.81 ms |
| Allocations | 160,001 | 40,010 |
| Bytes allocated | 7,843,866 | 6,884,042 |

That is about 59% less time and 75% fewer allocations in this alignment step.
Wall-clock results varied across runs; these figures are not a general speedup
claim for all CTE queries. Recursive-member execution and row materialization
still contribute to complete query time.

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkRecursiveCTEAlignment$' -benchmem -count=3 -benchtime=500ms
go test ./internal/engine -run 'TestCTE|TestRecursive'
```
