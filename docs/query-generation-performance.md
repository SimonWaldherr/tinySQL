# Query generation performance

`ToSQL` writes literal values directly into the statement's `strings.Builder`.
Strings escape quotes while being appended; BLOBs are hex-encoded in bounded
stack chunks; numeric and timestamp formats use append APIs. Uncommon fallback
values preserve the existing `%v` rendering. Small integers retain strconv's
allocation-free cached-string path.

INSERT estimates capacity before rendering the batch and writes column lists
without an intermediate joined string. UPDATE and DELETE also reserve capacity;
UPDATE assignments retain their deterministic sorted order. Public builder APIs,
SQL spelling and the serializer's existing supported syntax remain unchanged.

Measured on 2026-09-08, Go 1.27.1, Apple M2 Max, GOMAXPROCS=12, two 300 ms runs:

| Workload | Before | After | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| INSERT, 100 rows of integer/quoted text/BLOB | 15.53–15.68 µs | 8.09–8.11 µs | 24,328 → 6,784 | 413 → 1 |
| Nested SELECT | 342–343 ns | 351–352 ns | 352 → 352 | 1 → 1 |

The INSERT workload takes about 48% less time and allocates about 72% fewer bytes.
The already optimized nested SELECT retains its single allocation, with a small
measured timing increase. These benchmarks serialize prebuilt statements and do
not include builder construction, parsing, SQL execution, or file I/O.

Compatibility tests compare streamed literals with the previous formatting
contract, including quotes, Unicode/NULs, BLOB chunk boundaries, fractional
seconds, timezone offsets, floating-point extremes and signed integer limits.
An execution test reparses and inserts generated multi-row SQL with quoted text
and a BLOB spanning several encoder chunks.

```sh
go test . ./tinyorm
go test . -run '^$' -bench '^BenchmarkBuilder' -benchmem -benchtime=300ms -count=2
go test -race . -run 'Test.*(Builder|GeneratedInsert)' -count=1
go vet .
```
