# Standard SQL execution

The raw SELECT and mutation paths bind arithmetic and comparison expression
trees to column positions once, instead of resolving column names on every
input row. SELECT projections, WHERE predicates, DISTINCT, streaming and
columnar results share these evaluators. UPDATE also binds SET expressions
when at least 64 candidate rows can amortize the setup cost; smaller candidate
sets retain the interpreter. Its bound SET evaluators belong to the execution,
without modifying the statement's DML plan.

The compiler supports arithmetic and comparison trees over columns, literals
and the existing static conversion expressions. Unsupported expressions retain
the interpreter. Literals used as parameters are read afresh. Arithmetic and
comparison still call the existing SQL operations, preserving NULL, exact
decimal arithmetic, integer overflow promotion, string concatenation and error
messages. Operands execute in their original order. There is no new constant
folding, predicate algebra or reordering of AND/OR terms.

Raw ORDER BY comparisons now borrow candidate records rather than copying
both records on every comparison. Sort keys, NULL placement, scan-order tie
breaking, bounded heaps and result ownership retain their previous behavior.

## Measurements

Measured on 2026-11-10 with Go 1.27.3, darwin/arm64, Apple M2 Max and
GOMAXPROCS=12. The baseline is `2ea1df5`, compiled using a Go source overlay
for the four changed executor files and the same benchmark fixtures. Results
are medians of five serialized 300 ms samples before and after the change.
Fixtures are in memory and statements are parsed before timing. Numeric cells
in the 20,000-row scan fixtures are float64 values; integer and decimal
compatibility are tested separately.

| Workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Three-column ORDER BY, full result | 10.937 ms | 9.781 ms | 9,391,286 → 9,391,280 | 40,012 → 40,012 |
| SELECT id + val * 2 | 3.275 ms | 2.619 ms | 7,204,477 → 7,204,475 | 80,003 → 80,003 |
| SELECT val * 2 + id > 20000 | 3.319 ms | 2.789 ms | 7,204,477 → 7,204,474 | 80,003 → 80,003 |
| COUNT WHERE val * 2 + id > 20000 | 1.448 ms | 0.860 ms | 323,344 → 322,800 | 40,026 → 40,027 |
| UPDATE all 20,000 rows SET val = val + 1 | 1.870 ms | 1.599 ms | 3,597,974 → 3,597,542 | 20,413 → 20,417 |
| Arithmetic UPDATE by primary key | 1.674 µs | 1.675 µs | 2,123 → 2,123 | 29 → 29 |
| Plain projection control | 2.071 ms | 1.946 ms | 6,884,484 → 6,884,486 | 40,005 → 40,005 |
| Unfiltered COUNT(*) control | 1.190 µs | 1.105 µs | 2,176 → 2,176 | 21 → 21 |
| Float ORDER BY LIMIT 20 control | 0.375 ms | 0.385 ms | 8,441 → 8,441 | 47 → 47 |

The principal medians correspond to roughly 41% less time for the computed
WHERE, 20% for the arithmetic projection, 15% for the bulk UPDATE and 11% for
the three-column sort. Control timings also moved by about 2–7%, so these
percentages should not be treated as portable predictions. No speedup is
attributed to plain projections, unfiltered COUNT or the existing unboxed
float Top-N path. Arithmetic point updates retain their allocation count,
allocated bytes and overlapping timing ranges (1.63–1.72 µs before,
1.61–1.71 µs after). The new bound bulk-UPDATE evaluators cost four extra
allocations per statement, rather than allocating them per row.

These are execution measurements, excluding parsing, persistence and client
transfer. Result map materialization and boxed intermediate numbers still
account for most allocations; the expression binding change primarily reduces
CPU work. It does not establish a general reduction in peak live memory or a
speedup for arbitrary SQL shapes.

## Computed aggregate arguments

Eligible raw COUNT/DISTINCT, SUM/AVG and MIN/MAX arguments also reuse the
expression binder. Direct columns and the unboxed single-operation SUM/AVG
kernel retain priority. Computed evaluators belong to the execution. Trivial
literal arguments, unsupported functions, statement-clock expressions and
dynamic conversion declarations retain their previous evaluation paths.
Conversion validation keeps its existing rules and error order. Aggregate
filters still run before their arguments.

The following comparison isolates this extension against the preceding
working tree. The before build overlays only `exec_fastpath_aggregate.go`,
whose source snapshot has SHA-256
`84673291ca1d509212bf39eea80985252a640adb39a0137a4dd993f7545dc79f`.
The same machine and Go configuration as above runs five alternating
before/after process pairs, with 300 ms per fixture. Inputs contain 20,000
native integer/float rows and parsing is excluded.

| Workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| SUM((a + b) * 2) | 1.351 ms | 0.934 ms | 319,226 → 319,404 | 39,650 → 39,655 |
| SUM(a * 2), unboxed control | 0.325 ms | 0.319 ms | 2,130 → 2,130 | 23 → 23 |
| SUM(a * b), AVG(a + b), unboxed control | 0.468 ms | 0.469 ms | 2,338 → 2,354 | 26 → 26 |
| SUM(a), AVG(f), direct-column control | 0.186 ms | 0.185 ms | 11,752 → 11,766 | 26 → 26 |

The nested SUM uses about 31% less time in this fixture. Binding adds five
allocations per statement; intermediate numeric boxing remains. Some control
plans allocate a few more bytes because projections carry one extra function
field, while their allocation counts are unchanged. No timing gain is claimed
for the control queries.

## Verification

Differential tests compare compiled values and errors against the interpreter
for NULL, integer limits, large integers, REAL, NaN/Inf, decimal, text, boolean
and BLOB inputs. They also cover missing columns, short rows and parameter
rebinding. Query tests compare materialized, DISTINCT, streamed and columnar
results, excluded and skipped-row errors, and concurrent reuse of a parsed
statement. UPDATE tests exercise both sides of the binding threshold, index
maintenance and rollback after earlier rows have changed. Existing ordering
tests cover stable ties, NULL placement, mixed keys, pagination and heap key
ownership. Aggregate tests also compare COUNT/DISTINCT, SUM/AVG and MIN/MAX
against the general evaluator for nested arithmetic and conversions, decimals,
filter laziness, parameter changes, clocks and concurrent schema reuse. WASM
checks the aggregate results through both public modules and build profiles.

```sh
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(OrderByMultiColumnNoLimit|OrderBySingleColumnNoLimit|OrderByWithLimit|OrderByMultiColumnLimit|StandardExpressions|UpdateAllRows|UpdateByPrimaryKey|UpdateArithmeticByPrimaryKey|InsertBatchRows|SelectProjectedFullScan|UngroupedCountStar)$' \
  -benchmem -benchtime=300ms -count=5
go test ./internal/engine -run '^$' -bench '^BenchmarkAggregateBinary$' \
  -benchmem -benchtime=300ms -count=5
go test ./...
go test -race ./internal/engine ./internal/driver ./bindings/c \
  -run 'Test.*(Aggregate|CompiledRawValue|CompiledUpdateAcross|CompiledRawConversion|Order|Sort|Limit|TopN|Update|Delete|Arithmetic|Integer|Decimal|Parameter|Prepare|Columnar|Stream)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```
