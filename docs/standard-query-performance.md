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

## Computed ordering and trim-length consumers (2026-11-11)

Computed raw ORDER BY terms now bind eligible arithmetic, conditional, NULL and
text expressions once per execution. Keys still evaluate only after a source
row passes its filter, in the original term order. Unsupported expressions
retain their interpreter individually, including the execution's statement
clock. Direct-column keys and specialized float/vector/full-text scorers retain
their previous paths. Sorting algorithms, key ownership, scan-order ties and
pagination are unchanged.

LENGTH/LEN/CHAR_LENGTH over TRIM/LTRIM/RTRIM also consume trimmed text without
boxing an intermediate string. The shared trim operands retain input coercion
before cutset evaluation, NULL short circuiting, fresh parameters and the
distinction between omitted and explicit empty cutsets. BLOB input to TRIM is
still coerced to text before counting characters; direct LENGTH(BLOB) retains
its byte count. Ordinary TRIM reuses unchanged string boxes, including explicit
empty cutsets.

The following allocation checks compare against `e3099a5` using a source
overlay for `builtin_string.go`, `raw_scalar.go` and
`exec_fastpath_select.go`. Both executables contain identical fixtures. Go
1.27.3, darwin/arm64, Apple M2 Max, GOMAXPROCS=2; medians of three runs with
three operations per fixture, excluding parsing and input loading. Trim-length
queries aggregate 50,000 rows; ordering fixtures scan 20,000 rows.

| Workload | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| SUM(LENGTH(TRIM(body,cut))), changed short text | 802,392 → 2,386 | 50,030 → 30 |
| Same query, unchanged text | 2,386 → 2,386 | 30 → 30 |
| Same query, explicit empty cutset | 802,386 → 2,386 | 50,030 → 30 |
| Same query, 300-character result | 1,202,386 → 402,386 | 100,030 → 50,030 |
| Arithmetic ORDER BY, LIMIT 20 | 330,152 → 330,328 | 40,092 → 40,097 |
| CASE ORDER BY, LIMIT 20 | 9,864 → 10,088 | 54 → 61 |
| Direct-column ORDER BY, control | 9,037 → 9,037 | 51 → 51 |

Trimming that changes short strings saves about 800 KB and 50,000 temporary
objects per query. Long length results still box their numeric values, and
input coercions can still allocate. Computed ordering adds a few binding
allocations per query; it does not remove arithmetic-result boxing.

A longer alternating timing comparison was stopped after unrelated machine
load exceeded 380 and unchanged controls varied severalfold. These records
therefore make no timing-speedup claim. Allocation counts were confirmed with
the short fixed-operation runs above and a zero-allocation per-row regression
test. They do not establish a reduction in peak live memory.

Differential tests cover NULLs, typed nils, BLOBs, Unicode/invalid UTF-8,
coercion order, cutset parameters, full/Top-N/deep ordering, mixed keys, errors
after filtering, clocks, concurrent schema reuse and source ownership. The full
Go suite, targeted race checks, SQL-package compatibility, go vet and all four
WASM profiles pass. Public WASM checks combine computed ordering with trimmed
lengths and also verify their aggregate result.

```sh
go test ./internal/engine -run '^Test(BoundTrimLength|RawOrderValues)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^Benchmark(TrimLength|RawOrderValues)$' -benchmem -benchtime=3x -count=3
```

## WHERE text comparisons and IN-list setup (2026-11-11)

Raw WHERE comparisons now consume TRIM/LTRIM/RTRIM text directly, including
reversed operands and comparisons between two trims. This avoids boxing a
temporary string for each changed value. Both operands still evaluate in
written order before NULL excludes a row. Non-string counterparts retain the
existing comparison rules, including decimal-left text conversion. Parameters
remain fresh, and unsupported operands fall back to the existing evaluator.
The filter stays in the expression tier so cheap AND/OR guards keep their
previous order. NOT and IS NULL retain SQL UNKNOWN through their existing
value evaluators.

Literal IN-list setup now allocates only the typed sets actually required by
its values. Membership checks, exact integer comparisons, integer/float
matching, singleton comparisons and NULL/NOT IN behavior are unchanged.

These measurements compare the working tree immediately before these WHERE
changes with the resulting tree, using an overlay of `exec_raw_filter.go`.
Earlier trim-length and ordering improvements above are held constant. Both
executables contain identical benchmark fixtures. Go 1.27.3, darwin/arm64,
Apple M2 Max, GOMAXPROCS=2. Queries count 50,000 rows; parsing and input loading
are outside the measured region.

Timing samples run in before/after/after/before order, three 200 ms samples
per workload in each round. The table shows the medians of all six samples.
The machine load had fallen from the earlier overloaded session; the plain
comparison control differs by 0.6%. These are local measurements, not a
guarantee for other queries or systems.

| WHERE predicate | Time/op before → after | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| TRIM(body,cut) = 'α🙂' | 1.959 → 1.089 ms | ~802,393 → ~2,282 | 50,029 → 27 |
| 'α🙂' = TRIM(body,cut) | 1.952 → 1.063 ms | 802,395 → 2,281 | 50,029 → 27 |
| TRIM(body,cut) = TRIM(other,cut) | 3.403 → 1.872 ms | ~1,602,550 → ~2,379 | ~100,033 → 29 |
| TRIM(body,cut) = 'missing' | 1.950 → 1.017 ms | ~802,383 → 2,273 | 50,028 → 26 |
| body = 'missing', control | 0.268 → 0.269 ms | 2,128 → 2,128 | 22 → 22 |

The trim fixtures use changed short text. Unchanged string inputs already
reuse their original boxes, and non-string input coercion can still allocate.
The new string comparison callbacks have a zero-allocation regression check.

Separate IN setup checks use three runs of 1,000 operations each. Text-only
lists fall from 480 B / 6 allocations to 336 B / 3 allocations; integer and
float lists from 560 B / 7 to 464 B / 5; NULL-only and empty lists from
272 B / 5 to 80 B / 1. Mixed-domain and singleton controls are unchanged.
These are per-filter construction costs, not per-row savings.

Differential tests cover all comparison operators, Unicode and invalid UTF-8,
typed nils, mixed types, exact integer boundaries, live parameters, coercion
side effects, short rows and error order. Query checks cover materialized,
streamed and columnar scans, SQL UNKNOWN, cheap guards, UPDATE, DELETE and
index consistency. The root Go suite, targeted race checks, SQL-library
compatibility, go vet and full/minimal browser/Node WASM checks pass.

```sh
go test ./internal/engine -run '^Test(RawTrimComparison|InFilterDomains)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkWhereTrimComparisons$' -benchmem -benchtime=200ms -count=3
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkInFilterSetup$' -benchmem -benchtime=1000x -count=3
```

## UNION alignment and INTERSECT membership (2026-11-11)

Compound SELECT terms now normalize their positional source/target column
keys once before alignment. UNION, UNION ALL, EXCEPT and INTERSECT retain the
left-most names, missing-value NULLs, duplicate-target overwrite order and
fresh output maps. The same-name no-op still borrows the existing term rows.

INTERSECT consumes each private right-hand membership key on its first left
match, eliminating the separate seen set. Its result buffer is bounded by
the smaller of the left count and distinct right count. All left signatures
still evaluate after membership is exhausted, preserving callback order and
cancellation. Numeric/NULL set equality, first-left occurrence order, term
evaluation and compound ORDER BY/LIMIT boundaries are unchanged.

Allocation checks compare the working tree before this follow-up with the
result, overlaying only `exec_setops.go`; earlier changes remain fixed. Go
1.27.3, darwin/arm64, Apple M2 Max, GOMAXPROCS=2; medians of three runs of three
operations. Parsing and source loading are excluded. Aliased compound queries
read two 20,000-row terms with four columns; INTERSECT has 20,000 left rows.

| Fixture | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| Align 20,000 rows, four mixed-case aliases | 9,123,877 → 6,884,117 | 200,001 → 40,011 |
| Same alignment, lowercase aliases | ~6,883,882 → ~6,884,005 | 40,001 → 40,003 |
| Complete aliased UNION | 39,938,829 → 37,699,064 | 460,097 → 300,107 |
| Complete aliased UNION ALL | 38,259,976 → 36,020,221 | 440,026 → 280,036 |
| INTERSECT, 10 right rows | 1,038,141 → 621 | 90 → 15 |
| INTERSECT, 1,000 right rows | 1,102,618 → 68,048 | 2,072 → 1,007 |

Lowercase alignment pays two small key slices per term; mixed-case aliases
avoid per-cell normalization allocations. These short runs measure allocation
volume, not latency or peak live memory. Differential tests cover Unicode and
invalid UTF-8 aliases, duplicate names, sparse rows, mixed values, ownership,
concurrent alignment and cancellation after membership is exhausted. Native
and WASM checks cover mixed UNION/UNION ALL chains and INTERSECT.

```sh
go test ./internal/engine -run 'Test.*(Union|SetOperation|Intersect)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^Benchmark(UnionAlignmentBoundKeys|UnionAliasedQuery|IntersectMembershipSubset)$' \
  -benchmem -benchtime=3x -count=3
```
