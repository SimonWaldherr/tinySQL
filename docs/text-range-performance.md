# Text, range-filter and aggregate performance

## Changes

Quoted SQL strings and identifiers scan byte spans between quote delimiters.
Unescaped values remain slices of the SQL source; escaped values count doubled
quotes before allocating one output buffer. Invalid UTF-8 retains the previous
replacement of each malformed byte with U+FFFD, including unterminated input.

LTRIM and RTRIM with the default whitespace set inspect only the requested
edge. They no longer check the entire text for ASCII before trimming it.
Unicode whitespace, invalid UTF-8, custom cutsets, NULL handling and non-string
coercion retain their existing behavior. An unchanged string still returns its
original interface value without allocating.

Column BETWEEN predicates already parse into two comparisons. Fixed integer,
floating-point or text bounds now share a filter that reads the column and
checks its type once per row. The same applies to the corresponding explicit
`column >= low AND column <= high` and `column < low OR column > high` forms.
Integer comparisons retain all 64 bits. NOT BETWEEN keeps its two ordered
comparisons so NaN and reversed bounds behave as before. Two parameter bounds
can also share a filter, reading their current values on every call. Mixed
numeric parameter bounds widen only the comparison that uses a REAL bound,
preserving integer precision on the other side. NULL and uncommon types retain
the original short-circuit and error behavior. One parameter mixed with a fixed
bound, mixed fixed integer/float bounds and expression bounds keep the existing
filters. No mutable execution state is added to the cached plans.

LENGTH and CHAR_LENGTH check ASCII prefixes eight bytes at a time and decode
only the remaining suffix. This avoids a second pass over an ASCII prefix when
a later character needs UTF-8 decoding. The implementation uses ordinary Go
byte access, with no unsafe pointers, and preserves one character per malformed
UTF-8 byte. BLOB length remains a byte count.

SUM and AVG bind direct column/literal operands for one `+`, `-` or `*` operation
once per execution. Integer and REAL results go straight into the accumulator,
avoiding a boxed result per row. Integer overflow promotion and accumulation
order use the existing arithmetic helpers. An explicit rounding boundary keeps
multiplication separate from accumulation even on CPUs supporting fused
multiply-add. Decimal values, nested expressions and other operations retain
the evaluator; decimal promotion continues through the existing exact path.

## First-pass measurements

Measured on 2026-10-21 with Go 1.27.2, darwin/arm64, Apple M2 Max,
GOMAXPROCS=1. Values are medians from three alternating before/after runs,
200 ms per sample, with no concurrent builds or tests. Both binaries use the
same benchmark fixtures and working tree, including the calendar additions.
The baseline replaces only `lexer.go`, `builtin_string.go` and
`exec_raw_filter.go` with their pre-optimization sources using a Go overlay.

| Workload | Before | After | Allocated bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Parse INSERT with a 1.9 KiB escaped string | 14.377 µs | 2.572 µs | 5,824 → 2,504 | 20 → 11 |
| Tokenize 1.1 KiB unescaped string | 574.1 ns | 56.24 ns | 0 → 0 | 0 → 0 |
| Tokenize 1.9 KiB escaped string | 13.234 µs | 1.703 µs | 5,368 → 2,048 | 10 → 1 |
| COUNT with literal BETWEEN bounds, 100,000 rows | 0.782 ms | 0.616 ms | 2,592 → 2,568 | 35 → 33 |
| Same COUNT with parameter bounds, control | 1.042 ms | 1.047 ms | 2,640 → 2,640 | 35 → 35 |
| LTRIM, 36,004-byte ASCII text | 11.497 µs | 42.17 ns | 16 → 16 | 1 → 1 |
| RTRIM, 36,004-byte ASCII text | 11.295 µs | 42.01 ns | 16 → 16 | 1 → 1 |
| LTRIM predicate scan, 10,000 long-text rows | 111.714 ms | 0.630 ms | ~162,300 → ~162,200 | 10,026 → 10,025 |
| RTRIM predicate scan, 10,000 long-text rows | 110.132 ms | 0.666 ms | ~162,300 → ~162,200 | 10,026 → 10,025 |

The trim scan uses repeated 36,004-byte values with short whitespace edges and
compares the trimmed result to a short, nonmatching string. It deliberately
isolates the redundant full-text scan; workloads that consume the entire result
will see a smaller relative gain. Unicode-edge trimming stays approximately
flat, as do simple/complex SELECT parsing, bulk INSERT parsing, grouped COUNT,
hash join and paginated projection controls. These measurements describe the
listed in-memory workloads, not overall database throughput or storage I/O.

## Further optimizations

This comparison starts from the first-pass implementation above, preserving
all calendar and lexer changes. A source overlay restores only
`builtin_string.go`, `raw_range_filter.go` and `exec_fastpath_aggregate.go` to
their state before this second pass. The same machine, Go version and three
alternating 200 ms samples are used, with no concurrent test/build jobs from
this task.

| Workload | Median before → after | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| 20,000 rows, SUM(a * 2) | 1.031 → 0.346 ms | 160,947 → 2,025 | 19,901 → 30 |
| 20,000 rows, SUM(a * b) and AVG(a + b), integers | 2.259 → 0.508 ms | 307,294 → 2,216 | 38,182 → 33 |
| 20,000 rows, arithmetic SUM/AVG with REAL operands | 3.482 → 0.529 ms | 322,069 → 2,216 | 40,029 → 33 |
| Same integer expressions, 20 groups | 2.831 → 0.743 ms | 323,495 → 18,417 | 38,288 → 139 |
| Character count, 16 KiB ASCII | 5,268 → 1,052 ns | 0 → 0 | 0 → 0 |
| Character count, 16 KiB ASCII followed by two Unicode characters | 14,891 → 1,051 ns | 0 → 0 | 0 → 0 |
| LENGTH predicate scan, 10,000 ASCII rows | 59.760 → 10.861 ms | ~82,500 → ~82,300 | ~10,030 → ~10,027 |
| LENGTH predicate scan, same rows with Unicode suffix | 137.557 → 11.004 ms | ~82,800 → ~82,300 | ~10,035 → ~10,027 |

The aggregate allocation reductions were consistent. Host load affected
timings: even the unoptimized nested-expression control ranged from 2.06–2.75
ms before and 1.46–2.01 ms after. Timing percentages should therefore not be
treated as precise latency forecasts. Direct SUM/AVG and grouped COUNT controls
use one slightly larger projection descriptor (16 extra bytes in these cases)
with unchanged allocation counts. The LENGTH scan reuses a 16 KiB text value
across 10,000 rows and excludes fixture construction and SQL parsing.

The final parameter-filter comparison was repeated after moving NULL handling
ahead of numeric dispatch. Each row-filter benchmark scans 100,000 values;
all filter-only cases allocate zero bytes per scan.

| Parameter workload | Median before → after |
| --- | ---: |
| SQL COUNT with parameter BETWEEN bounds | 1.271 → 0.762 ms |
| Integer BETWEEN filter | 0.909 → 0.454 ms |
| REAL BETWEEN filter | 0.907 → 0.491 ms |
| Text BETWEEN filter | 1.221 → 0.763 ms |
| Mixed integer/REAL BETWEEN filter | 0.815 → 0.530 ms |
| NULL upper bound, BETWEEN | 0.662 → 0.607 ms |
| NULL upper bound, NOT BETWEEN | 0.645 → 0.626 ms |

The SQL COUNT plan adds 40 bytes (2,640 → 2,680 B/op), with 35 allocations in
both versions. The unchanged literal-bound COUNT control also varied from
0.794 to 0.652 ms, reinforcing the timing limitation above. NULL bounds retain
the old errors and required column access even for malformed short rows.

## Reproduction and validation

Full standard and minimal test suites, focused race tests, GORM/sqlx/Squirrel
compatibility tests, `go vet` and WebAssembly compilation passed. A 10-second
differential lexer fuzz run completed 52,982 executions without a failure.

```sh
GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkLexerQuotedValues|BenchmarkParseEscapedInsert|BenchmarkTrimEdges|BenchmarkTrimFilterScan|BenchmarkBetweenCountScan)$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1

# Compare fused and separate range filters within the current binary.
GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkRawBetweenFilter$' -benchmem -benchtime=200ms -count=3

GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkAggregateBinary|BenchmarkStringCharCount|BenchmarkLengthScan|BenchmarkRawBoundRangeFilter|BenchmarkBetweenCountScan)$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1

go test ./...
go test -tags=tinysql_minimal ./...
go test -race ./internal/engine ./internal/driver \
  -run 'Test(Lexer|Trim|StringCharCount|LengthMixed|RawRange|RawBoundRange|AggregateBinary|SQLPreparedAggregateBinary|Between|Prepared.*|CalendarConversion)' -count=1
go test ./internal/engine -run '^TestLexer' \
  -fuzz '^FuzzLexerQuotedToken$' -fuzztime=10s -parallel=2
go vet ./...
make test-sql-compat GO_TEST_FLAGS=-count=1
make wasm-check
```

Regression coverage includes quoted Unicode identifiers and values, retained
tokens, malformed UTF-8, every Unicode whitespace character, raw/general trim
evaluation, integer precision at 2^53 and int64 limits, NULL, NaN, reversed
bounds, column aliases, cached parameter changes and expression errors around
range predicates. Additional coverage checks ASCII word boundaries against the
standard UTF-8 counter, arithmetic aggregation against the general evaluator,
decimal promotion, separate float rounding, prepared aggregate rebinding and
concurrent execution of one statement against different table schemas.
