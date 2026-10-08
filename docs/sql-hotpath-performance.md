# SQL hot-path performance

The 2026-10-08 changes reduce temporary allocations while parsing SQL, avoid
unnecessary heap maintenance for large materialized result pages, and release
completed streams directly in the database/sql driver. A second pass reduces
GROUP BY state allocation and key formatting, and avoids discarded error
allocations in simple CASE expressions.

## Parser

Keyword classification folds short candidates into a stack buffer before
testing the existing keyword switch. Ordinary identifiers retain their original
spelling without allocating an uppercase copy. Lowercase keywords still receive
an owned uppercase token value; long candidates reuse the lexer's scratch
buffer. Quoted and qualified identifiers, Unicode handling and keyword matching
keep their existing behavior.

For multi-row INSERT VALUES, the preceding row's width supplies the next row's
initial expression capacity. Each row owns its expression slice independently;
different widths remain parseable and are validated during execution as before.

## Materialized ORDER BY with LIMIT

The first LIMIT + OFFSET candidates are accumulated before constructing their
bounded heap once, bottom up. Smaller pages retain the bounded TopN path. For
multi-column sorting, when the retained set exceeds approximately 90% of the
materialized input, the executor sorts the complete input once and returns only
the retained prefix. Single-column sorting keeps the bounded heap selection.
Retaining every row always skips heap construction.

Stable tie ordering, NULL placement, mixed directions and the caller's input
slice are preserved. Full sorting can use up to approximately 11% more
item/key storage at the cutoff than a bounded heap; this overhead decreases as
the retained fraction approaches the complete input. This change applies to
materialized result sorting, not the raw table-scan TopN executor.

## Completed database/sql streams

The driver checks ResultStream.Done immediately after ExecuteStream returns.
When production has already finished, it releases the reader reservation and
borrowed prepared AST synchronously, avoiding the watcher goroutine, closure
and synchronization allocations. Buffered rows retain their values after the
AST is rebound. Live streams continue to hold their resources until production
ends or Rows.Close stops the producer.

## Measurements

Apple M2 Max, darwin/arm64, Go 1.27.1, GOMAXPROCS=12. Baselines use the
corresponding source files from `ceb07aa71548`, selected through Go's source
overlay while preserving the surrounding working-tree changes. Before and after
use the same fixtures and separate compiled test binaries. Parser measurements
use three alternating 500 ms samples; driver and final sorting measurements use
three 300 ms samples per implementation.

| Workload | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| Parse simple SELECT | 1,216 → 1,172 | 29 → 19 |
| Parse complex SELECT | 3,736 → 3,624 | 89 → 78 |
| Parse 200-row, five-column INSERT | ~96,256 → ~64,360 | 2,421 → 1,816 |
| Tokenize complex SELECT | 112 → 0 | 11 → 0 |
| Tokenize uppercase keywords with lowercase identifiers | 168 → 0 | 8 → 0 |
| Tokenize lowercase keywords and identifiers | 200 → 32 | 14 → 5 |
| Sort 20,000 materialized rows, two columns, LIMIT 19,999 | ~1,622,064 → ~1,613,872 | 8 → 4 |
| database/sql indexed point result | ~1,490 → ~1,410 | 26 → 23 |
| database/sql LIMIT 1 | ~1,425 → ~1,355 | 22 → 19 |
| database/sql LIMIT 64 | ~2,440 → ~2,370 | 85 → 82 |
| database/sql empty indexed result | ~1,480 → ~1,410 | 26 → 23 |
| database/sql first-row control, live 256-row stream | ~23,270 → ~23,270 | 149 → 149 |

The INSERT parser uses about 33% fewer allocated bytes. Timing varied widely
with unrelated machine load, including the unchanged live-stream control, so
these runs do not establish a reliable percentage reduction in query latency.
The allocation reductions were consistent. Uppercase-only lexer input remains
at zero allocations. Re-run timing measurements on an otherwise idle machine.

## Reproduction and validation

```sh
go test ./internal/engine -run '^$' \
  -bench 'Benchmark(ParseSimpleSelect|ParseComplexSelect|ParseBulkInsert|LexerNextTokenOnly|LexerKeywordCasing)$' \
  -benchmem -benchtime=500ms -count=3
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkMaterializedOrderLimitFraction$' \
  -benchmem -benchtime=300ms -count=3
go test ./internal/driver -run '^$' \
  -bench '^(BenchmarkDriverCompletedSelect|BenchmarkDriverStreamFirstRow)$' \
  -benchmem -benchtime=300ms -count=3

go test ./...
go test -race ./internal/engine ./internal/driver \
  -run 'Test(Lexer|Parse|Parser|.*BoundedOrder|CompletedStream|Prepared.*(Stream|Bindings)|Driver.*Stream)' \
  -count=1
go vet ./internal/engine ./internal/driver
```

Regression coverage includes keyword case and token ownership across scratch
buffer growth, differently sized VALUES rows, stable bounded/full sort results
around the cutoff and key-arena chunk boundary, immediate cleanup of empty and
small results, exactly-once resource release, buffered BLOB ownership after AST
reuse, and existing concurrent prepared and live-stream behavior.


## GROUP BY and CASE: second pass

Single-column aggregation now uses separate string, integer and floating-point
maps instead of formatting a text key for every input row. NULL and boolean
keys have fixed slots. Integer types remain distinct; all NaNs share a group,
while positive and negative zero remain separate, matching the existing key
encoding. Composite and complex keys retain the framed encoding. Batch
execution keeps its composite-key buffer local to avoid per-row write barriers.

Group accumulators use query-local blocks growing from one to at most 64
states. Counts, sums, extrema and group values occupy disjoint, capacity-limited
slices. Only the arrays needed by the projection are allocated. Decimal and
COUNT DISTINCT metadata is allocated lazily, reducing the common state header
from 192 to 128 bytes on 64-bit systems. Blocks never move or get reused across
queries, so returned results remain owned by their execution. Small amounts of
unused space in the final block trade off against thousands of tiny allocations.

Simple CASE checks NULL operands before comparison, avoiding errors that would
immediately be discarded as a non-match. Every WHEN expression is still evaluated
in order, including expressions that return an error. This applies to raw-row,
materialized-row and aggregate evaluation.

Regression tests compare optimized queries against materialized subqueries for
mixed scalar and composite keys, NULL, signed zero, NaNs, BLOBs, JSON values,
COUNT DISTINCT, MIN/MAX and decimal promotion after several allocation blocks.
Concurrent executions also verify that retained results are not overwritten.

### Second-pass measurements

Apple M2 Max, darwin/arm64, Go 1.27.1, **GOMAXPROCS=1**. These baselines already
include the parser, sorting and driver changes above. Numbers are medians from
three alternating before/after runs of separately compiled test binaries,
300 ms per benchmark sample, without concurrent test or build jobs. Timings
represent these fixtures, not a universal database speedup.

| Workload | ms/op before → after | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| 50,000 rows / 10,000 groups, COUNT + SUM + HAVING | 5.140 → 3.241 | 5,035,320 → 3,721,752 | 75,422 → 6,072 |
| Same groups, SUM(CASE) + HAVING | 7.232 → 5.136 | 4,446,059 → 3,355,104 | 75,413 → 6,066 |
| 50,000 rows, simple CASE with NULL inputs | 2.410 → 1.934 | 161,789 → 1,704 | 10,024 → 23 |
| 20,000 rows, single text grouping column | 0.567 → 0.442 | 48,064 → 45,800 | 590 → 266 |
| 20,000 rows, two grouping columns | 1.025 → 0.985 | 322,192 → 294,712 | 3,549 → 1,495 |
| 50,000 rows, grouped COUNT DISTINCT | 2.392 → 2.017 | 145,520 → 143,048 | 2,434 → 1,850 |
| Whole-table SUM + AVG | 0.874 → 0.740 | 2,896 → 2,840 | 33 → 34 |
| Hash join control | 1.052 → 1.054 | ~1,722,800 → ~1,722,800 | ~10,020 → ~10,020 |

The many-group COUNT/SUM fixture uses 37% less time, 26% fewer allocated bytes,
and 92% fewer allocations. The whole-table SUM/AVG fixture adds one small
allocation while reducing total bytes and time. Hash join and plain DISTINCT
controls stayed approximately flat. Composite grouping still formats keys;
its smaller gain comes mainly from reduced state allocation.

```sh
GOMAXPROCS=1 go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkCaseHaving|BenchmarkGroupByTwoColumns|BenchmarkGroupBySingleColumnFastPath|BenchmarkGroupByWithHaving|BenchmarkUngroupedCountStar|BenchmarkUngroupedSumAvg|BenchmarkJoinHashJoinAboveThreshold|BenchmarkDistinctCount)$' \
  -benchmem -benchtime=300ms -count=3

go test ./...
go test -race ./internal/engine ./internal/driver \
  -run 'Test(Aggregate|SimpleCaseNull|Case|.*Aggregate|.*DistinctCount|CountDistinct|PreparedStmtConcurrentBindings)' \
  -count=1
go vet ./internal/engine ./internal/driver
```
