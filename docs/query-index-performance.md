# Query and index performance (2026-10-23)

Four focused changes reduce work in SQL parsing, text ordering, compound
SELECT execution, and secondary-index persistence snapshots.

## Measurements

Local measurements use Go 1.27.3, darwin/arm64, Apple M2 Max and
GOMAXPROCS=12. The baseline is `4c0f650`. Before/after runs use the same
fixtures and benchmark code, with original source overlays or a baseline
test binary. Timed runs were serialized to avoid competing benchmark jobs.
Values below are medians from three to five runs of 100–500 ms each.

| Workload | Before | After | Change |
| --- | ---: | ---: | ---: |
| Three-column text/numeric ORDER BY, 20,000 rows | 12.81 ms | 11.30 ms | 12% less time |
| Same ORDER BY, LIMIT 20 OFFSET 5 | 455.9 µs | 365.5 µs | 20% less time |
| UNION, 16 distinct terms of 128 rows | 2.044 ms | 0.588 ms | 3.5× faster |
| UNION, 64 distinct terms of 128 rows | 28.53 ms | 2.368 ms | 12.1× faster |
| UNION, 64 terms repeating the same 128 values | 2.720 ms | 1.553 ms | 1.8× faster |
| Warm index snapshot, 10,000 distinct keys | 745.4 µs | 368.7 µs | 51% less time |
| Lexer, lowercase query | 469.4 ns | 419.1 ns | 11% less time |
| Parser, lowercase query | 1,200 ns | 1,110 ns | 8% less time |

The 64-term distinct UNION allocates about 4.16 MB per execution instead of
28.0 MB, with about 33,600 rather than 293,000 allocations. Warm index
snapshots allocate 723,724 rather than 1,447,047 bytes and 20,005 rather than
40,006 objects. Lowercase lexing goes from five allocations to zero; the
parser benchmark goes from 28 to 20 allocations.

Controls remained approximately unchanged: numeric ORDER BY with LIMIT,
cold index snapshots, and uppercase/mixed-case parsing. The standalone
set-deduplication helper measured 595 versus 603 µs and adds two small
allocations (112 bytes) from its shared accumulator; this helper is used by
the regression tests, while compound queries benefit from reusing the state.

These are workload-specific results, not a claim that all tinySQL queries
run faster by these factors. Query benchmarks exclude parsing and fixture
construction. The index benchmark measures snapshot construction only,
excluding serialization, disk writes and fsync. Parser/lexer benchmarks
exclude execution.

## Implementation and correctness

- Text ORDER BY keys use one direct string comparison. Other types keep
  the existing comparator, including NULL placement and mixed-type handling.
  Tie-breaking, bytewise text order and ASC/DESC semantics are preserved.
- Adjacent UNION DISTINCT terms share one membership set. Each incoming
  row is hashed once instead of rehashing the growing accumulated result.
  UNION ALL, EXCEPT and INTERSECT end that run. Column alignment, first-value
  representation, result order, cancellation and later-term errors remain
  covered by regression tests.
- A warm secondary index already produces independent keys and row IDs
  during its skip-list traversal. Persistence uses that copy directly,
  avoiding an intermediate live `Entries` image and a second deep copy.
  Cold indexes still copy their persisted entries. Snapshot ownership is
  checked in both directions, including a GOB round trip; the format is
  unchanged.
- Keyword classification returns canonical string constants, so folding a
  lowercase keyword does not allocate its token value. The keyword set is
  unchanged, and retained tokens cannot alias the lexer's mutable scratch.

## Reproduction

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkOrderByMultiColumn(NoLimit|Limit)$' -benchmem -benchtime=500ms -count=3
go test ./internal/engine -run '^$' -bench '^BenchmarkUnionDistinctChain$' -benchmem -benchtime=200ms -count=5
go test ./internal/engine -run '^$' -bench '^Benchmark(Lexer|Parse)KeywordCasing$/Lower$' -benchmem -benchtime=200ms -count=5
go test ./internal/storage -run '^$' -bench '^BenchmarkSecondaryIndexEncodeSnapshot$' -benchmem -benchtime=200ms -count=3

go test ./... -count=1
go test -race ./internal/engine ./internal/storage -count=1
```

The new benchmark files must also be present when measuring the baseline.
Use an original-source overlay to compare versions without reverting other
working-tree changes.

## Validation

The full root-module suite passed with `go test ./... -count=1`. Both engine
and storage passed their complete race suites. `go vet` passed for those
packages, and the separate `tests/gorm` and `tests/sqlpackages` compatibility
suites also passed. Changed Go files are gofmt-clean.
