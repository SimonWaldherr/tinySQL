# RAG, vector search, and GROUP BY performance (2026-10-23)

This follow-up reduces redundant hit-row copies, repeated HNSW distance
calculations, integer group-key encoding, and result maps discarded by
aggregate pagination.

## Measurements

Measurements use Go 1.27.3 on darwin/arm64, Apple M2 Max, GOMAXPROCS=12.
The baseline includes the [query and index changes](query-index-performance.md)
on top of `4c0f650`. Original-source overlays or a pre-change test binary
compare implementations with the same fixtures and benchmark code.
Benchmark jobs ran serially; the table reports medians of three to five runs
of 200–350 ms each.

| Workload | Before | After | Change |
| --- | ---: | ---: | ---: |
| RAG_CONTEXT_FROM, 6 table hits | 14.69 µs | 8.18 µs | 1.8× faster |
| RAG_CONTEXT_FROM, 64 table hits | 103.26 µs | 27.25 µs | 3.8× faster |
| RAG_CONTEXT_FROM, 6 CTE hits | 21.85 µs | 10.07 µs | 2.2× faster |
| RAG_CONTEXT_FROM, 64 CTE hits | 197.86 µs | 32.11 µs | 6.2× faster |
| Warm HNSW VEC_SEARCH, top 20 | 7.91 µs | 7.27 µs | 8% less time |
| Warm HNSW VEC_SEARCH, top 100 | 25.51 µs | 24.85 µs | 3% less time |
| Two integer group keys, batch aggregation, 350 groups | 863 µs | 567 µs | 34% less time |
| Two integer group keys, scalar aggregation, 350 groups | 1,218 µs | 927 µs | 24% less time |
| GROUP BY, LIMIT 20 OFFSET 10000, no ORDER BY | 5.72 ms | 2.30 ms | 60% less time |
| Same aggregate page with HAVING | 4.58 ms | 2.73 ms | 40% less time |

The context benchmark uses 128 source chunks, a warmed neighbor index, and
hits with 24 additional metadata columns. It measures context expansion,
excluding the retrieval that produces the hits. At 64 CTE hits, allocations
drop from 340,162 to 33,984 bytes and from 641 to 129 objects per call.
These factors do not describe an entire hybrid RAG query.

The HNSW benchmark searches 12,000 vectors with 64 dimensions and excludes
index construction. Top 200 is approximately unchanged (48.06 versus
47.69 µs). A separate synthetic graph benchmark isolates the upper-layer
greedy walk: it improves from 731 to 343 ns at 64 dimensions and from
6,286 to 2,821 ns at 768 dimensions, with zero allocations in either version.

The grouping fixtures contain 20,000 input rows. Integer-pair grouping
saves one encoded-key allocation per group. With 20,000 distinct groups,
it saves about 309 KB and 20,000 allocations, while execution time changes
little because result construction dominates. Short text groups remain
approximately unchanged (745 versus 742 µs); the mixed-type control takes
909 versus 920 µs, about 1% more time.

Aggregate pagination uses 20,000 distinct groups. Allocations fall from
14.32 to 7.29 MB and from 61,464 to 1,525 objects. The ordered control remains
approximately unchanged (9.57 versus 9.48 ms), because ordering still needs
all group results. Timings are workload-specific local measurements.

## Implementation and correctness

- `RAG_CONTEXT_FROM` resolves document, chunk, and rank columns once and
  reads those cells directly. It avoids building complete projected hit
  maps for both validation and expansion. Declared missing cells remain
  NULL, undeclared CTE keys stay hidden, and rank precedence, validation
  errors, case handling, and input ownership retain their existing behavior.
- HNSW uses its existing visited set across upper greedy layers. Once a
  node has been scored, it cannot improve on a subsequently better node;
  distance and row-ID tie ordering only improve. The beam search resets
  that set before exploration. Search budgets, metrics, and index parameters
  are unchanged. A reference traversal checks identical selected nodes and
  distance bits across four metrics, ties, and multiple layers.
- Pairs of Go `int` values use a direct `[2]int` map key in batch and scalar
  grouping. Other representations, including `int64`, floating-point,
  decimal, and NULL values, keep the canonical typed encoding. Numeric
  representations that previously formed distinct groups still do so.
- Without ORDER BY, aggregate LIMIT/OFFSET slices the completed group states
  before projecting result maps. Input aggregation and HAVING still evaluate
  fully. Regression tests compare pages with complete results, including
  empty inputs, NULL values, scalar and batch aggregates, and offset bounds.
  Query profiles report the number of rows actually projected.

## Reproduction

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkRAGContextFromHitColumns$' -benchmem -benchtime=200ms -count=5
go test ./internal/engine -run '^$' -bench '^BenchmarkVecHNSWGreedyTraversal$' -benchmem -benchtime=200ms -count=3
go test ./internal/engine -run '^$' -bench '^BenchmarkVecSearchCosineTopK(100|200)?_HNSWCached$' -benchmem -benchtime=200ms -count=3
go test ./internal/engine -run '^$' -bench '^BenchmarkAggregateIntegerPairs$' -benchmem -benchtime=200ms -count=3
go test ./internal/engine -run '^$' -bench '^BenchmarkGroupedAggregatePage$' -benchmem -benchtime=250ms -count=3

go test ./... -count=1
go test -race ./internal/engine ./internal/storage -count=1
go vet ./internal/engine ./internal/engine/search
```

Retain the new benchmark files when measuring the original implementation.
Use source overlays to preserve unrelated working-tree changes; the greedy
microbenchmark also includes the original traversal directly as a reference.

## Validation

The complete root-module test suite and the engine/storage race suites pass.
`go vet` passes for engine and search. Both separate GORM and SQL-package
compatibility suites pass. The changed Go files are gofmt-clean.
