# RAG serving: cache reuse and result allocations

These changes reduce repeated work when tinySQL serves retrieval requests from
several embedded databases and when SQL qualifies the final hit rows. They also
fix document identity and authorization edge cases. SQL signatures, result
columns, ranking algorithms, and persisted index formats are unchanged.

## Cache identity across databases

Vector-column, IVF, HNSW, and optional result-cache keys include the source
table's runtime identity. A tenant, table name, and version are insufficient:
two independent databases can have the same values for all three.

Previously, alternating between such databases replaced the column/ANN cache
entry on each request. The optional result cache could return candidate row
positions from the other database. Each table now retains its own warm entries,
including after equal-version table replacement. DROP/recreate and rollback
remain covered by cache invalidation tests.

The caches are still process-wide and bounded: 256 vector-column entries and
64 entries in each ANN cache. Old table instances can remain referenced until
eviction; result-cache entries expire according to their configured TTL. These
are entry limits, not byte budgets. Budget vector copies and ANN topology memory
for every serving database, and warm each database separately with `RAG_WARM`.

A request waiting for a concurrent IVF/HNSW build can now leave when its context
is canceled. This does not cancel another request's build or its waiters.

## Final hit rows

The built-in vector, FTS, hybrid, and context retrievers allocate fresh result
maps on each execution. SQL now adds qualified column names directly to these
maps instead of copying every hit map again. Extension table functions retain
the copying path because their ownership contract may allow shared results.
Noncanonical column names also use the existing conversion to preserve casing
and dotted-name behavior.

Tests compare the resulting maps with the copying implementation, including
qualified aliases, absent branch scores, context expansion, and repeated calls
after a caller mutates its returned map.

## Identity and authorization

Context lookup and overlapping-window deduplication preserve exact `BIGINT`
document IDs. Adjacent IDs above 2^53 no longer collapse through a float64 key.
Cold scans and warmed lookups use the same equality rule; ordinary integral
values such as Go `int(7)`, `int64(7)`, and `float64(7.0)` still identify the same
document. This change is scoped to RAG context identity.

An explicit `pre_filter.allowed_row_ids: null` is rejected, including alongside
an `equals` restriction. Omitting the ID list still permits a metadata-only
filter; `[]` explicitly authorizes no rows. A null ID list must not silently
discard part of an application's authorization boundary.

## Measurements

Measured locally on Apple M2 Max, darwin/arm64, Go 1.27.3, GOMAXPROCS=12.
Values are medians of three warm runs. Baseline: `c2c484a`.

Full SQL retrieval uses 20,000 chunks, 96-dimensional vectors, six final hits,
and 24 candidates per hybrid branch. The vector branch uses exact flat cosine
search. The following allocation counts include SQL result materialization:

| Query | Bytes/op, before → after | Allocs/op, before → after |
| --- | ---: | ---: |
| Hybrid RAG | 45,333 → 42,697 | 198 → 181 |
| Hybrid with neighbor expansion | 61,465 → 59,917 | 305 → 270 |
| Hybrid with selective pre-filter | 41,729 → 39,105 | 198 → 181 |
| Vector-only RAG | 21,939 → 21,000 | 144 → 129 |

End-to-end timings varied substantially between runs and do not establish a
latency improvement. The allocation reduction is consistent across the runs.

A separate candidate-only benchmark alternates between two databases with the
same table/tenant names, each containing 1,024 synthetic 32-dimensional vectors,
with k=20. It excludes SQL result maps and disables the optional result cache:

| Mode | Alternating databases, bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| Flat | 300,178 → 368 | 12 → 2 |
| IVF | 336,880 → 624 | 153 → 3 |
| HNSW | 390,082 → 368 | 20 → 2 |

After the change, allocation counts match the corresponding single-database
case. The synthetic vectors are highly correlated, so these results establish
cache reuse, not ANN recall or a representative production speedup. Regression
tests also assert reuse of the actual column buffers and ANN objects.

Run the current workloads with:

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkRAGHybridSearch|BenchmarkRAGVectorOnly|BenchmarkRAGHybridSearchPreFilterSelective|BenchmarkRAGHybridSearchWithExpansion)$' \
  -benchmem -benchtime=300ms -count=3

go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecSearchDatabaseReuse$' \
  -benchmem -benchtime=200ms -count=3

go test ./internal/engine -run '^$' \
  -bench '^BenchmarkRetrievalRowQualification$' \
  -benchmem -benchtime=300ms -count=3
```

The last benchmark isolates six-hit materialization and qualification at eight
and 32 source columns. Compare repeated runs on an idle machine; evaluate
recall@k, tail latency, warm-up time, and resident memory on the actual corpus
before choosing a vector index mode.
