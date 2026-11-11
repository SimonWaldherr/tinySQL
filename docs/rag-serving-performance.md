# RAG serving: cache reuse and result allocations

These changes reduce repeated work when tinySQL serves retrieval requests from
several embedded databases and when SQL qualifies the final hit rows. They also
fix document identity and authorization edge cases. SQL signatures, result
columns, ranking algorithms, and persisted index formats are unchanged.

## Cache identity across databases

Vector-column, IVF, HNSW, context-index, and optional result-cache keys include
the source table's runtime identity. A tenant, table name, and version are
insufficient: two independent databases can have the same values for all three.

Previously, alternating between such databases replaced the column/ANN cache
entry on each request. The optional result cache could return candidate row
positions from the other database. Each table now retains its own warm entries,
including after equal-version table replacement. DROP/recreate and rollback
remain covered by cache invalidation tests.

The caches are still process-wide and bounded: 256 vector-column entries,
256 context-index entries, and 64 entries in each ANN cache. Old table instances
can remain referenced until eviction; result-cache entries expire according to
their configured TTL. These
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

## Context indexes after ingestion

An unrestricted physical-table context index now extends after pure appends.
It reads only new rows, clones the document directory, and replaces the chunk
slices of affected documents. Unaffected slices remain shared with the old
immutable index. Stable sorting preserves physical-row order for duplicate
chunk positions, even when ingestion arrives out of chunk order.

The table's structural-change counter gates this path. UPDATE, DELETE, schema
changes, and table replacement require a full rebuild. ACL-scoped indexes keep
their version-specific rebuild because appended rows can change authorization
membership. CTE sources also continue to build from their current result rows.
Context indexes built during a failed statement are purged on rollback: a later
write may reuse both the restored table pointer and the failed version number.
A trigger regression test demonstrates that otherwise an out-of-window row
could inherit a rolled-back neighbor's physical position.

The following follow-up measurement compares against `fa51a40`, on the same
machine and Go version as above. Each iteration starts with a warm index over
100 or 1,000 documents of 12 chunks each, appends 12 chunks to one existing
document, then measures index refresh and one 12-neighbor lookup. Ingestion,
SQL execution, and result-map materialization are excluded. Values are medians
of three 200ms runs:

| Existing rows | Time/op, before → after | Bytes/op, before → after | Allocs/op, before → after |
| --- | ---: | ---: | ---: |
| 1,200 | 88.35 µs → 4.74 µs | 121,304 → 12,120 | 612 → 14 |
| 12,000 | 852.15 µs → 21.44 µs | 1,316,457 → 150,144 | 6,023 → 16 |

This is a small-batch context-index benchmark, not a full RAG latency claim.
Directory cloning still scales with document count, and copying/sorting touched
documents scales with their chunk count. Large ingestion batches and a corpus
dominated by one huge document need their own measurements.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkRAGContextIndexAfterAppend$' \
  -benchmem -benchtime=200ms -count=3
```

The benchmark harness was added with this change; copy its file to the baseline
checkout to reproduce the comparison.

## Filtered vector selection and query keys (2026-11-11)

Filtered HNSW now copies the requested prefix of its already sorted candidate
list instead of rebuilding and sorting another top-k heap. The copy owns its
storage after pooled traversal scratch is released. When fewer than k candidates
exist, the same exact authorized-row fallback runs directly, without building
a heap that would immediately be discarded. Graph traversal, search breadth,
distance arithmetic and tie ordering are unchanged.

Exact filtered scans reserve each worker's heap for at most its allowed input
positions; the parallel merge reserves at most the whole allowed set. Requested
k remains unchanged. Invalid rows and dimensions can reduce the final count
further. This also permits very large k without a correspondingly large
reservation when the authorized set is small. Public filters still resolve
stable IDs and metadata restrictions before ranking.

The opt-in result cache hashes query components in batches of 32 rather than
one hash-writer call per component. The SHA-256 byte stream remains the exact
little-endian IEEE-754 bits in their original order, including signed zero and
NaN payloads. The final digest writes into its existing fixed buffer, removing
one 32-byte allocation per key. Disabled result caching still skips hashing.

Allocation measurements compare the working tree immediately before these
vector changes with the resulting tree, using source overlays for
`rag_filtered_ann.go`, `rag_prefilter.go` and `vector_query_cache.go`. Earlier
uncommitted CTE/WHERE/text changes remain fixed. Both executables contain the
same fixtures. Go 1.27.3, darwin/arm64, Apple M2 Max, GOMAXPROCS=2; medians of
three runs of three operations. Fixture setup and ANN graph construction are
outside the timed region. The HNSW fixture has 1,280 source rows, 960 allowed
positions and 64-dimensional embeddings, using cosine distance.

| Workload | Bytes/op before → after |
| --- | ---: |
| Exact filtered scan, 8 allowed positions, k=20 | 357 → 165 |
| Same subset, k=100000 | 1,605,669 → 165 |
| Exact filtered scan, 4,096 allowed positions, k=100000 | ~4,817,834 → ~131,653 |
| Same 4,096 positions, k=20 control | ~1,738 → ~1,738 |
| Filtered HNSW, k=20 | 320 → 320 |
| Filtered HNSW, k=1000 with insufficient graph candidates | 32,816 → 16,432 |
| Query key, 32/768/1,536 components | 32 → 0 |

Successful HNSW prefix selection still allocates the same owned result slice;
its improvement is removing repeated heap work. The insufficient-candidate
fixture drops from three allocations to two. Key hashing drops from one to
zero allocations. Parallel startup can vary small setup costs. These figures
measure allocation volume, not peak live memory or complete RAG request cost.

A separate warm timing check uses before/after/after/before rounds, with three
200 ms samples per workload in each round. Medians of all six samples:

| Workload | Time/op before → after |
| --- | ---: |
| Filtered HNSW, k=20 | 3.204 → 2.744 µs |
| Filtered HNSW, k=400 | 61.845 → 40.306 µs |
| Query key, 32 components | 287.75 → 171.65 ns |
| Query key, 1,536 components | 10.588 → 5.276 µs |
| Exact filtered scan, 4,096 positions, k=20 control | 35.346 → 33.384 µs |

The unchanged control differs by 5.6%, and some sample ranges are wide. Treat
these as local observations rather than a precise production speedup estimate.
In particular, neither table measures index construction, complete SQL/RAG
execution, client transfer or recall differences; the candidate algorithm and
search breadth are retained.

Differential tests compare filtered ANN selection with the former heap path
across all four metrics, tied distances, invalid/zero/NaN vectors and varied k.
Other checks cover sparse authorization, oversized k, serial/parallel scans,
segmented/updated caches, cancellation, output ownership, public RAG queries
and index invalidation after UPDATE. Hash tests compare the complete key with
SHA-256 of the original bit stream at batch boundaries and exercise concurrent
readers. The root Go suite, targeted vector/RAG race checks, SQL-library
compatibility, go vet and all four browser/Node WASM profiles pass. WASM also
checks large-k filtered queries and physical-order tie ranking.

```sh
go test ./internal/engine \
  -run '^Test(FilteredANNSelection|FilteredVectorHeap|VecQueryKey)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^Benchmark(FilteredANNSelection|FilteredVectorHeapCapacity|VecQueryKeyBatches)$' \
  -benchmem -benchtime=3x -count=3
```

## Parallel heap reuse and IVF bounds (2026-11-11)

Parallel exact vector scans, including authorized-row scans, now consume one
request-owned worker heap as the merge heap when its reserved capacity can
hold the final top-k. This avoids allocating another k-entry buffer and
reinserting that worker's entries. Large k retains a new bounded buffer when
the worker capacity is insufficient. Worker errors retain their original
priority, and the final result owns its storage. Distance calculations, NaN
exclusion, tied-row order and cancellation checks are unchanged.

IVF also bounds its initial result heap by the cached source row count. The
requested k and probe budget remain intact, including continuation through
additional lists when the initial probes yield too few results. Large k can
therefore return every valid row without reserving space for nonexistent hits.

Allocation checks use the working tree before this follow-up as baseline,
overlaying `vector_search.go`, `rag_prefilter.go` and `vector_index.go`; earlier
vector improvements remain fixed. Go 1.27.3, darwin/arm64, Apple M2 Max,
GOMAXPROCS=2; medians of three runs of three operations. Parsing, corpus/cache
construction and IVF training are excluded. The exact fixture has 8,192
three-dimensional source rows; its filtered case allows 4,096 positions.

| Fixture | Bytes/op before → after |
| --- | ---: |
| Parallel exact scan, k=400 | ~20,330 → ~13,605 |
| Parallel filtered scan, k=400 | 20,165 → 13,637 |
| Exact scan, k=10000 control | 262,693 → 262,693 |
| Filtered scan, k=10000 control | 131,653 → 131,653 |
| IVF over 48 source rows, k=100000 | 1,605,733 → 869 |
| Same IVF fixture, k=20 control | 421 → 421 |

The filtered k=400 case drops from seven allocations to six. Small parallel
startup costs vary across runs. These are allocation-volume measurements,
not end-to-end latency or peak-memory claims. Regression tests compare the
merge with an independent result heap and exhaustive scoring for all metrics,
covering ties, infinities, nonpositive k, large k, worker error priority and
IVF requests up to math.MaxInt. WASM exercises oversized explicit IVF queries.

```sh
go test ./internal/engine -run 'TestVector(Merge|ParallelMerge|IVFOversized)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVector(ParallelHeapMerge|IVFHeapCapacity)$' \
  -benchmem -benchtime=3x -count=3
```

## Large filtered graphs and incremental ingestion

Selective filtered ANN graphs now size their levels, adjacency headers and
vector-header cache to the allowed row set. The compact layout applies when
that set covers at most a quarter of the source corpus. Sorted local IDs map
back to physical rows, preserving physical-ID level seeds, topology, distance
bits and tie order. Broad or noncanonical row sets retain the previous layout;
an insufficient ANN frontier still falls back to exact allowed-row scoring.

The compact view borrows immutable vector payloads rather than duplicating
embeddings. It can keep a packed column segment resident while cached. New
publications remove obsolete versions of the same source-table object, and
superseded in-flight builds are returned privately without being cached.

Append-only HNSW maintenance now publishes a persistent topology image after
`min(1024, max(32, stored_rows/16))` new indexed rows. Refresh remains lazy:
the next HNSW query or `VEC_WARM` first makes the runtime index current.
Initial builds, incompatible snapshots and the first valid graph entry publish
immediately. The existing file format already supports older append-only
prefixes: reopening validates and clones that prefix, then inserts the missing
tail through the same deterministic path. The unpublished indexed tail is
bounded after a successful refresh; later writes without a refresh can add
more rows. UPDATE-delta graphs keep their
existing persistence rules. Rows retain their ordinary storage/WAL durability.

Exact scans over adjacent append-only column segments now validate their
layout once and walk each segment directly. UPDATE overrides, gaps, overlaps
or malformed layouts retain the general resolver. Absolute cancellation check
positions, result ownership and metric semantics remain unchanged.

Allocation measurements compare `b29f4ad` with the same benchmark fixtures in
separate before/after processes: Go 1.27.3, Apple M2 Max, `GOMAXPROCS=2`, medians
of three fixed-count runs. These measure allocated bytes, not peak RSS:

| Workload | B/op before → after | Allocs/op before → after |
| --- | ---: | ---: |
| Filtered graph build, 4,096 allowed positions in a million-row address space | 35,387,802 → 1,685,677 | 8,806 → 8,803 |
| One-row append/refresh, starting from 10,000 vectors | 2,944,049 → 18,775 | 32 → 26 |

The graph-build fixture materializes only the selected 16-dimensional vectors
as sparse cache overrides. It isolates corpus-sized graph bookkeeping rather
than database loading or payload memory. The 4,096-row subset exercises the
production filtered-ANN size range. Warm search uses a separate contiguous
million-row header fixture with the same selected payloads.

The append benchmark includes column-cache extension, graph insertion and
publication. Its 1,100 operations cross the publication threshold, so the
measurement includes full topology snapshots rather than only a short burst
that defers them. Context-sensitive wall-clock results varied with host load;
the allocation reductions were consistent. Single-segment and UPDATE-override
scan controls retain their one 320-byte heap allocation.

```sh
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkFilteredANNSelectiveBuild$' -benchtime=3x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkFilteredANNSelectiveSearch$' -benchtime=1000x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecHNSWIncrementalInsert_10k$' -benchtime=1100x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecSegmentedFlatScan$' -benchtime=50x -count=3 -benchmem
```

Regression coverage compares compact graphs and all four metrics with the
physical-ID implementation, including ties, invalid vectors, exact fallback,
ACL mutations and concurrent cold queries. Persistence tests cover immutable
prior snapshots, threshold crossings, actual close/reopen, hydration and
current warm-up results. Segmented scans are compared bit-for-bit with the
resolver path across partial ranges, invalid data and cancellation points.
