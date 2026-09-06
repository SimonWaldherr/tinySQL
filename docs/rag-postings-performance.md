# RAG: exact top-k over term postings

Literal OR queries now rank each term directly over its posting list rather
than looking up every term in every candidate document. This targets the
lexical branch used by RAG's default automatic OR expansion.

OR scoring in tinySQL takes the **maximum** term score. Consequently, the union
of each term's top-k contains the exact global top-k: a document outside one
term's top-k already has k better documents for that term, including the stable
row-ID tie-breaker. Each merge retains only global winners, bounding intermediate
winner storage by O(k). This property does not apply to summed term scores.

Terms are processed by decreasing IDF. Once k results exist, a term whose
BM25 upper bound cannot reach the heap threshold can be skipped along with
all lower-weight terms. The frequency factor is bounded by `k1 + 1`; bounds
are rounded upward and comparisons are strict to preserve ties. Actual scores
use the existing evaluator, preserving its floating-point rounding boundaries.

Restricted candidate sets, including RAG authorization filters, retain the
existing filtered scan and corpus-specific IDF. Phrases, wildcards, AND and NOT
queries retain the general evaluator. The change adds no result cache and does
not change query syntax, candidate limits or retrieval quality settings. It
uses the existing immutable posting index; cold index construction still has a
cost. Very common, similarly weighted terms may yield less pruning.

The RAG changes in the same commit also select fusion winners with a bounded
heap and merge vector worker heaps without intermediate sorting. The RAG demo
loads chunks in SQL batches and batches evaluation embeddings.

## Measurements

Apple M2 Max, darwin/arm64, Go 1.27.1, GOMAXPROCS=12; medians of three 700 ms
runs. The fixture contains 20,000 chunks and 96-dimensional vectors, with the
same four-term query and candidate window in both versions. Baseline is the
working implementation immediately before the posting-based change, already
including the fusion and worker-heap improvements.

| Workload | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| Full hybrid retrieval | 1.893 ms | 0.967 ms | 49% |
| Lexical branch | 1.159 ms | 0.089 ms | 92% |
| Hybrid allocated bytes/query | 54,357 | 49,929 | 8% |
| Lexical allocated bytes/query | 63,288 | 58,872 | 7% |

Individual runs varied (hybrid after: 0.943–1.371 ms; lexical after:
0.082–0.151 ms). These measurements include SQL execution and result
materialization but exclude embedding requests and cold index construction.
Existing indexes are warm; the optional vector answer cache is disabled by
default. Every query still runs retrieval. These are workload measurements,
not a guarantee for arbitrary corpora or a comparison of cold process starts.

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkRAGHybridSearch|BenchmarkRAGFTSSearchBranch)$' \
  -benchmem -benchtime=700ms -count=3
```

Verification compares posting-based ranking with the document scan using exact
score equality, randomized documents and queries, duplicate/absent terms,
varied k values, restricted rows and cancellation. Existing FTS/RAG suites cover
hybrid fusion, filters, corpus changes, boolean/phrase semantics and concurrency.

## Posting frequencies and block bounds (format 2)

The next implementation step adds position-aligned `int32` term frequencies to
persistent posting lists. Scoring reads these directly instead of searching the
document's term arena. The document arena remains available for phrases and
compound queries. Each 128-posting interval also stores maximum frequency and
minimum document length. Their combination gives a conservative BM25 bound even
when the two extrema belong to different documents. Bounds use the current query's
IDF and average document length, rather than freezing a score into the index.

Unrestricted single-term and literal OR top-k retrieval can skip blocks below
the local heap threshold. Strict comparisons and outward rounding preserve ties.
The filtered path is unchanged. A shared normalization function preserves the
floating-point rounding boundary between posting and document scoring.

Index maintenance covers append-only inserts (rebuild only the affected tail
blocks), row updates (rebuild blocks of affected terms), deletion/reordering and
compaction. Counts and bounds are deep-copied in snapshots and persisted alongside
the existing FTS metadata. Format-1 or structurally incomplete indexes rebuild
lazily from source rows; their first search therefore pays the rebuild cost.
There is no new answer cache. The additional raw frequency payload is four bytes
per posting, plus block metadata and map/slice allocation overhead.

Fresh local measurements on the same Apple M2 Max, Go 1.27.1, three 500 ms runs:

| Workload | Before this step | Frequencies only | Frequencies + blocks |
| --- | ---: | ---: | ---: |
| RAG lexical branch, 20k chunks | 54.408 µs | 46.404 µs | 39.733 µs |
| Full hybrid query, same corpus | 290.242 µs | 290.505 µs | 298.605 µs |

The lexical median improves 27%; the complete hybrid workload does **not** show
an improvement. Compare within this measurement session, not against earlier
runs with different machine load. The final hybrid runs span 293–371 µs.

`BenchmarkFTSPostingMetadata` separately compares a parallel document scan,
serial posting frequencies, and frequencies with block pruning on a deliberately
favorable 20k-document fixture: its first 128 documents dominate the ranking.
Medians are 75.539, 92.846 and 6.665 µs respectively. This illustrates both the
benefit of skipping and the fact that serial frequencies alone can be slower
than a parallel scan. It is not a representative speedup guarantee. The existing
common-term workload measures 44.346 µs after this change; no matched baseline
for that workload was collected in this session.

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkFTSPostingMetadata|BenchmarkRAGHybridSearch|BenchmarkRAGFTSSearchBranch|BenchmarkRAGFTSCommonTerm)$' \
  -benchmem -benchtime=500ms -count=3
```

These benchmarks exclude index construction. Index-build/update latency, index
size on disk and production concurrency percentiles remain separate measurement
work; no improvements for those metrics are claimed here.

## Direct vector-column scans

The exact flat vector pass now recognizes the usual immutable column layout: a
single packed segment without UPDATE overrides. It addresses vector, validity,
and norm data by row offset directly. This avoids an overrides-map probe and a
binary segment lookup for every candidate. Append-only columns with multiple
segments and tables with UPDATE overrides stay on the existing general resolver,
so their behavior and invalidation rules are unchanged. The same direct layout
is used for the physical row IDs selected by RAG `pre_filter`.

This is not a result or answer cache: every request still computes distances,
top-k selection, lexical ranking, and RRF fusion. It only removes redundant
metadata lookups during an exact scan.

On Apple M2 Max, Go 1.27.1, three 500 ms runs over 20,000 warm 96-dimensional
embeddings, direct scanning had a 1.188 ms median versus 1.324 ms through the
general segment resolver, a 10% reduction. Both variants allocate 384 bytes in
one allocation per scan. The benchmark isolates vector ranking; end-to-end RAG
latency also includes SQL execution, concurrent FTS retrieval, fusion, and
result materialization.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecSearchContiguousCacheScan$' -benchmem \
  -benchtime=500ms -count=3
```

Regression coverage compares contiguous and general scans for full and
pre-filtered row sets, including invalid and dimension-mismatched vectors.

## ARM64 compact embeddings

On ARM64, cosine retrieval uses a dot product for every candidate. The NEON
kernel is now selected from 32 dimensions instead of 128, covering compact
64- and 96-dimensional embedding models. An ARM64-specific equivalence test
checks empty, short, unaligned, and long vectors against the portable loop.

Across three 500 ms Apple M2 Max runs, a 96-dimensional dot product measured
38.31 ns with NEON versus 79.04 ns for the portable unrolled loop (median).
This is a kernel microbenchmark; complete RAG latency also includes candidate
selection, FTS, fusion, SQL execution, and materialization.

```sh
go test ./internal/engine/search -run '^TestVectorDotNEONMatchesUnrolled$' \
  -bench '^BenchmarkVectorDotNEONBySize$' -benchmem -benchtime=500ms -count=3
```

## Linux/amd64 cosine dispatch

Linux/amd64 already selects AVX2/FMA for suitable processors. The RAG-default
cosine path now bypasses the generic metric dispatcher after vector search has
selected cosine, while retaining its dimension, zero-norm, and NaN exclusion
checks. This removes two function calls and metric switches for every scanned
candidate, on top of the existing SIMD dot-product kernel.

On the local ARM64 control run, a 96-dimensional candidate took 26.91 ns in
the direct path versus 29.91 ns through generic dispatch (three 500 ms runs;
both allocate zero bytes). The Linux/amd64 engine and vector-math test binaries
are cross-compiled as ELF during verification. Actual Linux latency depends on
the deployed CPU's AVX2/FMA support and should be measured on that host.

```sh
GOOS=linux GOARCH=amd64 go test -c ./internal/engine
GOOS=linux GOARCH=amd64 go test -c ./internal/engine/search
```

## Server capability selection

The vector backend is selected once during package initialization, before any
query executes. On amd64, tinySQL combines CPUID with XGETBV so AVX2/FMA is
used only when the processor *and* operating system enable YMM state; all
other amd64 hosts use the SSE2 kernels. On ARM64, it reads the operating
system's ASIMD/NEON feature flag and falls back to the portable unrolled
kernels if it is unavailable.

`search.VectorMathBackend` exposes the chosen path for diagnostics:
`amd64-avx2-fma`, `amd64-sse2`, `arm64-neon`, `arm64-portable`, or
`portable-unrolled`. This avoids speculative instructions: deliberately
executing an unsupported SIMD instruction merely to test it could terminate a
process with `SIGILL`, so the OS feature interface is the safe server probe.

## ARM64 fused cosine parts

`VEC_COSINE_SIMILARITY` needs a dot product plus both squared norms when its
inputs have no cached norm. The ARM64 path now computes all three in one NEON
pass, loading each vector cache line once. It retains the portable unrolled
path for vectors shorter than 32 dimensions and an exact scalar tail for odd
lengths.

On Apple M2 Max, three 500 ms runs measured 19.67 ns for a 96-dimensional
fused kernel versus 44.80 ns for the portable loop; at 768 dimensions it took
118.9 ns versus 430.2 ns. Both paths allocate zero bytes. This improves scalar
cosine functions; RAG scans that already cache vector norms continue using the
separate SIMD dot-product path.

```sh
go test ./internal/engine/search -run '^$' \
  -bench '^BenchmarkVectorCosineNEONBySize$' -benchmem \
  -benchtime=500ms -count=3
```

## ARM64 Manhattan distance and centroid accumulation

The ARM64 backend now also uses NEON for Manhattan distance: a four-way
subtract, absolute-value, and add pipeline is selected from 32 dimensions and
retains an exact scalar tail. This covers direct `VEC_MANHATTAN_DISTANCE`
calls and vector searches configured with the `manhattan` metric.

The same threshold applies to in-place vector accumulation. `VEC_CENTROID`
and IVF k-means training now dispatch through this kernel, which loads the
current centroid and source vector in NEON lanes, adds them, and writes the
result back without allocating.

On Apple M2 Max, three 500 ms runs measured 114.7 ns for 768-dimensional L1
distance versus 302.8 ns for the portable loop. For 768-dimensional centroid
accumulation, the kernel took 118.6 ns versus 267.7 ns. Both improvements are
available without any cache dependency and both benchmark paths allocate zero
bytes.

```sh
go test ./internal/engine/search -run '^$' \
  -bench '^(BenchmarkVectorL1NEONBySize|BenchmarkVectorAccumulateNEONBySize)$' \
  -benchmem -benchtime=500ms -count=3
```

## Small-window RRF fusion

The default hybrid search retrieves `4 × k` candidates per branch, typically
24 vector and 24 lexical rows. At that size, matching physical row IDs with a
short reverse linear probe is cheaper than allocating and hashing a map. The
probe preserves duplicate handling: the latest vector candidate wins, as it
does in the map implementation. Candidate pools above 64 entries retain the
existing hash-map algorithm.

On Apple M2 Max, three 500 ms runs of the default 24-candidates-per-branch
fusion measured a 2.816 µs median versus 3.211 µs before (12% lower), with
7,560 rather than 8,752 bytes and 54 rather than 57 allocations. The 256- and
4,096-candidate benchmarks remained within run variance because they use the
unchanged map path. End-to-end hybrid requests also allocate three fewer
objects and about 1.2 KiB less per request in this workload.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkRAGFuseCandidates$' -benchmem -benchtime=500ms -count=3
```

## Context expansion without intermediate result rows

`RAG_SEARCH` with `expand_before` or `expand_after` now carries selected
physical hit-row IDs straight into neighbor expansion. Expansion reads document
ID and chunk index from those source rows directly, instead of formatting an
entire vector or RRF result — including every source column and retrieval
diagnostic — only to read those values and discard the formatted rows. The
compact hand-off applies to vector-only and hybrid retrieval, preserves rank
and stale-row handling, and is independent of the vector, FTS, or
context-index caches.

On the same Apple M2 Max workload, the warmed 20,000-row hybrid benchmark with
one preceding and one following chunk measured a 212 µs median with 64.7 KiB
and 341 allocations per request (three 500 ms runs). Its cold-start behavior
also benefits because neither intermediate result rows nor private hit maps are
materialized while the context index is built.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkRAGHybridSearchWithExpansion$' -benchmem \
  -benchtime=500ms -count=3
```

### Context-window bounds and native ARM64 regression coverage

Context expansion now saturates chunk-window endpoints at the platform's integer
limits. Previously, a large `before` or `after` could wrap an endpoint and make
the indexed lookup panic with an inverted slice range. Both the direct scan and
the indexed expansion use the same bounds. The direct scan also limits its initial
result allocation to the source row count instead of the requested window width.

`TestRAGContextExtremeWindows` covers both integer boundaries through raw-table
and Row-map sources, using both lookup paths. `TestRAGContextWideWindowSQL`
checks the public `RAG_CONTEXT` and `RAG_CONTEXT_FROM` calls and their offsets.
On an Apple M2 Max (Darwin ARM64, Go 1.27.1), the three-row
`BenchmarkRAGContextWideWindow` fixture allocates 120 B in 2 allocations for both
window widths 2 and 1,000,000. These are allocation measurements for a small
fixture, not corpus-wide retrieval latency claims.

Reproduce with:

```sh
go test ./internal/engine -run 'TestRAGContext.*Window' \
  -bench '^BenchmarkRAGContextWideWindow$' -benchmem
```

CI now executes the search package's kernel tests as well as engine integration
tests on AMD64 and native Darwin ARM64. The ARM64 job also builds the release CLI;
NEON L2 regression coverage includes empty inputs and lengths on either side of
vector-loop and dispatch boundaries.

### Reusing warm context indexes and bounded string windows

A single `RAG_CONTEXT` lookup now reuses an existing neighbor index prepared by
`RAG_SEARCH` expansion or `RAG_CONTEXT_FROM`. It checks the same table identity,
version, columns, tenant, and filter key as multi-hit expansion. A cold lookup
still scans directly and does not build an index; a stale entry also falls back
to that scan. Empty hit sets return their empty result schema without building
an unused neighbor index. Multi-hit expansion stores candidates in a contiguous
buffer, with an initial estimate capped at 256 candidates, instead of allocating
one object per candidate. Deduplication and provenance ranking remain unchanged.

`SUBSTRING` now locates UTF-8 boundaries only as far as the requested window.
Negative positions still require a character count, while positive positions
avoid counting or decoding the unused suffix. Malformed UTF-8 retains the
previous rune-conversion semantics. Tiny outputs are copied when necessary to
avoid retaining a large source string. Lengths are bounded by the available
suffix, also avoiding integer overflow for very large requested lengths.

Local measurements: Apple M2 Max, Darwin ARM64, Go 1.27.1; medians of three
200 ms runs. Baseline includes the preceding context-window bounds fix.

| Benchmark | Before | After | Allocations before → after |
| --- | ---: | ---: | ---: |
| Single context lookup, warmed index, 12,000 rows | 63.37 µs | 6.58 µs | 79 → 77 |
| Context expansion from vector top-k | 218.38 µs | 213.66 µs | 1,060 → 1,003 |
| Hybrid retrieval with expansion | 215.24 µs | 210.97 µs | 341 → 325 |
| Four-character SUBSTRING, 60 KB ASCII source | 17.57 µs | 45.99 ns | 1 → 2 |
| Four-character SUBSTRING, 90 KB Unicode source | 141.53 µs | 66.63 ns | 3 → 2 |

The modest end-to-end expansion timing differences need longer runs to establish
statistical significance; the allocation reductions are consistent. The large
SUBSTRING gains concern short windows near the beginning of long strings, not
arbitrary substring workloads. ASCII now allocates 20 B instead of 16 B to avoid
retaining its 60 KB input; the Unicode fixture drops from 122,912 B to 32 B.

```sh
go test ./internal/engine -run '^$' \
  -bench 'Benchmark(RAGContextSingleWarmIndex|RAGContextSingle|RAGContextFromTopK|RAGHybridSearchWithExpansion|SubstringShortWindow)$' \
  -benchmem -benchtime=200ms -count=3
```
