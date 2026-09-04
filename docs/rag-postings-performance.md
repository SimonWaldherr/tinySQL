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
