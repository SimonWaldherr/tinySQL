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
