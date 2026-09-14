# Filtered RAG posting retrieval

This optimization accelerates the lexical branch of filtered `RAG_SEARCH` and
`HYBRID_SEARCH`. It does not change the public API, persistent format, cache
lifetime, RRF fusion, or vector retrieval. For query design and authorization
semantics, see the [RAG guide](rag-guide.md#7-filtering-authorization-and-multi-tenancy).

## Execution contract

Filtered literal-term and literal-OR queries intersect term posting lists with
the sorted authorized row IDs before choosing each term's top-k candidates.
Because OR uses the maximum term score, the union of those per-term winners
contains the exact global top-k. The scorer uses filter-local IDF and average
document length; global posting bounds remain conservative under that local
normalization.

Unauthorized rows never reach the per-term heaps, and context expansion keeps
the same filter. Block pruning rounds outward and uses strict comparisons so ties
retain their previous order. Large row-ID gaps use binary seeks; adjacent IDs
advance monotonically.

Small authorized sets stay on the previous scan path: below 256 candidates,
intersection overhead is usually not worthwhile. Phrase, AND, NOT, and expanded
wildcard queries retain their existing evaluators. Unfiltered posting retrieval
remains separate, so it does not add an authorization branch to every row.

## Verify and measure locally

The differential coverage compares row IDs, order, and scores with the document
scan across dense/sparse/empty filters, block boundaries, gaps, missing terms,
ties, varied `k`, and cancellation. Existing RAG tests cover authorization
isolation, cache invalidation, context expansion, and concurrent queries.

```sh
go test ./internal/engine \
  -run 'Test(RAGFilteredPostingsMatchAuthorizedScan|FilteredFTSPlanMatchesAuthorizedCorpus|RetrievalPreFilterRunsBeforeVectorFTSAndRRF)' \
  -count=1
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkRAGFilteredPostingSelectivity|BenchmarkRAGHybridSearchPreFilterSelective|BenchmarkRAGHybridSearchPreFilterWithExpansion|BenchmarkRAGFTSFilteredBranch)$' \
  -benchmem
go test -race ./internal/engine -run 'Test(RAGFilteredPostingsMatchAuthorizedScan|FilteredFTSPlanConcurrentQueries)' -count=1
```
