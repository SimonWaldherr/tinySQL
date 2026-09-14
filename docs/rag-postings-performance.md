# RAG retrieval execution notes

This page describes the retrieval-path optimizations behind `FTS_SEARCH`,
`RAG_SEARCH`, and `HYBRID_SEARCH`. It is not a tuning guide: use the
[RAG guide](rag-guide.md) for corpus design, filtering, and evaluation. Run the
current Go benchmarks on a representative corpus instead of treating an earlier
machine's timings as a deployment target.

## Lexical top-k

Literal OR queries score term posting lists directly rather than probing each
query term in every candidate document. OR takes the maximum term score, so the
union of per-term top-k winners contains the exact global top-k; a bounded heap
keeps intermediate winners at O(k). Terms are processed by decreasing IDF, and
safe BM25 upper bounds can prune later terms. Strict comparisons and outward
rounding retain ties and existing score boundaries.

Phrase, wildcard, AND, NOT, and restricted-corpus paths retain their evaluators
when their semantics require them. Persistent postings can store aligned term
frequencies and block bounds for unfiltered literal-term/OR pruning. Format
upgrades, append/update maintenance, and incomplete metadata fall back to a
safe rebuild; no answer cache is introduced.

## Vector, fusion, and context paths

| Path | Optimization and contract |
| --- | --- |
| Flat vector scan | A single packed immutable segment uses row-offset access; multi-segment columns and UPDATE overrides keep the general resolver. |
| SIMD dispatch | ARM64 selects NEON from 32 dimensions; Linux/amd64 selects AVX2/FMA only when CPU and OS state support it, otherwise uses SSE2 or portable code. |
| Scalar vector functions | ARM64 fuses cosine parts and accelerates Manhattan distance and centroid accumulation while preserving scalar tails and NaN/dimension checks. |
| RRF fusion | Small candidate windows use a reverse linear probe; larger windows retain the map path. Tie and duplicate behavior stays deterministic. |
| Context expansion | Hit row IDs flow directly into neighbor expansion, avoiding intermediate wide result rows. Windows saturate at integer limits and bound allocation by source rows. |
| Context index reuse | A single lookup reuses a compatible warm index only when table, version, columns, tenant, and filter key match; cold/stale calls scan safely. |

Context expansion retains provenance, deduplication, rank semantics, and filter
boundaries. Empty hit sets do not build an unused index. `SUBSTRING` likewise
locates only the UTF-8 boundaries needed for a positive window, preserves
malformed-UTF-8 behavior, and avoids retaining a large source string for a tiny
result.

## FTS ingestion and maintenance

Persistent FTS build, append, and update paths tokenize columns in order rather
than first joining a temporary document string. This preserves token sequences,
including phrase boundaries across columns. Append maintenance updates postings
and frequencies in one pass over sorted term IDs; batch updates reuse local
scratch without retaining vocabulary-sized buffers. Mutation, delete, and
compaction rules still preserve complete postings and block metadata.

## Verify and measure locally

Differential tests cover exact scores/order, absent and duplicate terms, ties,
restricted rows, cancellation, contiguous/general vector parity, SIMD boundaries,
context ownership, cache invalidation, and FTS build/append/update equivalence.

```sh
go test ./internal/engine ./internal/engine/search \
  -run 'Test(FTSPostingTopKMatchesDocumentScan|FTSBlockPruningMatchesScan|RAGFuseCandidates|RAGContext|FTS(ColumnStreaming|AppendBatches|BatchUpdate))' \
  -count=1
go test ./internal/engine -run '^$' \
  -bench '^(Benchmark(RAGHybridSearch|RAGFTSSearchBranch|RAGFuseCandidates|RAGContextSingleWarmIndex|FTSPostingMetadata|FTSPersistent(Build|BatchUpdate))|BenchmarkVecSearchContiguousCacheScan)$' \
  -benchmem
go test ./internal/engine/search -run '^$' \
  -bench '^(BenchmarkVectorDotNEONBySize|BenchmarkVectorCosineNEONBySize|BenchmarkVectorL1NEONBySize|BenchmarkVectorAccumulateNEONBySize)$' \
  -benchmem
```
