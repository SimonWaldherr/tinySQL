# Retrieval performance

## Search-path changes

- RAG's lexical branch evaluates bound, literal OR queries as a flat list of
  term IDs and BM25 weights. OR still takes the maximum term score. Compound
  queries retain their boolean/phrase semantics, and filtered retrieval rebinds
  the weights to the authorized corpus as before. Document-length normalization
  is computed once per document, and recursive evaluation shares the immutable
  cache by pointer.
- Parallel vector and lexical searches merge worker heaps directly. Only the
  final winners need ordering; intermediate result slices and local sorts are
  eliminated. Worker-count thresholds remain unchanged.
- Spatial grids record whether rows occur in multiple cells. Point layers can
  concatenate candidate lists without deduplication. Broad polygon queries use
  a bitmap; small windows retain a sparse set. Queries covering the complete
  indexed extent enumerate valid rows directly. The final geometry residual
  checks remain in place, and GEO_SEARCH reuses candidate memory for its matches.

## Local measurements

Apple M2 Max, darwin/arm64, Go 1.27.1, GOMAXPROCS=12. Values below are medians
of three warm runs, comparing against the implementation before these changes
(which already included top-k RRF selection). RAG uses 20,000 chunks with
96-dimensional embeddings; GIS uses 20,000 regularly spaced points or polygons.
These are workload-specific measurements, not a latency guarantee.

| Workload | Before | After | Allocation bytes, before → after |
| --- | ---: | ---: | ---: |
| RAG lexical branch | 2.157 ms | 1.832 ms | 67,752 → 63,288 |
| Full RAG hybrid query | 2.311 ms | 2.418 ms | 62,834 → 54,431 |
| Full GIS query, regional point window | 12.217 ms | 9.646 ms | 10,809,104 → 10,363,456 |
| GIS point candidates, regional | 0.906 ms | 0.056 ms | 445,568 → 141,152 |
| GIS polygon candidates, regional | 2.664 ms | 0.317 ms | 502,912 → 51,840 |
| GIS point candidates, complete extent | 2.430 ms | 0.036 ms | 979,328 → 81,920 |
| GIS polygon candidates, complete extent | 4.409 ms | 0.035 ms | 979,328 → 81,920 |
| GIS polygon candidates, small window | 0.049 ms | 0.052 ms | 13,616 → 13,616 |

The full hybrid benchmark does **not** demonstrate a latency improvement:
its median was about 5% higher despite fewer allocations. The lexical branch
improved about 15%, and the full GIS query improved about 21%. Small polygon
windows keep the existing sparse algorithm. Candidate-only speedups exclude
SQL result materialization, which dominates queries returning many rows.

Reproduce the warm workloads with:

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkGeoGridCandidates|BenchmarkGeoSearchWarm|BenchmarkRAGHybridSearch|BenchmarkRAGFTSSearchBranch)$' \
  -benchmem -benchtime=700ms -count=3
```

The GIS and initial baseline measurements used 500ms per run; the final RAG
measurements used 700ms. Compare repeated runs on an otherwise idle machine
before drawing conclusions about smaller differences. Cold index building and
process reopen are outside these measurements.
