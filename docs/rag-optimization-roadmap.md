# RAG optimization roadmap

This roadmap separates the retrieval hot path already implemented in TinySQL
from the next improvements. Priorities assume an embedded RAG workload with
frequent reads, append-heavy ingestion, stable chunk IDs, and corpora large
enough that repeated full scans are visible.

## Implemented baseline

The first three priorities are implemented together because they remove work
at three consecutive stages of one hybrid query:

1. **Persistent and incremental FTS index.** `FTS_SEARCH` stores its compact
   document directory, postings, term dictionary, and BM25 statistics with the
   table. A reopened database hydrates the runtime cache without tokenizing the
   corpus again. Pure append workloads extend only the new rows; an update,
   delete, or schema change advances `StructVersion` and forces a safe rebuild.
2. **Late materialization.** Vector and FTS branches rank physical row IDs and
   scores first. Source columns are copied only for the final result set, not
   for every branch candidate.
3. **Row-ID-native hybrid fusion.** `RAG_SEARCH` fuses the two candidate lists
   directly by physical row ID. It no longer creates two wide intermediate
   result tables or reconstructs identity from projected values before RRF.

The public result schemas and SQL APIs remain unchanged.

## Remaining work

### Phase 1 — filtered retrieval before ranking

Push equality/range filters and authorization predicates into `VEC_SEARCH`,
`FTS_SEARCH`, and the composed RAG functions. Start with predicates backed by a
secondary index and represent the allowed row IDs as a sorted set or bitmap
shared by both branches.

Deliverables:

- an options/API shape for pre-filters that cannot be confused with an outer
  post-retrieval `WHERE`;
- intersection of FTS postings and vector candidates with the allowed row set;
- tenant/ACL tests proving unauthorized rows never consume candidate slots;
- filtered and unfiltered recall/latency benchmarks.

Acceptance criterion: selective filters reduce examined candidates and do not
change results relative to an exact search over the same authorized subset.

### Phase 2 — persistent vector indexes and compact encodings

Persist HNSW topology and its build metadata beside the vector column cache,
then extend it for append-only ingestion using the same `StructVersion`
contract as FTS. Add optional float32 and scalar-quantized storage while
retaining an exact float64 path for baselines and final rescoring.

Deliverables:

- versioned on-disk HNSW format with corruption and compatibility checks;
- append journal or copy-on-write update path;
- optional quantized candidate search followed by exact top-N rescoring;
- recall@k, build time, reopen time, disk size, and resident-memory benchmarks.

Acceptance criterion: reopening does not rebuild HNSW, append cost scales with
new vectors, and each compact mode has a documented recall/memory envelope.

### Phase 3 — tokenizer and document-format profiles

Make lexical normalization configurable per index rather than globally
implicit. Add language-aware profiles and a Markdown-aware profile that treats
repeated table separators, rule characters, and formatting punctuation as
boundaries without storing empty or punctuation-only terms. Preserve useful
code, identifiers, and table cell values.

Deliverables:

- persisted tokenizer/profile identifier included in index validity checks;
- normalization hooks for German and other target languages;
- Markdown table, code block, identifier, Unicode, and repeated-special-
  character regression corpora;
- query-time normalization guaranteed to match ingestion-time normalization.

Acceptance criterion: formatting-only changes do not dominate lexical scores,
while values inside tables and code identifiers remain retrievable.

### Phase 4 — mutable-index maintenance and compaction

Avoid full rebuilds after ordinary updates and deletes by adding tombstones and
small delta segments to FTS and HNSW. Merge them in the background or at an
explicit maintenance boundary.

Deliverables:

- atomic base-index plus delta-index snapshots;
- delete/update tombstones with transaction rollback coverage;
- deterministic compaction policy bounded by delta ratio and memory;
- crash-recovery tests across WAL replay and all persistence backends.

Acceptance criterion: query results are transactionally correct throughout a
mutation/compaction cycle and steady update cost is independent of corpus size.

### Phase 5 — retrieval quality controls

Add optional diversity and reranking stages after hybrid fusion: document-level
caps, duplicate suppression, maximal marginal relevance, and an application
callback for a cross-encoder. Keep the built-in exact RRF output available as a
stable baseline.

Deliverables:

- deterministic deduplication by document/content hash;
- configurable per-document result caps and MMR;
- bounded reranker input/output contract with cancellation;
- evaluation fixtures reporting Hit@k, MRR, nDCG, context duplication, and
  end-to-end latency.

Acceptance criterion: every quality feature can be evaluated and disabled
independently, and no reranker can bypass authorization filtering.

### Phase 6 — observability and capacity controls

Expose enough diagnostics to tune a live corpus without profiling internals.

Deliverables:

- `EXPLAIN`/diagnostic fields for cache source (memory, persisted, rebuilt,
  incrementally extended), candidates examined, and materialized rows;
- cache memory budgets and per-table eviction/maintenance controls;
- counters for FTS/HNSW build, extension, invalidation, and query latency;
- corpus benchmark command with reproducible datasets and saved baselines.

Acceptance criterion: operators can identify cold rebuilds, ineffective
candidate sizes, filter selectivity, and memory pressure from supported APIs.

## Recommended sequence

Implement Phase 1 before adding more approximate search because filtering is
both a recall feature and a security boundary. Phases 2 and 3 can then proceed
independently. Phase 4 should reuse their finalized persisted formats. Add the
evaluation harness from Phase 5 early, and gate each later optimization on its
quality and latency results. Observability should land alongside each phase,
with the consolidated public diagnostics completed in Phase 6.
