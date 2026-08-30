# RAG optimization roadmap

This roadmap separates the retrieval hot path already implemented in tinySQL
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
4. **Serving warm-up and query planning.** `FTS_WARM` eagerly prepares the
   same persistent/runtime lexical entry as `FTS_SEARCH`; repeated lexical
   queries reuse their immutable wildcard expansion, candidate set, and BM25
   weights while the source-table version is unchanged. A filtered query
   rebinds only the small prepared tree to its ACL-local BM25 statistics. This
   keeps cold-index work and recurring query preparation off the
   latency-critical RAG path.

The public result schemas and SQL APIs remain unchanged.

### Implemented: filtered retrieval before ranking

`RAG_SEARCH` and `HYBRID_SEARCH` accept an explicit `pre_filter` options
object. It supports stable `allowed_row_ids` (using `id_column`, or a
single-column primary key by default) and an AND of `equals` metadata values.
It also supports an indexed WGS84 `spatial` bbox or centroid-radius boundary
for georeferenced corpora; all supplied restrictions are intersected before
ranking.
`VEC_SEARCH_FILTERED` and `FTS_SEARCH_FILTERED` expose the same object for
standalone retrieval without overloading the legacy positional APIs.

The resolved sorted row-ID set is shared by vector and lexical branches. FTS
intersects it with postings before scoring and calculates BM25 document
frequency/length statistics within that same authorized subset; vector search
ranks that exact allowed subset before candidates enter RRF. This is strict ACL
isolation, so filtered BM25/RRF scores are intentionally comparable only within
the same pre-filter. Matching secondary-index prefixes are used for equality
filters, with a correctness-preserving scan fallback.
Neighbor expansion uses the same set, so it cannot expose an otherwise
forbidden adjacent chunk. ACL, candidate-slot, context-boundary, stable-ID, and
tenant-namespace regression tests cover the contract.

Current limitation: ordinary scalar range predicates are not yet exposed.
Filtered HNSW is process-local and built from the already-authorized row set,
so it never filters a global ANN frontier after candidate selection.

## Remaining work

### Phase 2 — persistent vector indexes and compact encodings

The HNSW baseline is implemented. `VEC_WARM(..., 'hnsw')` persists a versioned
graph alongside the table; the next process validates and hydrates that graph
instead of rebuilding it. Pure appends extend the vector-column cache and HNSW
topology only for new rows. An update, delete, schema change, incompatible
format, or incomplete/corrupt topology takes the safe full-rebuild path.

Remaining deliverables:

- optional persistent IVF, float32, and scalar-quantized candidate indexes;
- exact top-N rescoring after a compact/quantized candidate pass;
- configurable graph/delta compaction and disk-memory budgets;
- recall@k, build time, reopen time, disk size, and resident-memory benchmarks.

Acceptance criterion: reopening does not rebuild a compatible HNSW graph,
append cost scales with new vectors, and each compact mode has a documented
recall/memory envelope.

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
