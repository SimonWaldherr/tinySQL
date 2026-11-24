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
   result tables or reconstructs identity from projected values before RRF. A
   bounded top-k heap selects winners before sorting, preserving exact RRF
   scores and deterministic tie order while reducing ranking work for wide
   candidate pools.
4. **Serving warm-up and query planning.** `FTS_WARM` eagerly prepares the
   same persistent/runtime lexical entry as `FTS_SEARCH`; repeated lexical
   queries reuse their immutable wildcard expansion, candidate set, and BM25
   weights while the source-table version is unchanged. A filtered query
   rebinds only the small prepared tree to its ACL-local BM25 statistics. This
   keeps cold-index work and recurring query preparation off the
   latency-critical RAG path.

The public result schemas and SQL APIs remain unchanged.

Serving caches distinguish source-table instances across independent databases,
so equal names and versions neither mix optional cached candidates nor displace
each other's warm vector columns and ANN indexes. SQL qualifies freshly
materialized built-in retrieval rows without another map copy. Context identity
preserves exact BIGINT IDs, and explicit null ID allowlists fail closed. See
[RAG serving measurements](rag-serving-performance.md) for scope, allocation
results, and remaining memory-budget limitations.

Unrestricted context indexes also extend after pure appends, sharing untouched
document groups while copying the document directory and changed groups.
Structural changes and ACL-scoped sources still rebuild. Directory copying
remains proportional to document count; this is not a general mutable-index
implementation.

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
topology only for new rows. A DELETE is followed in place within the bounds
described under Phase 4; a schema change, an incompatible format, an
incomplete/corrupt topology or a removal beyond those bounds takes the safe
full-rebuild path.

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

Status (see [Retrieval maintenance](retrieval-maintenance.md) for the cost model,
the bounds in one table and the adoption guide): the persistent full-text index now
follows DELETEs without a rebuild.
The table keeps an ordered change log of UPDATE and DELETE events
(`Table.ChangesSince`); a DELETE that names the rows it removed is one event, and
the next search compacts the document directory, drops the dead documents from
their posting lists, renumbers the other entries and rebuilds the block bounds of
the lists that lost entries (three sweeps over the postings behind the first
removed row, no re-tokenizing of the corpus; the cost grows with the index and
is worst when the removed row is near the start of the table). Deletes that
cannot report their rows (no WHERE clause, swap-and-pop point deletes by
PRIMARY KEY/UNIQUE value, triggers, cascades), schema changes, rolled-back
deletes, indexes with more than a quarter of obsolete arena entries and indexes
in which a quarter of the terms are empty lists still rebuild. So does a log that
overflows: it holds at most 65536 events (every UPDATEd row is one event, a
DELETE statement one). When it fills, its older half is folded: a run of UPDATEs
becomes one column summary (it can only vouch that no column the reader depends
on changed, never which rows), a DELETE is kept whole as a fold of its own, with
its positions. A structure that missed a DELETE and then any number of
bookkeeping UPDATEs is therefore still followed, as long as none of the folded
UPDATEs touched a column it indexes; one that missed an UPDATE of its own column
inside a fold pays one full rebuild at the next search. At most 64 summaries and
4096 folds are kept, and at most 2^20 removed positions in log and folds
together; past a bound the OLDEST folds (then the oldest log blocks) are
forgotten, and only the stamps older than what was forgotten rebuild. This is
renumbering, not tombstones, so there is no delta segment to merge.

The flat vector column cache and the HNSW graph follow the same change log (see
"Vector structures after a DELETE" below), so a hybrid search after a DELETE no
longer waits for a cold rebuild of either. The RAG context index, the
ACL-filtered ANN graphs, the IVF index, row twins and subscriptions still
rebuild after a removal.

#### Vector structures after a DELETE

*Flat column cache.* The new cache entry shares the packed vector arrays of the
old one and only rebuilds the small per-row arrays (24 bytes of slice header, 8
of norm and 1 of validity per row, copied in runs between the dead rows), so
positions stay equal to table positions and no consumer changes. UPDATE
overrides are renumbered, the UPDATEs of the vector column reported with the
removal are applied as overrides, and the appended tail becomes a new segment.
The old entry is never written to, so a search that holds it is unaffected. The
floats of the dead rows stay in the shared arrays and are counted as garbage per
segment; once more than 10 percent of the packed floats are garbage, the next
removal rebuilds the cache (which also resets the count). At most 24 segments
are kept: beyond that the two smallest neighbours are merged. The memory
trade-off is up to 10 percent of the vector data (600 MB of a 6 GB column of a
million 768-dimensional rows) kept allocated for rows that no longer exist, in
return for not copying the column (2.4 us per row and 6 GB of transient memory
at 768 dimensions) on every DELETE.

*HNSW graph.* Under the graph's write lock the nodes of the dead rows are
dropped and every node, edge, delta row and the entry point is renumbered through
the dead list (a rank table with one prefix count per 256 ids; the edge pass runs
on all cores); rows appended since are inserted by the existing incremental path.
Edges that led to dead nodes are dropped, neighbours are not repaired: on
clustered embeddings recall@10 stayed within one point of a fresh build up to 20
percent removed (0.988 against 0.998), on isotropic Gaussian vectors, the
worst case, within 4 points (0.905 against 0.945), and every node stayed
reachable from the entry point. On source-shaped data (a source is 32 chunks of
one topic, one source deleted and one inserted per cycle, 17 of 190 sources
replaced) recall@10 was 0.9925 to 0.9988 against 0.9988 to 1.0 for a fresh build
and recall@50 0.983 to 0.989 against 0.987 to 0.994. All of this was measured at
32 dimensions and 6,000 rows; 768 dimensions and a million rows are not
measured. A graph that loses its entry point picks the
first scorable node of the highest level. The graph is marked not current while
it is edited, so a failure or panic in the middle leads to a rebuild, never to a
half-renumbered graph being served. The followed graph is persisted again, so a
reopen hydrates a topology with the right numbering; a graph with UPDATE delta
rows is not persisted (as before) and its stored predecessor, which has the old
stamp, is never used. The graph is rebuilt once more than 5 percent of its nodes
are removed by one statement or more than 10 percent since it was last built
(the count is stored with the persisted topology, so a reopen continues it), or
when the UPDATEs of the vector
column exceed the delta bound. The measured recall leaves headroom above both
bounds, at those sizes only; they are constants (`vecHNSWFollowSinglePercent`,
`vecHNSWFollowTotalPercent`).

Measured on a laptop (i7-10850H, GOMAXPROCS 4, 768 dimensions, one source of six
chunks removed, then the first `HYBRID_SEARCH`; medians of three runs of five
iterations, noisy because the machine was shared):

| | before | after |
| --- | ---: | ---: |
| flat, 50,000 rows, DELETE + first search | 63 ms, 312 MB/op | 21 ms, 4.8 MB/op |
| flat, 100,000 rows | 168 ms, 623 MB/op | 39 ms, 8.8 MB/op |
| flat, 100 sources in one DELETE, 50,000 rows | 117 ms | 76 ms |
| column cache refresh alone, 50,000 / 100,000 rows | 60 ms / 316 ms | 0.4 ms / 1.0 ms |
| HNSW, 10,000 rows, DELETE + first search | 3.9 s | 4.7 ms |
| HNSW, 20,000 rows | 7.1 s | 7.2 ms |
| HNSW graph refresh alone, 10,000 / 20,000 rows | 2.3 s / 5.6 s | 2.3 ms / 2.5 ms |

The following figures are projections, not measurements of a real table: they
come from synthetic benchmarks of the passes alone (one 16-dimensional segment
without tail, overrides or garbage for the cache; a graph with uniformly random
links, allocated back to back, for the graph, so a graph grown by inserts will
be slower to walk). At a million rows the passes alone cost 5.6 ms for the flat
cache (33 MB allocated, independent of the dimension), 89 ms for the HNSW graph
(6 dead rows; 101 ms for 600, 157 ms for 30,000) plus 61 ms and 321 MB transient
for the deep copy that persists it, against a graph rebuild of 380 to 600
seconds (an estimate supplied to this work, not measured here). The full-text
follow at a million rows and an end-to-end first HYBRID_SEARCH on a million
768-dimensional rows in hnsw mode were not measured. What the first search still pays: the flat scan itself
(O(rows x dimensions)), the full-text follow (the shift of the posting lists
behind the first removed row, which dominates a removal of many sources), and,
once per 10 percent of removed graph nodes, one full HNSW rebuild.

The HNSW "after" figures are medians of five iterations and leave out that
rebuild. Its cost per re-import is constant in the table size (the interval
grows with the table as the rebuild does): about 0.32 ms per row over 10
percent of the rows is roughly 19 to 36 ms per re-import of six chunks, four to
seven times the median at 10,000 rows. The first search that triggers it waits
for the whole rebuild (minutes at a million rows), and every other search of
the same graph waits for it. A 400-iteration run at 20,000 rows averaged 15 ms
per search for this reason.

The figures above were measured before the DELETE statement itself was changed,
on a benchmark table with no key and no index. The merged code also renumbers the
secondary indexes in place and takes candidate rows from an index (see
`insert-update-performance.md`, "What a multi-row DELETE costs"); the statement
still scans or seeks, copies the row pointer list (24 bytes per row) and marks the
table for a full WAL record. A DELETE by equality on a primary or unique key is a
swap-and-pop, which resets the change log and gets none of this (every structure
of that table rebuilds at its next search); deleting by a non-key column such as
the source goes through the reporting path. Still rebuilding after a removal,
deliberately: the RAG context index, the row-filter caches, the ACL-filtered
graphs, the IVF index, row twins and subscriptions. After a followed removal and
an append the flat cache has several segments, so the single-segment fast paths
of the row resolver and of the ACL-filtered scan take the per-row path (a binary
search over at most 24 segments); that effect is unmeasured and also exists for
append-only workloads. A window whose removals hit only rows appended after the
structure was built (insert a source, delete it again, search) has no dead row
among the indexed ones: the flat cache and the graph are extended over the
appended rows, as for a window without removals.

Limits of the following, all of which leave results correct and only cost a
rebuild at the next search: the flat cache and the graph are keyed by the table
object, so anything that replaces the live object starts them cold. A committed
explicit transaction that wrote the table installs its private copy of the table
(autocommit statements do not), and a DELETE or UPDATE issued while a direct
SELECT stream is open on the table installs a copy without search structures. The
full-text index survives the first (the transaction's copy carries a deep copy of
it, which costs O(index) per writing transaction) but not the second. A failed
statement drops the flat cache, the graphs and the full-text cache of its table;
a rollback that undoes a logged DELETE resets the change log. Writers that need
the followers to help issue their DELETEs and UPDATEs as autocommit statements on
a connection that holds no open stream on the table. The flat cache entries of a
replaced table object stay in memory until the entry cap (256, 64 for graphs)
evicts them at random; each pins its packed vectors.
After a followed removal the graph that picks a new entry point takes the first
scorable node of the highest level, not a former neighbour of the old entry.

The flat cache is checked against cold builds by a differential fuzz test
(`vector_cache_follow_test.go`): random statement sequences with NULL, ragged
and zero vectors, rolled-back statements, transactions and several table
shapes. The graph has no differential test against a cold build, because its
answers are approximate by design: `vector_hnsw_follow_test.go` checks the
invariants, an exact renumbering oracle keyed by the id column (including the
multi-worker pass), recall and reachability on four corpora, failure
injection, cancellation, concurrent searches and reopen, and
`retrieval_maintenance_diff_test.go` keeps the strict comparison with a cold
build for every window the graph rebuilt.

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

See [retrieval performance](retrieval-performance.md) for the current RAG/GIS
search-path optimizations, measured tradeoffs, and reproduction commands.
