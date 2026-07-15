package mcpserver

// functionsCatalog is served as the tinysql://functions resource. It is a
// static reference — these are built-in engine functions, not database
// state — kept here rather than generated so it stays reviewable as plain
// text. Keep it in sync with docs/rag-guide.md when function signatures or
// return columns change.
const functionsCatalog = `tinySQL RAG / Vector / Full-Text function reference

These are usable directly inside read_query's SELECT (table-valued functions
go in the FROM clause; scalar functions anywhere an expression is expected).

## Table-valued functions (FROM clause)

VEC_SEARCH(table, column, query_vector, k [, metric [, index]])
    k-nearest-neighbor vector search.
    metric: 'cosine' (default) | 'l2'/'euclidean' | 'manhattan'/'l1' | 'dot'/'inner_product'
    index:  'flat' (default, exact) | 'ivf' | 'hnsw' (approximate; prebuild with VEC_WARM)
    Returns every column of ` + "`table`" + ` plus:
      _vec_distance    lower = closer
      _vec_similarity  higher = closer — feed THIS (not _vec_distance) into
                       RAG_HYBRID_SCORE / RAG_RANK_SCORE, which expect a
                       similarity; passing a distance silently inverts ranking
      _vec_rank        1-based rank, 1 = closest
    Example:
      SELECT * FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1,0.2,0.9]'), 10, 'cosine')

VEC_TOP_K(table, column, query_vector, k [, metric])
    Alias for VEC_SEARCH.

VEC_WARM(table, column [, metric [, index]])
    Eagerly builds the vector column cache and, if requested, the IVF/HNSW
    index, so the first real query after a bulk load doesn't pay the cost.

FTS_SEARCH(table, query, k [, column1, column2, ...])
    BM25 full-text search (IDF-weighted, corpus-length normalized).
    Adjacent query terms are an implicit AND: a verbose natural-language
    question (e.g. "how do I configure the server") will often match
    nothing — OR-expand it instead ('configure OR server OR setup').
    With no column list, EVERY column is searched, including VECTOR columns
    (stringified) — always pass explicit text column names to avoid that.
    Returns every column of ` + "`table`" + ` plus _fts_score and _fts_rank (1-based).
    Example:
      SELECT * FROM FTS_SEARCH('chunks', 'timeout OR retry', 10, 'chunk_text')

RAG_CONTEXT(table, doc_id_col, chunk_index_col, doc_id, chunk_index, before [, after])
    Expands one known chunk into its neighboring chunks within the same
    document (before/after = how many chunks to include on each side).

RAG_CONTEXT_FROM(table, doc_id_col, chunk_index_col, hits_table, hit_doc_id_col, hit_chunk_index_col, before [, after])
    Expands a set of retrieval hits (e.g. a VEC_SEARCH/FTS_SEARCH result, or
    a CTE over one) into their neighboring chunks. Overlaps across hits are
    deduplicated. Returns _hit_rank, _context_offset, _context_hits,
    _context_rank alongside the source columns.

## Scalar functions

Vector construction / serialization:
  VEC_FROM_JSON(json_string)     '[1.0, 2.0, 3.0]' -> vector
  VEC_TO_JSON(vector)            vector -> JSON string
  VEC_TO_BYTES(vector) / VEC_FROM_BYTES(hex)
      Compact float32 hex encoding for export/transport interchange — NOT an
      in-table storage optimization (the hex string is not smaller than a
      native VECTOR column).

Vector similarity / distance (for expressions outside VEC_SEARCH):
  VEC_COSINE_SIMILARITY(v1, v2)   -> [-1, 1], higher = more similar
  VEC_COSINE_DISTANCE(v1, v2)     -> [0, 2],  1 - cosine similarity
  VEC_L2_DISTANCE(v1, v2)         Euclidean distance
  VEC_MANHATTAN_DISTANCE(v1, v2)  L1 / city-block distance
  VEC_DISTANCE(v1, v2 [, metric]) generic distance, metric as in VEC_SEARCH
  VEC_DOT(v1, v2)                 dot / inner product

Vector math / manipulation:
  VEC_DIM, VEC_NORM, VEC_NORMALIZE, VEC_ADD, VEC_SUB, VEC_MUL, VEC_SCALE,
  VEC_CONCAT, VEC_SLICE, VEC_QUANTIZE, VEC_BINARY_QUANTIZE,
  VEC_HAMMING_DISTANCE, VEC_CENTROID, VEC_MIN_DISTANCE, VEC_RANDOM

Full-text (standalone — no corpus statistics, unlike FTS_SEARCH):
  FTS_MATCH(text, query)   -> bool. Query syntax: word, "phrase", prefix*,
                              A AND B, A OR B, NOT A, "A B" (implicit AND)
  FTS_RANK(text, query)    -> BM25-style score (alias: BM25)
  FTS_SNIPPET(text, query [, before, after, ellipsis, max_tokens])
  FTS_HIGHLIGHT(text, query [, before, after])  simpler FTS_SNIPPET alias
  FTS_WORD_COUNT(text)

RAG scoring (combine retrieval signals into one rank):
  RECENCY_SCORE(ts, half_life_days [, now])
      -> [0, 1] exponential decay; 0.5 at exactly one half-life old
  RAG_HYBRID_SCORE(similarity, ts, half_life_days [, sim_weight, now])
      blends normalized similarity and recency
  RAG_RANK_SCORE(similarity, ts, half_life_days, quality [, sim_w, recency_w, quality_w, now])
      blends similarity, recency, and a caller-supplied quality signal
  For all three: similarity should be in [-1, 1] (VEC_SEARCH's
  _vec_similarity, or VEC_COSINE_SIMILARITY). The optional trailing ` + "`now`" + `
  defaults to the current statement's start time — stable across every row
  of one query, but NOT across separate query executions; pass it
  explicitly for scores that must stay identical run to run.

See docs/rag-guide.md in the tinySQL repository for worked examples: hybrid
vector+keyword retrieval (RRF), context-window expansion, and read-only
serving.
`
