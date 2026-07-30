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

RAG_SEARCH(table, vector_column, query_vector, k [, options_json])
    Composes VEC_SEARCH + FTS_SEARCH (hybrid RRF fusion) + RAG_CONTEXT_FROM
    (neighbor-chunk expansion) into a single call, so a typical RAG retrieval
    pipeline no longer needs to be hand-assembled from those three
    primitives. Vector-only by default; options_json (5th arg, a JSON
    string) opts into hybrid fusion and/or context expansion:
      metric              same as VEC_SEARCH (default 'cosine')
      index               same as VEC_SEARCH (default 'flat')
      text_column          }  set a text source and a query to enable BM25
      text_columns         }  hybrid fusion via RRF (requires key_columns too).
      text_query           }  text_columns searches several columns in one
                              pass — prefer it on a chunk table so a short,
                              discriminative heading contributes lexical
                              signal alongside the body text.
      key_columns         []string identifying a row across the independent
                           vector/text candidate sets (required for hybrid)
      auto_or_expand       OR-expand text_query's terms (default true) —
                           see FTS_SEARCH's implicit-AND caveat
      candidate_k          candidates fetched per pass before fusion/truncation
                           to k (default k*4)
      rrf_k                RRF constant (default 60)
      expand_before,
      expand_after         }  set either to enable RAG_CONTEXT_FROM-style
      doc_id_column,       }  neighbor-chunk expansion of the final hit set
      chunk_index_column   }  (doc_id_column/chunk_index_column required)
    Returns every column of ` + "`table`" + ` plus whichever of _vec_distance/
    _vec_similarity/_vec_rank, _fts_score/_fts_rank, _rrf_score/_rrf_rank,
    or _hit_rank/_context_offset/_context_hits/_context_rank apply to the
    requested mode. Because RAG_SEARCH computes its own similarity/rank
    values internally (rather than trusting a caller-supplied column), it
    sidesteps the distance-vs-similarity and metric-mismatch footguns noted
    under VEC_SEARCH above by construction.
    Example (hybrid + context expansion in one call):
      SELECT * FROM RAG_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1,0.2,0.9]'), 5, '{
        "text_columns": ["heading", "chunk_text"],
        "text_query": "timeout OR retry",
        "key_columns": ["doc_id", "chunk_index"],
        "expand_before": 1,
        "expand_after": 1,
        "doc_id_column": "doc_id",
        "chunk_index_column": "chunk_index"
      }')

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
  CONTAINS_ALL(text, term1, term2, ...)    -> bool, ALL terms present.
                              Simpler than FTS_MATCH: no tokenizing/stemming,
                              plain case-insensitive substring match. Good for
                              exact codes/IDs/numbers or a quick "must contain
                              everything" filter without FTS_MATCH's syntax.
  CONTAINS_ANY(text, term1, term2, ...)    -> bool, ANY term present (same
                              case-insensitive substring semantics)
  CONTAINS_SCORE(text, term1, term2, ...)  -> int count of terms found (0..N),
                              handy in ORDER BY to rank by term-match count

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
