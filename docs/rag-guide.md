# Using TinySQL for RAG

TinySQL ships everything a single-process RAG (Retrieval-Augmented
Generation) pipeline needs — vector storage, SIMD-accelerated k-NN search,
BM25 full-text search, recency/quality scoring, and chunk-context expansion —
embedded in your Go program, with no external vector database. All examples
below run as-is.

See also: [Storage & Persistence Guide](./storage-guide.md) for read-only
serving, and [Developer Integration Guide](./developer-integration.md) for
embedding TinySQL in Go, `database/sql`, or WASM.

## 1. Store chunks and embeddings

Declare the embedding column as `VECTOR` (alias: `EMBEDDING`). Embeddings come
from your embedding model; TinySQL stores them as `[]float64`.

```sql
CREATE TABLE chunks (
    doc_id      TEXT,
    chunk_index INT,
    chunk_text  TEXT,
    created_at  TEXT,
    quality     FLOAT,
    embedding   VECTOR
);

INSERT INTO chunks VALUES
    ('doc-1', 0, 'TinySQL is a lightweight SQL engine...',
     '2026-07-01 10:00:00', 0.9, VEC_FROM_JSON('[0.12, -0.03, 0.87]'));
```

From Go, pass the vector directly as a query parameter — both a `[]float64`
value and a JSON string through `VEC_FROM_JSON(?)` work with the
`database/sql` driver:

```go
vec := embed(chunkText) // []float64 from your embedding model
db.ExecContext(ctx, `INSERT INTO chunks VALUES (?, ?, ?, ?, ?, ?)`,
    docID, idx, chunkText, createdAt, quality, vec)
```

`VEC_TO_BYTES`/`VEC_FROM_BYTES` round-trip vectors through a compact float32
encoding — half the size of `float64` *on the wire*, which is useful as a
portable serialization/interchange format for export or transport. Note the
current `VEC_TO_BYTES` output is a hex string, so storing it in a `TEXT` column
is **not** smaller than a native `VECTOR`; treat it as an interchange format,
not an in-table memory optimization.

## 2. Retrieve: VEC_SEARCH

`VEC_SEARCH(table, column, query_vector, k [, metric [, index]])` returns the
k nearest rows plus `_vec_distance`, `_vec_similarity`, and `_vec_rank`.
`_vec_distance` is lower = closer; `_vec_similarity` is higher = closer (for
cosine: `1.0 - _vec_distance`, matching `VEC_COSINE_SIMILARITY`'s [-1, 1]
range). **Feed `_vec_similarity`, not `_vec_distance`, into `RAG_HYBRID_SCORE`
/ `RAG_RANK_SCORE`** — those functions expect a similarity, and passing a
distance silently inverts the ranking (no error is raised):

```sql
SELECT doc_id, chunk_index, chunk_text, _vec_distance, _vec_similarity, _vec_rank
FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 5, 'cosine');
```

Metrics: `cosine` (default), `l2`/`euclidean`, `manhattan`/`l1`,
`dot`/`inner_product`. Index modes:

| Index | Behavior | Use when |
|---|---|---|
| `flat` (default) | Exact scan; SIMD + multi-core, column cache | Default choice; stays in low single-digit ms up to ~100k rows |
| `ivf` | Approximate (inverted file); ~2-3x faster than flat | Larger tables, small recall loss acceptable |
| `hnsw` | Approximate graph; fastest repeated queries, highest build cost | Static data, many queries; prebuild with `VEC_WARM` |

Indexes and column caches build lazily on first query and invalidate
automatically on writes. After a bulk load, prebuild them explicitly so no
query pays the one-time cost:

```sql
SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'hnsw');
```

Prefer `VEC_SEARCH` over `ORDER BY VEC_COSINE_SIMILARITY(...) LIMIT k` for
plain k-NN — it uses cached norms, a top-k heap, and a parallel scan
(~7x faster at 12k rows). The `ORDER BY` form is still fast and the right
tool when the ranking expression blends more than similarity.

## 3. Rerank: blend similarity with freshness and quality

`RAG_RANK_SCORE(similarity, ts, half_life_days, quality [, w_sim, w_rec, w_q])`
combines normalized similarity, exponential recency decay, and a quality
signal; `RAG_HYBRID_SCORE` (similarity + recency) and `RECENCY_SCORE` are the
simpler variants. Use `_vec_similarity` from `VEC_SEARCH` directly:

**Cosine only:** `RAG_HYBRID_SCORE`/`RAG_RANK_SCORE` normalize similarity
assuming it already falls in the cosine `[-1, 1]` range (`(sim + 1) / 2`,
clamped to `[0, 1]`). `_vec_similarity` only satisfies that for the `cosine`
metric. For `l2`/`euclidean`, `manhattan`/`l1`, or `dot`/`inner_product`
searches, `_vec_similarity` is an unbounded, always-non-positive value that
clamps to a flat `0` for nearly every row — the similarity term silently
drops out and ranking degrades to recency/quality only, with no error. Use
the `cosine` metric when reranking with these functions, or pre-normalize
your similarity into `[-1, 1]` before calling them.

```sql
WITH hits AS (
    SELECT * FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 20, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text,
       RAG_RANK_SCORE(_vec_similarity, created_at, 30, quality, 0.65, 0.25, 0.10) AS score
FROM hits
ORDER BY score DESC
LIMIT 5;
```

`RECENCY_SCORE`/`RAG_HYBRID_SCORE`/`RAG_RANK_SCORE` all take an optional
trailing `now` argument. When omitted, they default to the timestamp the
current SQL statement started executing (stable across every row of one
query, so ranking within a single result set is self-consistent) — not a
fresh `time.Now()` per row. Pass `now` explicitly for scores that must stay
identical across separate statement executions (e.g. golden-file tests, or
pipelines run at different times over the same data).

Retrieve generously (k=20), rerank, then keep the top few — reranking is
cheap compared to a second retrieval round.

## 4. Expand context: neighboring chunks

LLM answers improve when retrieved chunks arrive with their surrounding text.
`RAG_CONTEXT_FROM` takes a hit set (a CTE or table) and returns each hit plus
its neighbors within the same document, annotated with `_hit_rank`,
`_context_offset` (position relative to the best supporting hit), and
`_context_rank`:

```sql
WITH topk AS (
    SELECT doc_id, chunk_index
    FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 5, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset, _context_hits
FROM RAG_CONTEXT_FROM('chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1, 1)
ORDER BY _context_rank;
```

The trailing `1, 1` fetches one chunk before and one after each hit.
`RAG_CONTEXT` does the same for a single known chunk. Overlapping windows are
deduplicated: `_context_hits` is the number of retrieved hits that contributed
that chunk, while `_hit_rank` and `_context_offset` come from its best-ranked
supporting hit. This gives prompt builders a cheap, explainable confidence
signal without repeating text.

## 5. Hybrid retrieval: vectors + keywords

Embeddings miss exact identifiers, error codes, and rare terms; BM25 misses
paraphrases. Fuse both with reciprocal rank fusion (RRF) over `VEC_SEARCH`
and `FTS_SEARCH`:

```sql
SELECT c.doc_id, c.chunk_index, c.chunk_text,
       1.0/(60.0 + COALESCE(v._vec_rank, 1000))
     + 1.0/(60.0 + COALESCE(f._fts_rank, 1000)) AS rrf_score
FROM chunks c
LEFT JOIN (SELECT doc_id, chunk_index, _vec_rank
           FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 20, 'cosine')) v
    ON v.doc_id = c.doc_id AND v.chunk_index = c.chunk_index
LEFT JOIN (SELECT doc_id, chunk_index, _fts_rank
           FROM FTS_SEARCH('chunks', 'hnsw index build', 20, 'chunk_text')) f
    ON f.doc_id = c.doc_id AND f.chunk_index = c.chunk_index
WHERE v.doc_id IS NOT NULL OR f.doc_id IS NOT NULL
ORDER BY rrf_score DESC
LIMIT 5;
```

Always pass the text column(s) to `FTS_SEARCH` explicitly (the trailing
`'chunk_text'` above). With no column list it searches *every* column,
including the `embedding` VECTOR — tokenizing thousands of float values into
the index, wasting memory and polluting ranking. Also note that `FTS_SEARCH`
treats adjacent terms as an implicit **AND**, so a verbose natural-language
question can match nothing; for question-style input, OR-expand the terms
(e.g. `'error OR timeout OR retry'`) or keep queries to the key terms.

For a lighter variant, retrieve by vector and rerank with the scalar
`FTS_RANK` (BM25) inside one CTE:
`0.7 * _vec_similarity + 0.3 * FTS_RANK(chunk_text, 'query terms')`.
`FTS_SNIPPET` and `FTS_HIGHLIGHT` format the matched passages for prompts.

## 6. Serving and performance notes

- **Load once, serve read-only.** Bulk-insert into a snapshot, then reopen it
  with `ReadOnly: true` and run `VEC_WARM` at startup (full example in the
  [Storage & Persistence Guide](./storage-guide.md#read-only-serving)).
- **Query-vector literals are free.** `VEC_FROM_JSON('[...]')` with a literal
  argument is folded to a constant at parse time — it is not re-parsed per
  row, so passing the query vector as JSON text costs nothing.
- **SIMD is automatic.** Distance kernels use AVX2+FMA on amd64 (detected at
  startup, SSE2 fallback) and NEON on arm64, with a portable fallback
  everywhere else; no build tags or cgo required.
- **Repeated statements skip the parser.** Through the `database/sql`
  driver, SELECT/EXPLAIN statements up to 8 KB are cached by their final
  SQL text, so re-issued query templates don't re-parse. Vector caches and
  ANN indexes are dropped eagerly on `DROP TABLE` and bounded overall, so
  long-running services don't accumulate memory from schema churn.
- **Context expansion scales with the window, not the source × hit count.**
  `RAG_CONTEXT` and `RAG_CONTEXT_FROM` build a document/chunk index once per
  query and binary-search each requested window. Keep `chunk_index` monotonic
  within a document for predictable prompt ordering.
- **Exposing the schema to an LLM agent:** `tsql.BuildAgentContext(...)`
  renders a compact, token-budgeted schema summary for system prompts, and
  `cmd/tinysql-mcp-server` serves the database over MCP.

See [BENCHMARKS.md](../BENCHMARKS.md) for measured numbers on the vector and
RAG query paths.
