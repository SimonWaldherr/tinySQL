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

For memory-constrained setups, `VEC_TO_BYTES`/`VEC_FROM_BYTES` round-trip
vectors through a compact float32 encoding at half the storage cost.

## 2. Retrieve: VEC_SEARCH

`VEC_SEARCH(table, column, query_vector, k [, metric [, index]])` returns the
k nearest rows plus `_vec_distance` and `_vec_rank`:

```sql
SELECT doc_id, chunk_index, chunk_text, _vec_distance, _vec_rank
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

Cosine *distance* converts to similarity as `1.0 - _vec_distance`.
`RAG_RANK_SCORE(similarity, ts, half_life_days, quality [, w_sim, w_rec, w_q])`
combines normalized similarity, exponential recency decay, and a quality
signal; `RAG_HYBRID_SCORE` (similarity + recency) and `RECENCY_SCORE` are the
simpler variants:

```sql
WITH hits AS (
    SELECT * FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 20, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text,
       RAG_RANK_SCORE(1.0 - _vec_distance, created_at, 30, quality, 0.65, 0.25, 0.10) AS score
FROM hits
ORDER BY score DESC
LIMIT 5;
```

Retrieve generously (k=20), rerank, then keep the top few — reranking is
cheap compared to a second retrieval round.

## 4. Expand context: neighboring chunks

LLM answers improve when retrieved chunks arrive with their surrounding text.
`RAG_CONTEXT_FROM` takes a hit set (a CTE or table) and returns each hit plus
its neighbors within the same document, annotated with `_hit_rank`,
`_context_offset` (position relative to the hit), and `_context_rank`:

```sql
WITH topk AS (
    SELECT doc_id, chunk_index
    FROM VEC_SEARCH('chunks', 'embedding', VEC_FROM_JSON('[0.1, 0.0, 0.9]'), 5, 'cosine')
)
SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset
FROM RAG_CONTEXT_FROM('chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1, 1)
ORDER BY _context_rank;
```

The trailing `1, 1` fetches one chunk before and one after each hit.
`RAG_CONTEXT` does the same for a single known chunk.

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
           FROM FTS_SEARCH('chunks', 'hnsw index build', 20)) f
    ON f.doc_id = c.doc_id AND f.chunk_index = c.chunk_index
WHERE v.doc_id IS NOT NULL OR f.doc_id IS NOT NULL
ORDER BY rrf_score DESC
LIMIT 5;
```

For a lighter variant, retrieve by vector and rerank with the scalar
`FTS_RANK` (BM25) inside one CTE:
`0.7 * (1.0 - _vec_distance) + 0.3 * FTS_RANK(chunk_text, 'query terms')`.
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
- **Exposing the schema to an LLM agent:** `tsql.BuildAgentContext(...)`
  renders a compact, token-budgeted schema summary for system prompts, and
  `cmd/tinysql-mcp-server` serves the database over MCP.

See [BENCHMARKS.md](../BENCHMARKS.md) for measured numbers on the vector and
RAG query paths.
