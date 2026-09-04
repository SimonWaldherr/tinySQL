# Local LM Studio RAG evaluation

Part of [tinySQL](../../README.md). See the root guide and
[RAG guide](../../docs/rag-guide.md) for the engine-side retrieval features.

Makes retrieval quality inspectable instead of judging only the final LLM
answer. Chunks the repository Markdown docs, gets embeddings from an
OpenAI-compatible LM Studio server, stores them in tinySQL, and reports the
retrieved chunks, cosine similarities, vector/BM25 ranks, Hit@k, and MRR.

Retrieval itself is a single `RAG_SEARCH` call. Hybrid mode hands the engine a
text query alongside the query vector and it fuses the vector and BM25 rankings
with reciprocal-rank fusion, so a chunk either retriever found can reach the
final results — including one matched only by an exact identifier the embedding
model does not represent well. Because fusion happens in the engine, the
printed `vec=` and `fts=` ranks show which pass contributed each hit, and `-`
means that pass did not return it at all.

Start LM Studio's local server on port 1234 and load an embedding model. Then:

```sh
go run ./cmd/ragdemo -verbose

# Compare vector-only and hybrid retrieval
go run ./cmd/ragdemo -hybrid=false
go run ./cmd/ragdemo -hybrid=true

# Inspect one question, optionally with a grounded answer
go run ./cmd/ragdemo \
  -query "How do I expand a vector hit with its neighboring chunks?" \
  -generate
```

Tuning knobs: `-chunk-size`, `-overlap`, `-candidate-k`, `-top-k`. Run
`go run ./cmd/ragdemo -help` for model and endpoint options.

The built-in quality gate requires every expected source *and marker-bearing
chunk* to occur in the top-k results, so a neighboring but irrelevant chunk
does not count as a success.

Chunking is heading-aware: each chunk is labeled with its full heading path
(`VEC_SEARCH › Options`) rather than the nearest heading alone, and a `#` line
inside a fenced code block is treated as a shell comment, not a section break.

On the repository docs with Granite Embedding 278M Multilingual, the tested
default of 900 characters with 250 characters overlap reached 100% Hit@5,
66.7% Hit@1, and 0.792 MRR across the built-in English/German questions. Those
figures predate the switch to engine-side RRF fusion; rerun the suite to
measure the current pipeline.

## Startup and timing

Chunk insertion uses SQL batches of at most 128 rows. Evaluation questions are
embedded in batches controlled by `-batch-size` (one request for the default six
questions), then each question gets its own fresh `RAG_SEARCH`. No embedding or
answer cache is added. Embedding response indices are validated before attaching
vectors to inputs, including responses returned out of order.

The demo prints corpus embedding time, database load time, evaluation query
embedding time and each retrieval time separately. First-query index preparation
is included in retrieval timing; these numbers are not a controlled benchmark.
Ctrl-C cancels the active request and database work. The complete LM Studio demo
still needs a running embedding model; the Go tests use local fixtures.

See [posting-based RAG retrieval](../../docs/rag-postings-performance.md) for
engine-side exact top-k improvements and reproducible benchmark results.
