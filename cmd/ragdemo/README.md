# Local LM Studio RAG evaluation

Part of [tinySQL](../../README.md). This demo makes retrieval quality visible
before asking an LLM for an answer. It chunks repository Markdown files, gets
embeddings from a local OpenAI-compatible LM Studio server, stores them in
tinySQL, and reports retrieved chunks, ranks, Hit@k, and MRR.

Start LM Studio on port 1234 with an embedding model, then run:

```sh
go run ./cmd/ragdemo -verbose

# Compare vector-only and hybrid retrieval
go run ./cmd/ragdemo -hybrid=false
go run ./cmd/ragdemo -hybrid=true

# Inspect one question and optionally generate a grounded answer
go run ./cmd/ragdemo \
  -query "How do I expand a vector hit with its neighboring chunks?" \
  -generate
```

Hybrid mode passes text and vector queries to one RAG_SEARCH call. tinySQL fuses
vector and BM25 ranks with reciprocal-rank fusion, so an exact identifier can
still surface when an embedding does not represent it well.

## Tune and interpret results

Use -chunk-size, -overlap, -candidate-k, and -top-k to change retrieval. Run
go run ./cmd/ragdemo -help for model and endpoint options.

The built-in quality gate requires each expected source and marker-bearing chunk
to appear in the top-k results. Chunk labels use the complete heading path, and
hash lines in fenced code blocks do not start a new section.

The demo prints loading, embedding, and retrieval timings to help inspect one
local run. They include first-query index preparation and are not a controlled
performance comparison. No embedding or answer cache is used.

For production schema design, ingestion, evaluation, tuning, and context
expansion, see the [RAG guide](../../docs/rag-guide.md).
