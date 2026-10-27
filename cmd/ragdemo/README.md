# Local RAG retrieval demo

Part of [tinySQL](../../README.md). This demo chunks repository Markdown files,
stores their vectors in tinySQL, and shows ranked retrieval results. It can use
local word features without a server or model embeddings from an
OpenAI-compatible LM Studio server. Model-based evaluation also reports Hit@k
and MRR before optionally asking an LLM for an answer.

To try retrieval without installing a model or starting a server:

```sh
go run ./cmd/ragdemo -offline
go run ./cmd/ragdemo -offline -query "How do I use VEC_SEARCH?" -top-k 3
go run ./cmd/ragdemo -offline -query "VEC_WARM" -hybrid=false
```

Offline mode uses deterministic hashed word-presence features, not semantic
embeddings; it illustrates the retrieval pipeline but does not measure
embedding-model quality. It runs a sample question when `-query` is omitted.
The built-in evaluation suite and `-generate` require LM Studio; `-generate`
cannot be combined with `-offline`.

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
