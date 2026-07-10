# Local LM Studio RAG evaluation

This demo makes retrieval quality inspectable instead of judging only the final
LLM answer. It chunks the repository Markdown docs, obtains embeddings from an
OpenAI-compatible LM Studio server, stores them in TinySQL, and reports the
actual retrieved chunks, cosine similarities, vector/BM25 ranks, Hit@k, and MRR.
Hybrid mode conservatively reranks the semantic candidate set with BM25, so a
weak keyword-only hit cannot displace a strong semantic match.

Start LM Studio's local server on port 1234 and load an embedding model. Then:

```sh
go run ./cmd/ragdemo -verbose
```

Compare vector-only and hybrid retrieval:

```sh
go run ./cmd/ragdemo -hybrid=false
go run ./cmd/ragdemo -hybrid=true
```

Inspect one question and optionally generate a grounded answer:

```sh
go run ./cmd/ragdemo \
  -query "How do I expand a vector hit with its neighboring chunks?" \
  -generate
```

Useful optimization knobs are `-chunk-size`, `-overlap`, `-candidate-k`, and
`-top-k`. Run `go run ./cmd/ragdemo -help` for model and endpoint options. The
built-in quality gate requires every expected source *and marker-bearing chunk*
to occur in the top-k results, so a neighboring but irrelevant chunk does not
count as a success.

On the repository docs with Granite Embedding 278M Multilingual, the tested
default of 900 characters with 250 characters overlap reached 100% Hit@5,
66.7% Hit@1, and 0.792 MRR across the built-in English/German questions.
