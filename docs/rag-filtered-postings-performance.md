# RAG: authorized posting-list retrieval

The follow-up accelerates the lexical branch of filtered RAG/HYBRID_SEARCH.
Previously, filtered TERM and literal-OR queries scanned each candidate document
and binary-searched its term directory for every query term. They now intersect
the term posting lists with the sorted authorized candidate IDs before selecting
per-term top-k. Because literal OR uses the maximum term score, the union of
those per-term winners contains the exact global top-k. RRF fusion and vector
retrieval are unchanged.

The posting scorer uses the same filter-local IDF and average document length
as the previous scan. Global posting block minima/maxima are conservative for
any subset when evaluated with that local normalization. Unauthorized rows
never enter the per-term heaps, and context expansion keeps the same filter.
Block pruning retains outward rounding and strict comparison, preserving ties.
Large row-ID gaps use binary seeks; adjacent candidates advance monotonically.

Below 256 candidate rows the planner retains the previous scan: pilot results
showed that intersection/merge overhead outweighed the benefit for tiny filters.
Phrase, AND, NOT, and expanded wildcard queries retain their existing paths.
The unfiltered posting loop stays separate so it does not acquire a per-row
authorization branch. No public API, persistent format or cache lifetime changes.

## Measurement

The baseline includes the preceding uncommitted trigger/index/WAL/CTE work,
so the comparison isolates the RAG follow-up. Reconstruct it from commit
`1ae111c84628ec6e15cc3b66054a8d80ac21ff8f` plus the supplied
[prior-optimizations.patch](benchmarks/rag-filtered-postings/prior-optimizations.patch).
Initial three-sample measurements preceded RAG source edits.

Final runs use identical benchmark functions, Go 1.27.1 darwin/arm64 on Apple
M2 Max, GOMAXPROCS=1, GOGC=100, eight alternating before/after rounds and
300 ms per sample. Builds, tests and analysis finish outside measurement.
The shared computer is not a dedicated, thermally controlled benchmark host.
The corpus has 20,000 chunks, 96-dimensional vectors and deterministic
Zipf-distributed text. Setup, SQL parsing and cache warmup are excluded.
These are warm retrieval measurements, not index construction, disk loading,
LLM inference or network latency. Flat vector retrieval is used in hybrid tests.

The added selectivity benchmark covers all rows, 25%, 1% and two authorized
rows, with a common TERM and four-term OR query. Standalone vector, unfiltered
hybrid, selective/common FTS and phrase queries are controls. B/op is cumulative
query allocation, not retained heap. Source/binary hashes and raw runs are in
[benchmarks/rag-filtered-postings](benchmarks/rag-filtered-postings).

## Results

| Workload | Before median | After median | Paired time change (95% interval) |
|---|---:|---:|---:|
| Filtered hybrid, 25% authorized | 693.61 µs | 193.57 µs | -72.0% [-72.5, -71.5] |
| Filtered hybrid with context expansion | 688.34 µs | 197.90 µs | -74.2% [-79.8, -70.5] |
| Standalone filtered FTS branch | 518.57 µs | 44.73 µs | -91.3% [-91.5, -91.0] |
| Four-term OR core, 25% authorized | 472.92 µs | 10.71 µs | -97.7% [-97.8, -97.7] |
| Common TERM core, 25% authorized | 126.74 µs | 53.85 µs | -57.3% [-57.9, -56.8] |
| Common TERM core, 1% authorized (scan) | 4.44 µs | 4.43 µs | -0.4% [-1.3, +0.7] |
| Common TERM core, two rows (scan) | 0.36 µs | 0.38 µs | +5.9% [+3.5, +8.7] |

The hybrid gain is accompanied by 1,456 additional B/op and 15 additional
allocations for the per-term heaps and winner merge. Filtered hybrid allocation
is 40,624 → 42,080 B/op (+3.6%), or 31,240 → 32,696 (+4.7%) with expansion.
The common-term core instead saves 160 B/op and one allocation. No persistent
cache or full-corpus duplicate is added.

The tiny two-row TERM case regresses by roughly 20 ns despite retaining the
scan; its allocation count is unchanged. One-percent filters avoid the large
regressions observed when forcing the posting path in the pilot. The threshold
is a conservative heuristic, not a claim of optimality for every corpus.

Unfiltered hybrid, vector-only, common-term FTS and phrase controls have paired
intervals spanning no change. Unfiltered selective-term FTS appears faster
(-10.9%) despite no algorithm change on that path; this is not attributed to
the filtered optimization. Some hybrid samples have outliers, reflected in
wider intervals and differences between median ratios and paired estimates.
The [complete comparison](benchmarks/rag-filtered-postings/comparison.md) and
all raw samples are retained, with no outlier removal.

## Correctness

A differential test compares exact row IDs, order and float scores against the
previous document-scan implementation on 4,096 varied-length documents. Cases
include dense/sparse/empty filters, block-boundary offsets, large gaps, duplicate
and missing terms, ties, k=1/10/200, and cancellation. It checks both the posting
primitive and adaptive filtered entry point. Existing authorization-isolation,
mutation/cache invalidation, neighbor-expansion and concurrent-query tests
remain part of validation. Ranking equivalence is tested; this patch changes
no retrieval-quality or approximate-nearest-neighbor setting.

Validation passed: `go test ./...`, `go vet ./...`, and
`go test -race . ./internal/engine ./internal/storage ./internal/driver`.

## Reproduction

Extract the named commit into a temporary directory. Apply the zero-context
prior patch with `git apply --unidiff-zero PATH/prior-optimizations.patch`.
Copy `internal/engine/rag_posting_intersection_benchmark_test.go` from the final
tree into that baseline. Build both trees with the same Go compiler:

```sh
# baseline tree
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/rag-before.test
# final tree
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/rag-after.test
python3 -B docs/benchmarks/rag-filtered-postings/run.py /tmp/rag-before.test /tmp/rag-after.test /tmp/rag-results
python3 -B docs/benchmarks/columnar/compare.py /tmp/rag-results/before.txt /tmp/rag-results/after.txt
```

The comparison reports medians and paired geometric time ratios with seeded
10,000-resample bootstrap 95% intervals; the percentages need not equal the
ratio of the displayed medians.
