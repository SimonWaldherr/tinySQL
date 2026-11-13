# IVF training: initialization and convergence

The [e-means paper](https://arxiv.org/abs/2611.09501) and its
[Faiss implementation](https://github.com/facebookresearch/faiss/tree/2b648f0605f2ef3d14c4705350f8a0b214463878/contrib/e_means)
motivate checking initialization, cluster usage, training work and retrieval
quality together. e-means uses mini-batch updates with exponentially weighted
cluster sums and populations, shuffled epochs and cluster revival. Its reported
GPU speedups compare with substantially longer Lloyd training than tinySQL's
three-pass limit.

tinySQL retains up to three Lloyd updates, float64 centroids and the existing
four distance metrics. Two changes apply to the current CPU implementation:

- Initial centroids sample one valid row from each disjoint part of the input.
  A private, fixed-seed PCG generator jitters the sample positions. This retains
  coverage of sorted inputs and avoids aliasing a periodic ingestion order.
  Source rows retain their order; each centroid owns its vector copy.
- Each training pass records its assignments in the byte buffer already needed
  for packed inverted lists. If every centroid remains bit-identical, training
  stops and those assignments and counts build the lists directly. Otherwise,
  final assignments are recomputed against the final centroid positions.

This keeps exact final list membership even when training ends before
convergence. The Faiss README explicitly notes that labels captured during
training can refer to centroids that subsequently moved. Bit comparisons also
preserve signed zero and avoid a tolerance changing metric ties.

The initialization changes approximate IVF partitions and can change returned
neighbors. SQL signatures, row eligibility, exact flat search, HNSW and stored
vector formats are unchanged. Repeated builds over the same input use the same
seeds. No Faiss, Python or GPU runtime is required.

## Validation

Tests compare centroid bits, list membership, list order and search results
against independent full three-pass training with the same initial centroids.
They include all four metrics, duplicate and empty clusters, moving centroids,
mixed dimensions, zero vectors, nonfinite input, and cooperative cancellation.
Source row order and vector ownership are checked separately.

A second test uses eight separated, noisy groups in periodic, sorted and
shuffled ingestion order. Its 4,096 rows and 32 lists make a fixed stride of 128
select all starting vectors from one group in periodic order. Scalar distance
calculations and exhaustive sorting provide independent loss and held-out
recall checks. All admitted rows must occur in exactly one inverted list.
The periodic test fails with the previous initialization and passes with
stratified jitter. Search checks use the unchanged probe budget.

Commands, with a shared Go build cache and no concurrent benchmark work:

```sh
go test ./internal/engine -run 'TestIVF(Seeds|Convergence|PackedBuild)' -count=1
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkIVF(Convergence|SeedOrder)$' \
  -benchtime=200ms -count=5 -benchmem
```

`BenchmarkIVFConvergence` covers 4,096 settled or Gaussian-random vectors at
8, 64 and 384 dimensions with L2 and cosine. `BenchmarkIVFSeedOrder` uses 4,096
vectors at 64 dimensions, eight held-out queries and top-10 retrieval. It
reports build/search time, loss, occupied lists, candidate count and recall.
Fixture construction and quality evaluation are outside timed loops.

## Measurements: initialization and convergence

Apple M2 Max, Go 1.27.4, `GOMAXPROCS=2`; medians of five 200 ms runs per case.
The baseline is the engine at `a793de2`, with the same benchmark fixtures.
Baseline and changed runs were sequential. These are synthetic workloads, not
a prediction for every embedding corpus.

| Workload | Before | After |
|---|---:|---:|
| Build, settled 384D / L2 | 26.039 ms | 6.622 ms |
| Build, Gaussian 384D / L2 control | 27.126 ms | 27.126 ms |
| Build, periodic noisy 64D / L2 | 7.178 ms | 7.521 ms |
| Search, periodic noisy 64D / L2 | 27.509 µs | 15.308 µs |
| L2 candidates/query on that periodic corpus | 1,274 | 596 |
| Mean squared L2 loss on that periodic corpus | 101.4 | 1.392 |
| Recall@10 on that periodic corpus | 98.75% | 100% |

All six settled build cases (8/64/384 dimensions, L2/cosine) improved by
74–75%. Gaussian controls ranged from 1.5% faster to 2.8% slower. A moving
build allocates one additional 16-byte PCG source; stopping at a fixed point
avoids later centroid-norm allocations (44 → 43 total build allocations).
Search remains at 672 B and two allocations per query in these fixtures.

Changing initialization changes the approximate search tradeoff. Sorted and
shuffled L2 query controls took about 8–9% longer as more candidates were
visited; their recall stayed at 100%. Shuffled cosine recall was 97.5%, versus
100% before, on eight held-out queries. Periodic cosine recall improved from
97.5% to 100%, with nearly unchanged search time. The periodic noisy builds
took about 5% longer. Measure both latency and held-out recall on the target
corpus before choosing IVF; exact flat search remains available.

## Packed centroids and reusable scratch (2026-11-13)

Further research examined the CPU memory layout. Faiss keeps its
[clustering centroids](https://github.com/facebookresearch/faiss/blob/main/faiss/Clustering.cpp)
in one `d*k` vector, with assignment and distance buffers outside the iteration
loop. Its [flat-vector storage](https://github.com/facebookresearch/faiss/blob/main/faiss/IndexFlatCodes.h)
also packs fixed-size entries, and its
[performance guide](https://github.com/facebookresearch/faiss/wiki/How-to-make-Faiss-run-faster)
discusses buffer allocation and cache behavior.

tinySQL now copies the same selected centroids into disjoint, capacity-bounded
slices of one arena. Append cannot overwrite a neighboring centroid, and source
vectors remain independent. Training reuses one index-owned norm buffer,
refreshes cosine norms after centroid moves, and still computes final norms
for all four metrics. One row resolver serves every pass and final assignment;
segmented caches and UPDATE overrides retain their existing lookup path.

The query's centroid frontier uses a local 64-entry buffer, matching the
production list limit. Larger internal indexes retain dynamic allocation.
Heap removal stays local to avoid Go moving that buffer to the heap through a
pointer-to-slice helper. Results own a separate allocation, so concurrent
queries share neither scratch nor result storage.

Distance arithmetic, probe budgets and seed draws remain the same. In
particular, L2 keeps its direct squared-difference kernels: Faiss documents
[cancellation errors from norm/dot-derived L2](https://github.com/facebookresearch/faiss/wiki/FAQ#why-do-i-get-weird-results-with-brute-force-search-on-vectors-with-large-components).

### Measurements and limits

The same machine/toolchain and five-run 200 ms median procedure compare this
change with `bb39784`. Fixture creation and quality checks remain outside the
timed loops.

| Workload | Before | After |
|---|---:|---:|
| Build, moving 384D / L2, allocations | 45 | 11 |
| Same build, allocated bytes | 269,454 | 268,686 |
| Same build, elapsed time | 27.538 ms | 27.336 ms |
| Search scratch, 32 lists / cosine, allocated bytes | 672 | 160 |
| Same search, allocations | 2 | 1 |
| Same search, elapsed time | 838 ns | 816 ns |
| Search scratch, 64 lists / L2, allocated bytes | 1,184 | 160 |
| Same search, elapsed time | 1,138 ns | 1,080 ns |

All 4,096-row build cases use 11 allocations. The six ordered-corpus search
cases keep identical reported loss, occupied lists, candidate counts and recall;
their times ranged from 2.7% faster to 2.9% slower. The 129-list fallback kept
the same allocation count and took about 6% longer. Allocation reduction is the
main result; these samples do not establish a general latency improvement.

Tests retain the old seed allocation and allocating-search paths as independent
references. They compare training bits and results across all four metrics,
cache layouts and norm states; check slice ownership and append boundaries;
and exercise cancellation, ties, nonfinite input, 0–129 centroids and concurrent
readers. The search scratch benchmark includes the dynamic fallback:

```sh
go test ./internal/engine -run 'Test(IVF|VectorIndexBuild)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkIVF(Convergence|SeedOrder|SearchScratch)$' \
  -benchtime=200ms -count=5 -benchmem
```
