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

## Measurements

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
