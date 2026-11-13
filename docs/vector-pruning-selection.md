# Exact pruning and vector result selection

## Research and scope

[SuperKMeans (2026)](https://arxiv.org/abs/2603.20009) accelerates clustering
with previous assignments, partial distances and progressive dimension pruning.
Its [default implementation](https://github.com/cwida/SuperKMeans/blob/f00b55615ffea1ff2ce521de20de1f9479982618/include/superkmeans/common.h)
disables its probabilistic pruning for small cluster counts, including tinySQL's
maximum of 64. tinySQL uses a lightweight, exact prefix screen suitable for its
existing float64, vector-at-a-time kernels.

Faiss's [result handlers](https://github.com/facebookresearch/faiss/blob/main/faiss/impl/ResultHandler.h)
choose top-1, heap or buffered reservoir selection according to `k`. tinySQL
adopts bulk initialization of the existing heap for larger result counts. This
keeps its current memory bound and distance/row-ID ordering.

## Previous assignments as exact upper bounds

For L2 IVF training at 256 or more dimensions and at least 32 centroids, a
previous assignment supplies an initial candidate after the first pass. Its
distance to the **current** centroid is recomputed in full. The final list
assignment can use the same hint after centroids move.

Each other candidate first receives a distance over 64 coordinates. That is a
lower bound relative to the existing full kernel: 64 ends on every current
backend's accumulator groups, and extending their nonnegative accumulators
and applying the same reduction cannot decrease the result. Tests check this
property for portable, NEON, SSE2 and AVX2 kernels, including all-zero tails
that expose changes in floating-point reduction order.

A candidate whose prefix cannot beat the incumbent is skipped. A surviving
candidate is scored again with the original full kernel. Lower-index ties are
preserved; a zero incumbent can finish once earlier indices have been checked.
Invalid hints and the `MaxFloat64` sentinel retain the full-scan fallback.
After four consecutive unproductive prefix checks, the rest of that assignment
uses full distances. This bounds wasted work on poorly separated vectors.

The first training pass, other metrics and smaller shapes use the existing
path. Seed draws, updates, convergence checks, final assignments and public
distance calculations retain their previous numerical behavior.

## Bulk initialization for larger top-k scans

A flat scan with `k >= 64` and at least 64 source rows collects its first
accepted candidates in the already reserved result slice. It then constructs
the heap bottom-up once, replacing repeated initial heap insertion. The
remaining scan uses its previous threshold and heap operations.

Underfilled worker results are heapified too, so parallel merging receives
valid heaps. NaN exclusions, override-provided norms, physical-row cancellation
checkpoints and result ownership are preserved. Small `k` retains the existing
scan, and HNSW's incremental frontier continues to be a valid heap throughout.

## Ordering complete result sets

When a result heap already has every entry in worst-first order, reversing its
backing slice replaces the final heap drain. This linear check applies to full
results of at least 256 entries and stops at the first out-of-order pair.
Unordered heaps retain the existing drain and ownership transfer. Partial
drains keep their previous behavior too.

Parallel scans whose combined worker results fit within `k` can sort those
worker heaps separately and merge their ordered runs through a small frontier.
This path requires at least 1,024 candidates, 2–64 workers and disjoint physical
row ranges. Counts and worker errors are checked before examining or changing
any heap. The output reserves exactly the combined result size, reducing
unused capacity when NULLs, filters or mixed dimensions exclude source rows.
If the previous merge could reuse its first worker buffer without allocation,
that reuse still takes priority.

One active worker run transfers its buffer when capacity equals length; empty
worker headers are cleared too, so their spare capacity cannot overwrite the
returned result. Oversized buffers are copied into a tightly sized output.
Two active runs use a direct two-way merge followed by a block copy of the
remaining tail. Larger worker sets continue to use the frontier heap.

The frontier, run slices and positions use bounded local arrays. Multiple runs
need one output allocation; transferring a single tight buffer needs none.
There is no per-row state beyond existing scored rows. NaNs,
negative zero, invalid heaps and overlapping row ranges use the previous merge
to retain its bit-level behavior and avoid reordering aliased worker buffers.
These changes do not alter distance calculations or select fewer candidates.

## Verification

Independent full-distance and pre-change scan references compare centroid
choices, centroid bits, partition membership and final results. Cases include
unaligned vector tails, near ties, signed zero, nonfinite values, invalid hints,
mixed dimensions, segmented caches, overrides, missing norms, sparse workers,
oversized `k` and cancellation.

```sh
go test ./internal/engine/... \
  -run 'Test(IVFPruned|VectorTopKBatch|VectorL2AlignedPrefix)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkIVFPrunedBuild|BenchmarkVectorTopKBatch)$' \
  -benchtime=100ms -count=5 -benchmem
```

Build fixtures contain 4,096 vectors with 256–1,536 dimensions, covering
Gaussian, settled and noisy separated distributions. Scan fixtures contain
8,192 two-dimensional vectors and vary score order and `k`. Additional layout
benchmarks cover cosine norms, UPDATE overrides and mixed dimensions. Setup is
outside timed loops; scans include final result ordering.

## Measurements and limits

Apple M2 Max, Go 1.27.4, `GOMAXPROCS=2`, baseline `9160468`. High host load
distorted some initial wall-time samples, so the table uses a paired CPU-time
comparison. The previous function bodies and current paths ran in the same
process over identical fixtures, alternating invocation order. Process CPU time
was measured with `getrusage(RUSAGE_SELF)`; entries are medians of five runs,
with four old/new pairs per build case and 100 pairs per scan case.

| Workload | Before CPU time | After CPU time |
|---|---:|---:|
| IVF build, noisy separated 256D | 20.064 ms | 13.314 ms |
| IVF build, noisy separated 384D | 28.570 ms | 16.995 ms |
| IVF build, noisy separated 768D | 66.405 ms | 31.570 ms |
| IVF build, noisy separated 1536D | 132.489 ms | 60.761 ms |
| IVF build, Gaussian 384D control | 28.020 ms | 28.361 ms |
| Flat scan, random scores, k=8192 | 883.30 µs | 730.34 µs |
| Flat scan, ascending scores, k=8192 | 706.13 µs | 516.94 µs |
| Flat scan, tied scores, k=8192 | 837.70 µs | 751.32 µs |
| Flat scan, random scores, k=10 control | 72.57 µs | 72.58 µs |

The four Gaussian build controls changed by 0–1.3%; settled controls varied
between 1.2% faster and 0.6% slower. For `k=8192` or oversized `k`, scan savings
ranged from 9.2% to 27.5%. Smaller result lists generally changed little. These
are synthetic distributions; correlated embeddings and different prefix signal
can change the payoff. The four-miss cutoff reduces unproductive screening.

Pruning allocates no additional per-row state: it reuses the existing byte
assignments. The selection phase reuses the same result allocation. Ordinary
scan benchmarks retain one allocation with exactly `16*min(k,8192)` bytes in
these fixtures. End-to-end build timings exclude column-cache creation, and
the speedups do not predict total database startup time.

Root and minimal suites, targeted race tests, Go 1.27.3 checks and Vet passed.
The prefix contract also runs in portable WebAssembly with `GOMAXPROCS=1`;
the existing browser/Node WASM integration smoke tests passed. AMD64 contract
tests were cross-compiled locally; their native SIMD execution belongs to CI.

## Result ordering measurements

The ordering changes use baseline `1b07097`, the same machine and Go version.
Again, shared host load made wall time unreliable. These are process CPU-time
medians of five runs, each alternating 50 old/new pairs over identical inputs.
The references retain the previous merge and heap-drain function bodies.

| Complete search workload | Before CPU time | After CPU time |
|---|---:|---:|
| Flat, 8,192 random 2D vectors, k=8,192 | 794.32 µs | 772.44 µs |
| Flat, 8,192 descending-score 2D vectors, k=8,192 | 608.12 µs | 97.40 µs |
| Flat, 8,192 tied-score 2D vectors, k=8,192 | 756.08 µs | 742.12 µs |
| Flat, random 2D vectors, k=10 control | 72.46 µs | 71.84 µs |
| Parallel, 8,192 physical rows, k=10,000 | 901.14 µs | 729.16 µs |
| Parallel filtered, 4,096 allowed rows, k=10,000 | 421.12 µs | 387.56 µs |
| Parallel, k=400 control | 190.64 µs | 191.32 µs |

Parallel fixtures include missing vectors, mixed dimensions and repeated
distances. Their times include scoring, worker scheduling, selection and final
ordering. The descending case benefits especially from an already ordered
heap; it does not predict typical RAG latency at small `k`. The small-result
controls changed by less than 2% in these runs.

Focused merge benchmarks over 8,192 valid results retain one 131,072-byte
allocation with two or eight workers. A sparse-worker regression case verifies
that 1,024 accepted results reserve 1,024 entries rather than 4,096 source rows.
The fixtures and pre-change references are available in
`vector_result_sort_benchmark_test.go` and `vector_full_merge_benchmark_test.go`:

```sh
go test ./internal/engine -run 'TestVector(FullMerge|ResultSort)' -count=1
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVector(ResultSort|FullMerge|SparseMerge|ParallelHeapMerge)$' \
  -benchtime=150ms -count=5 -benchmem
```

The tests compare row IDs and distance bits, check partial drains and buffer
ownership, and exercise oversized `k`, sparse workers, duplicate IDs,
infinities, NaNs, signed zero, invalid heaps, aliasing and worker errors.

Both complete native profiles passed on an isolated copy of HEAD with only
these engine changes applied. Targeted race and Go 1.27.3 checks, Vet and
documentation checks passed too. The new ordering and ownership tests also
passed under `js/wasm`; full/minimal browser and Node integration checks passed.

### Direct merge follow-up

Compared with `a40b704` on the same machine and toolchain, the one/two-run
dispatch avoids maintaining the frontier's positions and root for each result.
The general frontier loop now lives in a separate routine. Output ownership,
row-ID tie breaking and all eligibility/fallback checks are unchanged.

These process CPU-time medians come from five runs, each alternating 50
old/new batches of five merges over restored, identical worker heaps.

| Two active workers | Before CPU time | After CPU time |
|---|---:|---:|
| 1,024 random scores | 41.936 µs | 36.604 µs |
| 8,192 random scores | 644.404 µs | 599.348 µs |
| 8,192 tied scores | 606.224 µs | 582.324 µs |
| 8,192 descending scores | 70.848 µs | 52.352 µs |

Across all six two-worker cases, savings were 3–36%. Eight-worker controls with
8,192 results varied from 2.2% faster to 0.8% slower. At 1,024 results those
controls varied from 2.3% faster to 4.8% slower (up to 1.6 µs more CPU time).
The optimization therefore benefits two active runs; it is not a general
speedup for every worker count. These are merge-phase measurements, including
input restoration and final ordering, rather than total RAG query timings.

The 8,192-result fixtures retain one 131,072-byte output allocation. The native
suite, targeted minimal/race and WebAssembly ordering tests, Vet and documentation
link checks passed. Sparse-worker tests cover one, two and three active runs,
including gaps in worker IDs, and compare result bits with the previous merge.

### Single-run ownership follow-up

The direct-merge measurements above precede the single-run ownership transfer.
Against that implementation, `BenchmarkVectorSparseMerge` with four workers,
4,096 source candidates and one full 1,024-row run reduces merge allocations
from **16,384 B/op and one allocation to 0 B/op and zero allocations**. Three
runs of 100 iterations each gave the same allocation counts. Timing claims are
omitted because host load varied between runs.

A run with spare capacity still copies into a 16,384-byte result, and two active
runs retain one 32,768-byte allocation. The ownership regression test checks
buffer reuse, oversized-buffer refusal and empty slices that alias the active
run. Reusing any worker header after the merge must leave returned rows intact.
