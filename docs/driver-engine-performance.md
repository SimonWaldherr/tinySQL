# Driver and engine allocation improvements

The September 2026 changes reduce temporary allocations and scheduling work in
embedding, connection admission, parameter binding, and retrieval. Public SQL
syntax, parameter validation, search ranking, and worker-count thresholds stay
unchanged.

## Driver lifecycle and connection admission

- Storage owns GOB type registration; the driver's `init` only registers the
  `database/sql` driver. Both embedding paths share the same server defaults.
- `SetDefaultDB` constructs the server before taking the global lock.
  `OpenWithDB` constructs its private connector directly without parsing a DSN.
- `OpenWithConfig` defers cancellation only when it creates a timeout context.
  Caller-owned native databases must still be closed after their SQL pools.
- Reader/writer admission checks an already canceled context before accepting
  a slot. The uncontended path avoids timer and deadline bookkeeping. If the
  context deadline precedes the busy timeout, admission waits on that context
  directly, preserving `context.DeadlineExceeded` instead of racing a second
  timer and potentially reporting a busy timeout.
- Binding SQL with no arguments and no possible placeholder characters
  (`?`, `$`, `:`) returns the original string. Other queries retain the existing
  placeholder validation and escaping path.

These changes do not introduce a background worker pool or change pool limits.
Cancellation concurrent with slot acquisition can still occur after admission;
query execution remains responsible for honoring its context.

## Retrieval memory and workers

- Parallel vector, lexical, and filtered RAG scans execute their final chunk on
  the calling goroutine after launching the other chunks. Results retain their
  per-worker slots and deterministic merge order.
- RAG allowed-ID filters size integer, float, and string maps for the actual
  number of values of each type. The integer-to-float lookup map is built only
  when a float cell needs that cross-type comparison. Integer precision and
  mixed-type SQL equality semantics remain unchanged.
- Already sorted row IDs are filtered and deduplicated into a new slice without
  a map or another sort. Unsorted inputs retain the map-based path. The output
  does not alias the input.
- Draining a complete vector or FTS top-k heap reuses its backing slice for the
  ordered result. The emptied heap drops its reference so later pushes cannot
  overwrite returned results. Partial drains retain a separate output slice.

## Local measurements

Measured on Apple M2 Max, darwin/arm64, Go 1.27.1, GOMAXPROCS=12. Each baseline
is the implementation immediately before the corresponding optimization, with
other working-tree changes retained using Go's `-overlay` option. These are
isolated microbenchmarks, not end-to-end query latency or throughput promises.

| Benchmark | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Free pool slot, context with deadline | 66–67 ns | 28 ns | 0 → 0 | 0 → 0 |
| Binding SQL without parameters | 133 ns | 28 ns | 64 → 0 | 1 → 0 |
| Normalize 10,000 sorted row IDs | 627–633 µs | 22–32 µs | ~377,480 → 81,920 | 34 → 1 |
| Filter 10,000 integer IDs | 619–691 µs | 249–291 µs | ~1,405,442 → 377,473 | 133 → 34 |
| Build and drain vector heap, k=100 | 1.755–1.756 µs | 1.539–1.541 µs | 3,584 → 1,792 | 2 → 1 |
| Build and drain FTS heap, k=100 | 1.692–1.715 µs | 1.532–1.540 µs | 3,584 → 1,792 | 2 → 1 |

Pool measurements used three runs, parameter-free binding one run, and the
remaining measurements two runs per implementation. Repeat on an otherwise
idle machine before interpreting small timing differences. The worker changes
were validated for correctness; no standalone speedup is claimed for them.
Explicit stack arrays for parameter bookkeeping showed no allocation advantage
and were not retained.

Reproduce current measurements from the repository root:

```sh
go test ./internal/driver -run '^$' \
  -bench '^(BenchmarkPoolUncontended|BenchmarkBindWithoutParameters)$' -benchmem -count=3
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkRAGMapSliceAllocations|BenchmarkTopKStorage)$' -benchmem -count=3
```

## Validation

Regression tests cover canceled admission with available slots, context versus
busy deadlines, slot release, binding argument-count boundaries, exact mixed-type
ID matching, sorted/unsorted row normalization, ranking ties, partial heap drains,
and result ownership after heap reuse. Existing parallel search tests verify
serial-equivalent results and worker panic handling.

```sh
go test ./driver ./internal/driver ./internal/engine ./internal/storage
go test -race ./internal/driver ./internal/engine \
  -run 'Test(Pool|ServerAcquire|Bind|PreparedStmtConcurrentBindings|TopKTransfersStorage|FTSParallel|VecSearchTopKWorker|RAGNormalizeRowIDsOwnership|RAGAllowedIDBucketsMatchExactScan)' -count=1
```

See also the [integration guide](developer-integration.md) for embedding and
ownership, and [retrieval performance](retrieval-performance.md) for earlier
end-to-end workloads.
