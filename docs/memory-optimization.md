# Memory and garbage collection

tinySQL reduces GC work by avoiding unnecessary copies, limiting reusable
scratch space, and releasing references when their owners finish. It leaves
process-wide GC policy to the embedding application: opening, querying, or
closing a database does not change `GOGC` or `GOMEMLIMIT`, disable collection,
or force a collection. The host can choose those settings for its entire
workload; see the [Go GC guide](https://go.dev/doc/gc-guide).

## Measure allocation and retention separately

- **Allocation churn:** bytes and objects allocated per operation. Short-lived
  result maps and expression wrappers can increase collection frequency even
  when little survives the query.
- **Live heap:** reachable tables, snapshots, indexes, caches, results, and
  reusable buffers. Retention depends on their ownership and lifetime.
- **Scannable heap:** heap space that can contain pointers. A large numeric
  array retains bytes without adding the same scanning work as a similarly
  sized array of interfaces or row headers.

These are different measurements. Lower allocation counts do not establish a
smaller live heap, and heap usage is not peak process RSS. Clearing references
can release the objects they point to while retaining the container's backing
storage; dropping that storage may be necessary to reduce its scan footprint.

## Stored rows and transaction snapshots

Stored row slices are immutable. UPDATE builds a replacement row and replaces
its table slot; it must not overwrite an existing row's cells to save an
allocation. Existing readers, transaction snapshots, WAL before-images, and
derived indexes can still depend on the old row.

In-memory transaction shadows initially share and pin tables. A writer
detaches a pinned table before changing it, and a transaction copies a shared
table only on its first write. Backends with evictable tables use the eager
snapshot fallback. Scalar row cells can be shared; mutable BLOB, VECTOR, and
JSON contents require copying on the relevant snapshot paths.

Eligible transactions reuse row-header and constraint-index buffers after
commit. Recycling requires that the old table has no pinned readers; unsafe
cases fall back to copying. Reuse trades additional retained buffers for less
work on subsequent transactions. Commit or rollback releases the transaction's
remaining pins, so long-lived transactions can keep old table images alive.
See [transaction snapshots](transaction-snapshot-performance.md) and
[transaction buffer reuse](transaction-twin-performance.md) for the invariants
and focused benchmarks.

WAL replay clears row-header slots after DELETE compacts a table. RBAC
revocation clears discarded membership/grant slots after capturing a pending
rollback image. Buffer-pool removal and eviction also remove empty tenant
maps. These release references without waiting for their containers to die.

## Result ownership

The ordinary `Row`/`ResultSet` API returns materialized row maps. Those maps
belong to the caller and are not recycled. Qualified result keys such as
`table.column` are part of the existing API. Removing them or pooling escaped
results would require an explicit compatibility and ownership contract.

`ExecuteColumnar` and `ExecSQLColumnar` return caller-owned
`Values[column][row]` slices and `RowCount`. Simple unordered scans project
directly into these slices; more complex queries use the ordinary executor
and transpose its final output. Cells remain `any`, and nested values follow
the row API's ownership rules. See [columnar execution](columnar-execution.md).

`ExecuteStream` bounds the producer/consumer queue: its default is 64 rows,
and `ExecuteStreamWithOptions` accepts an explicit capacity, including zero
for strict backpressure. Simple scans produce rows directly; plans requiring
materialization still allocate their intermediate result. Close an abandoned
stream promptly so its producer can stop and release its snapshot.

A stream row is valid until the next `Next` call. `ReleaseRow` explicitly
ends the caller's use of the current map and makes it available for reuse;
the caller must not access it afterward. The SQL driver copies values before
releasing the map. Prepared streamed executions retain their borrowed AST
until production stops. Closing driver rows waits for that stop, clears its
stored callback reference, and invokes the resource-release callback once.

## Reusable scratch and caches

Pooling is restricted to temporary objects with exclusive ownership. Release
paths clear pointer-bearing state or detach its source/target before reuse.
For example, disk readers and writers detach their I/O objects; expression
scratch clears literal values; prepared statement slots restore their
parameter markers. HTML output buffers larger than 64 KiB are discarded.
Returned values must never alias scratch that another operation can reuse.

| Scratch | Retention rule on release |
|---|---|
| Stream row maps | Clear keys and values; retain maps with at most 256 keys, counting qualified keys. |
| HNSW candidate/result/selection arrays | Retain at most 8 MiB of combined backing capacity per scratch instance. |
| HNSW visited marks | Retain at most 16 MiB of `uint32` backing capacity per instance. |
| Routing arrays and heap | Retain at most 64 MiB of combined backing capacity, in either the hot slot or pool. |

Oversized numeric buffers are detached while their small wrapper remains
reusable. Generation marks avoid clearing every numeric element on ordinary
reuse; epoch wrap clears the full mark capacity. The map key threshold is a
width-based policy, not a precise byte limit on arbitrary caller-supplied maps.

Scratch limits apply to each released instance, rather than to the aggregate
memory of a pool. Concurrent operations can borrow several instances.
[`sync.Pool`](https://pkg.go.dev/sync#Pool) may discard entries at any time;
correctness and resource cleanup must not depend on a later `Get` returning
the same object. Repeated oversized operations may allocate again when their
buffers exceed the retention budget.

Entry-count limits on parsed-query or search caches are separate from scratch
budgets. They do not imply a byte bound: one cached table or search index can
be much larger than another. Profile the live table/index graph as well as
per-query allocations when evaluating a RAG workload.

The full vector-query analytics window reuses its event slice, and pruning
clears expired tail references. With a 128-event window, the focused benchmark
changed from 16,384 B/op and 1 alloc/op to 0 B/op and 0 alloc/op in three
10,000-operation runs (Go 1.27.3, Apple M2 Max, `GOMAXPROCS=2`; baseline
`d504ec0` with the same benchmark fixture). This measures window maintenance,
not the full cost of vector search.

The four focused warm-reuse benchmarks below retained 0 B/op and 0 allocs/op
before and after these limits in the same three-run comparison. Larger
operations remain supported; buffers above the limits are allocated again
on subsequent requests rather than kept idle in the pools.

## Profiling a change

Record the Go version, dataset and result shape, concurrency, `GOMAXPROCS`,
`GOGC`, and `GOMEMLIMIT`. Compare the same workload in fresh processes before
and after the change. Include both a warm steady-state run and a large request
followed by ordinary small requests when evaluating a pool budget.

Use allocation benchmarks to find churn. The stream benchmark consumes rows
without calling `ReleaseRow`; `BenchmarkPooledProjectionReuse` measures the
projection/release cycle directly. The scratch benchmarks isolate warm reuse.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkExecuteStreamAllRows$' -benchmem -count=5
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecSearchCosineTopK_HNSWCached$' -benchmem -count=5
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(PooledProjectionReuse|HNSWScratchReuse|VisitedScratchReuse|RouteScratchReuse)$' \
  -benchmem -count=5
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVectorAnalyticsFullWindow$' \
  -benchtime=10000x -count=3 -benchmem
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkExecuteStreamAllRows$' \
  -memprofile=/tmp/tinysql-memory.prof
go tool pprof -top -sample_index=alloc_space /tmp/tinysql-memory.prof
go tool pprof -top -sample_index=inuse_space /tmp/tinysql-memory.prof
```

`alloc_space` reports accumulated allocation volume; `inuse_space` reports
sampled retained allocations at the profile snapshot. A test memory profile
is collected at the end of the run, so it does not show peak heap or peak RSS.
The profile's collection can also change disposable pool contents. Use a
running workload's heap profile and metrics to study retention over time.

For a host application's measurement harness, sample `runtime/metrics` around
a fixed amount of work and let GC cycles occur naturally:

| Metric | Interpretation |
|---|---|
| `/gc/heap/allocs:bytes` | Cumulative allocated bytes; compare deltas per operation. |
| `/gc/heap/allocs:objects` | Cumulative heap allocations; compare deltas per operation. |
| `/gc/heap/live:bytes` | Live heap marked by the previous collection. |
| `/gc/scan/heap:bytes` | Heap space that is scannable, not bytes scanned in the last cycle. |
| `/cpu/classes/gc/mark/assist:cpu-seconds` | Estimated cumulative GC-assist CPU; compare deltas with other CPU-class metrics. |

Keep intentionally retained results and fixtures alive through the observation.
Sample across several natural cycles rather than asserting a heap reduction
after a fixed delay. Pointer-free HNSW and routing arrays primarily affect
retained bytes; their size alone does not demonstrate a scan-byte reduction.

The existing footprint probes are useful for separate, controlled experiments:

```sh
TINYSQL_COLUMNAR_MEMORY=maps go test ./internal/engine \
  -run '^TestColumnarMemory$' -count=1 -v
TINYSQL_COLUMNAR_MEMORY=columns go test ./internal/engine \
  -run '^TestColumnarMemory$' -count=1 -v
go test ./internal/engine -run '^TestRAGCacheFootprint$' -count=1 -v
```

These probes use forced collection to report a particular retained snapshot;
their output is observational, not a GC timing or performance assertion. Test
reset rules and capacity limits directly. Do not test pool behavior by
assuming an entry survives collection or that finalization happens on time.
