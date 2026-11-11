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
| HNSW visited state | Dense `uint32` epochs through 4,194,304 rows, then a bitmap and touched-word list; retain at most 16 MiB combined capacity. |
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

## Large datasets

Dense visited-array growth stops at the retention budget. Larger HNSW corpora
use one bit per row and record which bitmap words a traversal changes. Reset
clears those words, so a sparse search does not clear the entire corpus-sized
bitmap. For ten million rows the initial bitmap has about 1.88 MB of capacity
(including growth room), plus its touched-word list. It is reusable across
searches instead of allocating a new approximately 60 MB epoch array each
time. Segment compaction also clears discarded headers so old packed vectors
and norm buffers do not stay reachable through slice capacity.

Fresh checkpoint, disk-table, and replication decoders transfer ownership of
secondary indexes, full-text postings, and statistics to the restored table.
The restored ANN graph adopts its outer headers and levels, but packs inner
layers and links into two arenas. Adopting every decoder allocation directly
would retain an extra heap object per row/layer. An opt-in 100,000-row probe
observed about 700,000 live objects with compact topology versus 850,000 with
uncompacted adoption; the final loader preserves the compact footprint. WAL
records that remain caller-owned still use the independent copying path.
Legacy nil/empty index shapes and writable omitted maps remain compatible.

Paged caches reserve bookkeeping for at most the default 1,024 pages at open,
then grow toward the configured cache limit as pages are used. The limit and
LRU policy are unchanged. Row encoding reserves each vector payload once and
preserves the existing binary format, including IEEE-754 payload bits.

Artifact imports reuse exclusively owned row cells across synchronous paged
writes and clear them after each successful full or partial batch. Tile data
borrowed from a source callback is still copied. This reuse does not apply to
generic importers whose in-memory fallback retains caller row slices. Physical
page GC uses the existing free set and marks persistent secondary-index trees,
including their overflow pages, before reclaiming unreachable pages.

The following are allocation measurements, not peak RSS. Each workload ran
three times against `e44dc47` with the same fixtures, in separate before/after
test processes (Go 1.27.3, Apple M2 Max, `GOMAXPROCS=2`):

| Workload | Before B/op | After B/op | Before → after allocs/op |
|---|---:|---:|---:|
| Warm visited set, 3 million rows, 1,000 visited nodes | 18,006,280 | 0 | 3 → 0 |
| Warm visited set, 10 million rows, 1,000 visited nodes | 60,006,664 | 0 | 3 → 0 |
| Empty page-cache bookkeeping, 131,072-page limit | 4,729,088 | 36,944 | 513 → 5 |
| Fresh row encoding, 768-dimensional vector | 24,800 | 6,560 | 12 → 2 |
| Fresh row encoding, 1,536-dimensional vector | 62,944 | 13,600 | 15 → 2 |
| Snapshot load, 10,000 rows with secondary/FTS/ANN indexes | 10,036,112 | 8,449,136 | 251,807 → 231,776 |

Warm visited-set figures exclude first allocation and assume the disposable
pool entry survives; they measure traversal bookkeeping, not an entire ANN
query. Actual cached HNSW controls at k=20/100/200 retained their allocation
counts and comparable timings. Reused encoding buffers remain allocation-free.
The complete 1,025-tile artifact workload saved approximately 1,000 allocations
per import; its file I/O and validation dominate total allocations and timing.

Reproduce the focused workloads with fixed operation counts:

```sh
GOMAXPROCS=2 go test ./internal/engine -run '^$' \
  -bench '^BenchmarkVecVisitedLargeCorpus$' -benchtime=20x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/storage/pager -run '^$' \
  -bench '^Benchmark(PageBufferPoolStartup|MarshalRowEmbeddingBuffers)$' \
  -benchtime=1000x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/storage -run '^$' \
  -bench '^BenchmarkIndexedSnapshotLoadReader$' -benchtime=10x -count=3 -benchmem
GOMAXPROCS=2 go test ./internal/importer -run '^$' \
  -bench '^BenchmarkImportTileArtifactBatch$' -benchtime=3x -count=3 -benchmem
TINYSQL_INDEXED_LOAD_MEMORY=1 go test ./internal/storage \
  -run '^TestIndexedSnapshotLoadFootprint$' -count=1 -v
```

The last command deliberately collects a retained snapshot for observation;
it does not assert GC timing, object counts, or a memory threshold.

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
