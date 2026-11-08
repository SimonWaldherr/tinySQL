# Database loading and replica lifecycle

Baseline: `20d4b65`. This work reduces restore allocations, adds streamed replica
bootstrap and drains replication before closing the database. Existing snapshot
files, unary Bootstrap, WAL recovery and the public Go API remain compatible.

## Research applied

| Primary source | Application in tinySQL |
| --- | --- |
| [gRPC flow control](https://grpc.io/docs/guides/flow-control/) lets synchronous streaming sends follow transport backpressure. | BootstrapStream sends bounded byte chunks directly from a batched serializer. The receiver decodes them without collecting an entire encoded database in a byte slice. |
| [gRPC graceful shutdown](https://grpc.io/docs/guides/server-graceful-stop/) separates draining from a forceful stop deadline. | Withdraw readiness permanently, cancel open-ended replication, drain HTTP/gRPC concurrently and close storage after database leases finish. A deadline forces transports closed and reports an error; persistence follows. |
| [PostgreSQL standby operation](https://www.postgresql.org/docs/current/warm-standby.html) combines an initial base image with subsequent WAL and requires reinitialization when needed history is unavailable. | Capture rows, catalog, epoch and resume watermark together. Preserve the existing committed-only feed, checkpoint-boundary detection and re-bootstrap behavior. |
| [Go diagnostics](https://go.dev/doc/diagnostics) distinguishes allocation/heap profiling from other process measurements. | Report allocated bytes separately from maximum RSS, and measure the latter in fresh processes rather than inferring it from benchmark allocation totals. |

## Loading and memory ownership

Fresh GOB/JSON decoders now transfer their row slices into restored tables.
Schema normalization still restores JSON and vectors, clears cells beyond the
schema and preserves nil/empty-row behavior. Slice capacities are clipped to
their lengths. Borrowed WAL records still use the existing copying adapter.
The change applies to reader/byte/file restores, OpenDB checkpoints and disk/JSON
table loads. It removes a redundant row-header/cell copy, not the decoded data
the database actually needs to retain.

## Bootstrap and shutdown

The new replication-only format encodes table metadata and 256-row batches.
The primary captures a detached image under content/WAL locks in checkpoint
lock order, including private mutable cell/default values. Encoding and network
output happen after those locks are released. Capturing checks cancellation
between rows; encoded rows are released progressively. One bootstrap per server
is admitted at a time, with cancellation for queued requests.

Network chunks are at most 64 KiB and shrink to fit sender/receiver limits.
The receiver allocates final table headers after the announced rows have really
arrived. A truncated stream with a huge declared row count cannot cause an
immediate allocation based on that count alone. Inner format completion, a final
message containing epoch/watermark and successful RPC EOF are all required before
publishing a database. New replicas fall back to unary bootstrap only when an
older server reports Unimplemented before sending any snapshot data.

The primary's detached image and the replica's complete database still require
memory. Individual rows, catalog or index metadata can be larger than a row
batch; this is not a constant-memory database engine. WAL batches remain unary
messages and need appropriate message limits. Existing GOB snapshot files keep
their old encoding; no on-disk migration is required.

SIGTERM/SIGINT withdraws readiness even if a late replica callback tries to mark
the node ready again. Open-ended feeds terminate during drain, ordinary requests
get their configured grace period, and database leases protect active readers
until the final close. Query caches are cleared at that close. HTTP and gRPC
drain together. Final flush/checkpoint I/O can exceed the request-drain timeout;
container grace periods must allow that storage work.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Medians of three serialized
300 ms samples over 50,000 two-column scalar rows. The replication round-trip
includes capture, encoding and restore through an in-process pipe; it excludes
gRPC, network, parsing and durable I/O.

| Fixture | Before | After | Allocated bytes/op before → after |
| --- | ---: | ---: | ---: |
| Restore existing GOB through LoadFromReader | 20.20 ms | 20.37 ms | 18,073,619 → 15,269,403 |
| Legacy snapshot round-trip → batched round-trip | 42.87 ms | 26.44 ms | 47,182,153 → 19,489,392 |

Restore timing ranges overlap; no startup-latency gain is established by that
fixture. Its allocated bytes drop about 16% and allocation count falls from
451,673 to 401,672. The round-trip reduces allocated bytes about 59%. Batching
adds some small objects compared with the legacy path using the already optimized
loader; the benefit is chiefly fewer large transient buffers.

Maximum RSS was measured with `/usr/bin/time -l` in three fresh benchmark-binary
processes per format, one round-trip per process. Both use the final optimized
loader, isolating the format comparison: legacy median **37.83 MiB** (37.80–40.91),
batched median **32.44 MiB** (32.38–32.59). These are whole-process peaks for this
small fixture, not a production server memory budget or a dataset-independent
reduction.

```sh
go test ./internal/storage -run '^$' \
  -bench '^Benchmark(SnapshotLoadReader|LegacyReplicationSnapshot|StreamedReplicationSnapshot)$' \
  -benchmem -benchtime=300ms -count=3
go test -c -o /tmp/tinysql-storage-memory.test ./internal/storage
/usr/bin/time -l /tmp/tinysql-storage-memory.test -test.run='^$' \
  -test.bench='^BenchmarkStreamedReplicationSnapshot$' -test.benchtime=1x
go test ./...
(cd cmd/server && go test -race ./...)
bash scripts/check-wasm.sh
```

Coverage includes legacy restores, multi-tenant/catalog and typed-value parity,
concurrent mutation/commit versus capture, mutable value isolation, lock release
before slow output, truncated/trailing data, cancellation, old-primary fallback,
authentication and a snapshot over 6 MB with an 8 KiB receive-message limit.
Shutdown tests cover active replication, irreversible readiness withdrawal,
admission cancellation, database leases and reopening committed data.

A separate native process smoke uses one primary, two replicas and a test HTTP
gateway. It verifies replica removal/rejoin and primary restart with preserved
data. [The deployment guide](cluster.md) supplies the Compose/HAProxy setup and
smoke command. Compose configuration validates locally; its containers and
HAProxy could not be executed here because the Docker daemon was unavailable.
The added cluster CI workflow runs that deployment and replica restart check.
