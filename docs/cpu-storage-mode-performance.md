# GOMAXPROCS and storage-mode performance

Measurements: 2026-09-08, Go 1.27.1, Apple M2 Max (12 hardware CPU cores),
darwin/arm64. `-cpu=1,36` exercises both requested scheduler settings. Setting
GOMAXPROCS to 36 on this host tests oversubscription and contention, not scaling
on a machine with 36 physical cores. No worker-count heuristic is tuned to this
host; existing FTS/vector worker limits continue to respect GOMAXPROCS and workload
size, including their serial paths at GOMAXPROCS=1.

## Changes

- Disk table operations borrow fixed 64 KiB buffered readers/writers from
  `sync.Pool`. Decoders own their result values; no table value references a
  returned I/O buffer.
- Gzip writers reuse their compression state, including the encoding step before
  encryption. Failed writes are reset before reuse. Returned writers detach the
  file/encryption-buffer target. These are disposable GC-managed pools, not
  permanently reserved per-worker buffers; reduced allocation volume is not a
  promise of proportionally lower peak RSS.
- Hybrid/Index LRU hits skip list mutation when the table is already most recent.
  The unused private LRU timestamp is removed; CachedTable.LastAccess and exact
  LRU order remain maintained. No sampling or approximate eviction is introduced.

The disk changes also serve Hybrid/Index cache misses and saves, and JSON table
files. They leave encoding formats, flush/error handling, file fsync, atomic rename
and directory fsync intact. Memory, WAL and paged storage do not gain a new codec
or persistence policy from this patch.

## Backend measurements

Backend fixtures have 1,000 rows. Baseline runs use 150 ms; concurrent after-runs
use two 200 ms repetitions. These short samples illustrate allocation and cache
costs; file-write times vary with filesystem sync latency.

| Workload | GOMAXPROCS | Before ns/op | After ns/op |
| --- | ---: | ---: | ---: |
| Concurrent Hybrid cache hits | 1 | 120 | 93.5–94.3 |
| Concurrent Hybrid cache hits | 36 | 317 | 252–257 |
| Concurrent Index cache hits | 1 | 121 | 87.7–91.8 |
| Concurrent Index cache hits | 36 | 362 | 251–256 |
| Concurrent Disk reads | 1 | 793,444 | 758,560–765,004 |
| Concurrent Disk reads | 36 | 137,158 | 118,595–131,298 |

Parallel ns/op is aggregate throughput cost, not one request's latency. The hot
cache benchmarks allocate zero bytes before and after. Contention still makes
these tiny cache operations slower at 36 than at 1; this change reduces that cost
without claiming linear scaling.

For repeated compressed disk saves, allocated bytes/op fall from about 1.44 MB
to 0.294 MB at GOMAXPROCS=1 and 0.385 MB at 36 in the measured runs. Pool misses and
GC affect those totals. Uncompressed reads save roughly one 64 KiB allocation per
operation; concurrent reads at 36 measured about 405 KB/op instead of 465 KB/op.
Durable writes remain around 11–13 ms here: no proportional latency improvement
is claimed from the large allocation reduction.

Disk read benchmarks reopen/decode files, but do not purge the operating system's
page cache. Hybrid/Index reads are warmed by fixture saves. They are not cold-disk
measurements and should not be interpreted as such.

## SQL and reopen matrix

`BenchmarkStorageModeCPU` exercises one parsed, filtered LIMIT query over 128
rows, with warm database/table caches. Both sequential and concurrent readers
validate the returned row count and first ID. This guards mode compatibility;
Disk/JSON may serve already-loaded table leases, so it does not measure physical
file reads on every query.

| Mode | Sequential ns/op, 1 / 36 | Parallel ns/op, 1 / 36 |
| --- | ---: | ---: |
| Memory | 1,672 / 1,493 | 1,661 / 1,463 |
| Disk | 1,598 / 1,528 | 1,727 / 1,445 |
| Hybrid | 1,644 / 1,741 | 1,832 / 1,485 |
| Index | 1,587 / 1,552 | 1,751 / 1,337 |
| JSON | 1,569 / 1,564 | 1,780 / 1,499 |
| Paged Index | 1,966 / 1,913 | 2,133 / 1,836 |

These are after-change measurements, not before/after speedup claims.
`TestStorageModesCPUReopen` checks persisted rows after close/reopen for all six
modes at both scheduler settings. WAL recovery and optional SQLite storage have
separate tests and are not covered by this matrix.

## Reproduction

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkStorageModeCPU$' -benchmem -benchtime=150ms -cpu=1,36
go test ./internal/storage -run '^$' -bench '^BenchmarkConcurrentLoad$' -benchmem -benchtime=200ms -count=2 -cpu=1,36
go test ./internal/storage -run '^$' -bench '^(BenchmarkLoadTable|BenchmarkSaveTable)$/^(Disk|DiskGzip|Hybrid|Index)$/^rows=1000$' -benchmem -benchtime=150ms -cpu=1,36
go test ./internal/engine ./internal/storage ./internal/driver ./driver -cpu=1,36 -count=1
go test -race ./internal/storage -run 'Test.*(BufferPool|LRU|Disk|Hybrid|Encrypt)' -cpu=1,36 -count=1
```

Additional regression coverage checks GOB/JSON result ownership after buffer
reuse, compressed writer recovery after an I/O failure, gzip CRC/trailer integrity,
and concurrent buffer-pool updates and accesses.

The FTS scan equivalence test now checks the serial worker count at
GOMAXPROCS=1 and a bounded parallel worker count above 1, instead of rejecting
the correct single-worker setting. Its comparison with the serial reference is
retained at both settings.
