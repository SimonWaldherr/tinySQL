# Benchmarks and acceptance gates

Benchmark results are only meaningful when source, artifact, hardware, warmup
state and request corpus are recorded together. tinyTiles keeps two layers:

1. package benchmarks (`make bench`) catch local cache/synchronizer regressions;
2. `tinytiles benchmark` compares an actual `.ttiles` artifact against its
   SQLite MBTiles source and validates byte parity for every sampled lookup.

The CLI supports both flat `tiles` and normalized `map/images` source layouts.

## Fixture tiers

| Tier | Purpose | Checked into source control? |
|---|---|---|
| Small | deterministic unit/integration fixtures, malformed input and race tests | yes |
| Regional | representative zooms, payload sizes and spatial clusters | only if licensing/size allow; otherwise documented local fixture |
| Full DACH | capacity, artifact size, resource planning and release-canary evidence | no |

Never make CI download or regenerate a multi-gigabyte DACH fixture. It should
run on a dedicated machine after checking free disk and RAM, with the exact PBF
generator/configuration recorded in the `.ttiles` provenance block.

## Local quality benchmarks

```bash
make bench
make coverage
```

The package benchmarks report allocations and cover in-memory cache reads,
native FileStore reads and a bounded range synchronization. They are regression
signals rather than absolute deployment numbers: filesystem cache, CPU governor
and browser quota policies affect results.

## SQLite versus tinyTiles fixture run

First build and validate a side-by-side artifact:

```bash
./dist/tinytiles import \
  --batch 64 --max-memory $((64 * 1024 * 1024)) \
  --min-free $((8 * 1024 * 1024 * 1024)) \
  dach.mbtiles dach.ttiles/
./dist/tinytiles validate dach.ttiles/
```

Then use deterministic warm point requests:

```bash
make bench-fixture \
  MBTILES=/data/dach.mbtiles \
  ARTIFACT=/data/dach.ttiles \
  REQUESTS=4096 \
  MEMORY=$((16 * 1024 * 1024)) \
  READERS=8
```

The output is tab-separated and includes strict open time, byte size, sampled
binary parity and p50/p95/p99 for one, four and eight readers plus a 2x2
spatial workload:

```text
resource      SQLite    tinyTiles
open          ...       ...
bytes         ...       ...
sample-parity ...       PASS
full-parity   ...       PASS
workload  backend   readers   p50   p95   p99
point     SQLite    1         ...   ...   ...
point     tinyTiles 1         ...   ...   ...
point     SQLite    4         ...   ...   ...
point     tinyTiles 4         ...   ...   ...
point     SQLite    8         ...   ...   ...
point     tinyTiles 8         ...   ...   ...
spatial-2x2 SQLite  1         ...   ...   ...
spatial-2x2 tinyTiles 1       ...   ...   ...
gate      p95 <= 2x SQLite    PASS
```

The command warms each backend, uses the same deterministic corpus and
retrieves full tile BLOBs for both point and spatial measurements. In addition
to sample byte equality, it streams every ordered source key/BLOB and metadata
row, rejects duplicates and compares the resulting digests and row counts with
the independently validated artifact. It exits non-zero when full parity fails
or single-reader tinyTiles p95 exceeds twice SQLite p95. Re-run at least five
times on an idle machine and retain all raw outputs instead of selecting the
fastest run.

The latest checked local DACH record is
[benchmark-results-2026-08-05.md](benchmark-results-2026-08-05.md). It is a
capacity/canary record, not a promise that another filesystem or CPU will
produce identical timings.

## Full evaluation record

For a regional or DACH decision, collect:

- tinyTiles/tinySQL version, Go version, OS, CPU model, RAM and filesystem;
- source MBTiles size, SHA-256, schema, tile/image/metadata counts;
- PBF generator version and supplied flags when built from PBF;
- importer batch, memory, free-disk reserve, elapsed time, peak RSS and final
  source/artifact byte size;
- cold/open latency and warm random plus spatially adjacent p50/p95/p99;
- 1, 4 and 8 independent reader results through three open/close cycles;
- complete tile and relevant metadata parity; missing/duplicate-key audit;
- race-test result and full artifact validation after three process restarts.

The repository's small fixtures exercise the mechanics. A full-DACH canary is
only ready when the record above passes on the actual fixture. Do not infer
capacity or p95 from a small regional result.

## Interpreting a failure

- **p95 gate fails:** preserve the raw result, request sequence and cache
  budget; investigate index/page cache behavior before increasing the gate.
- **parity fails:** treat the artifact as invalid. Compare source schema,
  TMS coordinates, tile digest and import logs before serving it.
- **resource gate fails:** increase actual available resources or reduce the
  planned batch/cache configuration; do not bypass the preflight check.
- **WASM sync differs:** verify CORS exposes `X-TinyTiles-SHA256` and
  `X-TinyTiles-Content-Encoding`, and ensure the endpoint is not applying
  HTTP `Content-Encoding` to raw tile bytes.
