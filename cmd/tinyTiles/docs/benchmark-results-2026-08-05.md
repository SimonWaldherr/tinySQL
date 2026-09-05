# Bayern and DACH benchmark — 2026-08-05

## Decision

**Ready for DACH canary.** The freshly built DACH artifact passed complete
key/BLOB/metadata digest validation, unique-key and physical-index validation,
three process restarts, and the required point-lookup p95 gate for 1, 4 and 8
readers. This is approval for a controlled canary, not a claim that tinyTiles
is universally faster or smaller than SQLite.

## Environment and method

| Item | Value |
|---|---|
| Host | Apple M2 Max, macOS arm64 |
| Go | 1.26.5 |
| SQLite CLI | 3.45.2 |
| Reader cache | 16 MiB per reader |
| Reader counts | 1, 4, 8 |
| Import cache / batch | 64 MiB / 64 rows |
| Coordinate system | TMS |

The benchmark uses deterministic samples distributed across available zoom
levels. Both backends are warmed and return complete tile BLOBs. Point parity
uses byte equality. The importer hashes every ordered `(z,x,y,tile_data)` and
relevant metadata value; opening the artifact recomputes those digests while
also validating row counts, uniqueness, physical indexes and checksums.

Fixtures:

| Fixture | Source schema | Tiles | Metadata | SQLite bytes | tinyTiles bytes | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| Bayern | flat | 68,372 | 12 | 1,021,419,520 | 1,253,132,371 | 1.227x |
| DACH | normalized source, flat artifact | 291,811 | 18 | 4,750,925,824 | 5,629,782,097 | 1.185x |

The DACH source contains 284,343 image rows referenced by 291,811 map rows.
The flat artifact intentionally resolves this source-side deduplication during
import.

## Final DACH reader result

The measured corpus contained 2,426 point requests. Durations are warm
per-request latency; strict tinyTiles open includes checksum and complete
logical validation of the 5.63 GB artifact plus creation of all eight readers.

| Workload | Backend | Readers | p50 | p95 | p99 | tinyTiles / SQLite p95 |
|---|---|---:|---:|---:|---:|---:|
| Point | SQLite | 1 | 40.500 us | 115.833 us | 193.542 us | — |
| Point | tinyTiles | 1 | 18.792 us | 88.375 us | 144.125 us | 0.763x |
| Point | SQLite | 4 | 97.209 us | 301.084 us | 509.666 us | — |
| Point | tinyTiles | 4 | 32.417 us | 205.959 us | 542.375 us | 0.684x |
| Point | SQLite | 8 | 178.334 us | 522.625 us | 869.500 us | — |
| Point | tinyTiles | 8 | 37.417 us | 237.666 us | 484.958 us | 0.455x |
| Spatial 2x2 | SQLite | 1 | 172.500 us | 415.083 us | 585.250 us | — |
| Spatial 2x2 | tinyTiles | 1 | 81.208 us | 317.875 us | 469.833 us | 0.766x |

| Resource | SQLite | tinyTiles |
|---|---:|---:|
| Strict open | 2.132 ms | 6.899 s |
| Benchmark process peak RSS | included in process | 299,941,888 bytes total |
| Sampled binary parity | 2,426 / 2,426 | PASS |
| Full binary/key/metadata parity | 291,811 tiles / 18 metadata | PASS |
| Required single-reader p95 <= 2x SQLite | — | PASS |

tinyTiles now has lower p50 and p95 in every measured DACH workload and lower
p99 except at four readers, where it is 6.4% slower. SQLite remains much better
at cold open, artifact size and import tooling, so tail latency and process
startup remain canary metrics.

## Import and memory

| Operation | Time | Peak RSS | Result |
|---|---:|---:|---|
| Bayern tinyTiles import, batch 64 / 64 MiB | 36.05 s | 222,969,856 B | PASS |
| DACH tinyTiles import, batch 64 / 64 MiB | 192.42 s | 224,460,800 B | PASS |
| DACH strict validation only, 32 MiB cache | 7.39 s | 77,463,552 B | PASS |
| Bayern SQLite `VACUUM INTO` | 2.48 s | 8,110,080 B | PASS |

`VACUUM INTO` is a useful SQLite publication lower bound, not an equivalent
import: it keeps the existing SQLite representation and does not construct a
different durable index format or recompute tinyTiles' key/BLOB/metadata
digests. A source `.mbtiles` needs no conversion at all when SQLite remains the
runtime.

The importer printed its estimates before writing. A deliberately attempted
Bayern run with batch 1000 and a 128 MiB limit was rejected before publication
because its conservative worst-case batch required 498,894,152 bytes. Batch 64
reduced that estimate to 33,892,160 bytes and completed. The failed target was
never published.

The validation cache reduction plus the owned-scan and value-free index checks
lowered isolated DACH validation peak RSS from 165,052,416 to 77,463,552 bytes
(53.1%) and runtime to 7.39 seconds. The import phase boundary now releases the
mutable writer heap before checksum/validation buffers are allocated. tinyTiles'
package default is 16 MiB per reader and at most eight default readers, capping
default aggregate page caches at 128 MiB.

## Lookup optimization

The unique-index hot path now resolves prevalidated index roots directly,
decodes a single 12-byte row locator and materializes only `tile_data` (or the
normalized `tile_id` followed by `tile_data`). It no longer creates generic
row-ID/result slices or decodes unused columns.

| Microbenchmark | Before | After |
|---|---:|---:|
| Public reader | 535–885 ns/op | 247–250 ns/op |
| Bytes/op | 278 | 32 |
| Allocations/op | 12 | 2 |
| Selected-column decoder | — | 35.96–37.54 ns/op, 32 B, 2 allocs |

Small spatial windows use exact unique seeks; larger windows use one tightly
bounded y interval per x. This avoids scanning every y value in selected x
columns. On Bayern, the corrected full-BLOB spatial p95 was 244.5 us versus
203.9 us for SQLite.

The 2026-08-06 tinySQL core pass removed a second payload-sized copy for
overflow-backed rows. `BTree.Get` now returns assembled overflow memory
directly, typed decoders borrow BLOB columns only when that row is already
caller-owned, and inline page values retain defensive copy semantics. On the
regional Bayern fixture this changed the direct tinySQL benchmark as follows:

| Bayern direct reader | Before | After |
|---|---:|---:|
| Point p50 | about 33 us | about 22 us |
| Point p95 | about 64 us | about 40 us |
| Point bytes/op | 147,914 | 74,087 |
| Spatial p50 | about 92 us | about 62 us |
| Spatial bytes/op | 519,039 | 260,098 |

Strict validation uses a new value-free unique-index reachability lookup and
combines row counting with the existing digest pass. It therefore no longer
loads each Tile BLOB a second time merely to prove that its index locator
references an existing row.

## Durability, parity and tests

- Full DACH tile/key digest: `1a4e1be50cb67c9f4bb6a194c24cb87f8addc569095ae4c029834dd882bc1603`.
- Full DACH metadata digest: `6c03da7c1cfb2f5e106f1ec4fb91b5e5f0b54b430cdc85688763401430b4cdd6`.
- Three fresh process validations completed in 7.389 s, 7.546 s and 7.448 s.
- Full tinySQL tests with `sqliteimport`: PASS.
- Race tests for storage/pager/importer/public tiles and all tinyTiles
  packages: PASS.
- tinyTiles statement coverage: 65.7% overall (library 57.3%, CLI 71.4%,
  offline 74.9%, server 69.9%; binary `main` packages reduce the total).

## Remaining canary watch items

- Artifact size is 18.5% above SQLite for DACH. The current 8 KiB pager stores
  every large BLOB in its own overflow chain, leaving tail slack in the final
  page. A packed immutable BLOB arena is the likely structural fix; changing
  page size alone does not solve it.
- Strict cold open is intentionally expensive because it validates every
  checksummed byte and logical digest. Production rollout should validate once
  before pointer swap, then keep the reader process warm.
- Monitor eight-reader p99, page-cache misses, RSS and open failures during the
  canary. Do not change Karte.Bayern production configuration until those
  observations are accepted.
