# Geospatial execution notes

This page records the query-path optimizations behind GIS operations. It does
not change the standards or coordinate-system contract in the
[geospatial standards guide](geospatial-standards.md).

## Geometry predicates

Planar line/line, line/polygon, and polygon-boundary tests reject disjoint
axis-aligned bounds before comparing every segment pair. Segment tests make the
same cheap rejection before their orientation calculation. Touching bounds still
match; exact predicates remain authoritative; NaN/Inf bounds disable the
shortcut. Containment and polygon-hole behavior is unchanged.

Disjoint paths take O(n+m) bound work; overlapping bounds can still require
O(n×m) exact segment tests. No geodesic, antimeridian, or CRS semantics changed.
On Apple M2 Max with Go 1.27.1, two disjoint 1,000-segment ring boundaries
changed from 4.56–4.64 ms to about 2.35 µs, with zero allocations in both cases.
This is a predicate microbenchmark, not a spatial-SQL latency claim.

## Spatial candidate windows

`GEO_SEARCH` selects direct grid-cell lookups for small bounding-box/radius
windows and scans occupied cells for broad sparse windows. Spatial RAG
pre-filters use the same path. Overflow geometries, deduplication, exact
residual predicates, and final row ordering are retained. Built indexes scan
their sorted occupied columns once a window covers more than 16 cells; the
sparse-map fallback uses the occupied-cell ratio only when those columns are
unavailable.

In a synthetic 128×128 grid with 128 occupied diagonal cells and one overflow
row, broad-window candidate selection changed from 105–107 µs to about 1.38 µs;
a small window remained about 64 ns. These measurements exclude index building,
exact distance checks, and SQL result materialization.

For wider warm-query measurements and their limits, see
[retrieval performance](retrieval-performance.md). Reproduce the focused checks
with:

```sh
go test ./internal/engine -run '^Test.*(Geo|Spatial)' \
  -bench '^(BenchmarkDisjointRingBoundaries|BenchmarkSparseGridCandidates)$' -benchmem
```

Randomized differential tests compare optimized predicates with the original
orientation test; existing GIS relation tests cover crossings, containment,
holes, boundaries, and spatial-search ordering.

## GeoJSON decoding and encoding

Compact Point, LineString, Polygon and MultiPolygon inputs decode directly to
geometry values. Other ordinary JSON objects use a small generic decoder.
Escaped strings, non-ASCII input and unsupported forms retain the original
`encoding/json` path. Number parsing and output formatting preserve float64
rounding, negative zero and JSON error behavior. Differential tests, randomized
inputs and `FuzzFastJSONObject` check compatibility with the standard decoder.

Geometry output avoids reflection for coordinates and metadata, including
CRS axes (`[]string`) and GeoPackage version/SRID fields (`byte`/`int32`).
Unsupported values still use `json.Marshal`, including its base64 treatment of
BLOBs and rejection of NaN/Inf.

Local samples on Apple M2 Max, Go 1.27.1, darwin/arm64, GOMAXPROCS=12, three
150 ms runs compared against commit `2ecf4a1` using the same SQL benchmark:

| Full scan of 2,000 rows | Before | After | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| GEO_LON | 2.77–2.89 ms | 0.63–0.67 ms | 38,007 → 6,005 |
| GEO_DISTANCE | 5.34–5.40 ms | 0.93–1.04 ms | ~70,008 → 6,005 |
| GEO_BUFFER (16 segments) | 15.77–15.91 ms | 9.87–10.23 ms | ~166,045 → ~90,007 |

CRS metadata encoding took about 320 ns and one allocation versus 1,030 ns and
13 allocations with `json.Marshal`; GeoPackage metadata took 482–599 ns and one
allocation versus 1,416–1,426 ns and 18 allocations. These are local samples,
not throughput guarantees; repeat on an idle machine for precise comparisons.

```sh
go test ./internal/engine -run '^$' \
  -bench 'BenchmarkGeoSQL/(GEO_LON|GEO_DISTANCE|GEO_BUFFER)$' \
  -benchtime=150ms -count=3 -benchmem
go test ./internal/engine -run '^$' -bench BenchmarkMarshalGeoMetadata \
  -benchtime=200ms -count=3 -benchmem
```
