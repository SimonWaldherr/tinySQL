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

## Box queries on a composite index

`WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?` on an index over `(lat, lon)`
walks the latitude band, and the longitude bounds are now applied to each entry's
key during that walk (`LookupSecondaryIndexRangeNext`). Only entries the residual
`WHERE` would reject anyway are skipped: an entry whose next component has another
type tag, is NULL or is shorter than a numeric component is always kept, bounds
use the same kind conversion and signed-zero normalization as the range column,
and the residual filter still runs on everything returned. Randomized tests
compare indexed and unindexed answers over NULLs, signed zeros, integers mixed
into float columns and integer or float bound spellings.

With 50,000 points and a 0.05° box (about 250 rows in the latitude band, 1.25
hits) `ViewportIndexed` went from 87 µs to 15 µs per query; SQLite with the same
two-column B-tree measured 46–48 µs in the same runs.

## Constant regions

`ST_WITHIN(pt, <region>)`, `ST_CONTAINS(<region>, pt)`, `ST_INTERSECTS`, `ST_AREA`
and `ST_PERIMETER` receive the region as an ordinary argument and used to decode
its GeoJSON for every row. The last eight large texts (160 bytes to 1 MiB) are
now kept with their parsed polygons and bounding boxes in a small lock-free cache;
a repeated string with the same backing storage is an O(1) hit; equal text
with different storage still requires a byte comparison. Point containment rejects points outside the
bounding box immediately. A second cache of decoded objects (256 bytes to 1 MiB)
with a memoized typed polygon serves the map-based functions (`ST_COVERS`,
`ST_TOUCHES`, `ST_EQUALS`, `ST_CLIP`, WKT). Cached values are shared and
read-only: `TestGeoObjectCacheIsNeverMutated` runs every registered geometry
function against cached values and compares them with a fresh decode. Functions
that edit geometry still decode their own copy.

Two thousand points against one constant region, Apple M2 Max, Go 1.27.1:

| Region vertices | `ST_WITHIN` | `ST_INTERSECTS` | `ST_COVERS` | `ST_TOUCHES` |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 3.7 → 0.68 ms | 5.4 → 0.76 ms | 1.7 → 1.3 ms | 1.7 → 1.2 ms |
| 500 | 78 → 1.1 ms | 97 → 1.4 ms | 10.5 → 3.3 ms | 13.1 → 6.1 ms |
| 5,000 | 766 → 5.3 ms | 954 → 7.4 ms | 92 → 25 ms | 118 → 52 ms |

(`ST_COVERS` and `ST_TOUCHES` are compared with the state after the decode cache
and before the typed memo.) The 5,000-vertex `ST_INTERSECTS` case allocated 1.4 GB
and 40 million objects before and 0.74 MB afterwards.

## Direct paths for common conversions

Geometry text that holds only plain coordinates is processed in one pass without
building maps; anything else (extra members, escapes, non-ASCII, Features,
GeometryCollections, malformed input) takes the previous route, so results and
error messages are unchanged.

- `canonicalGeoJSON` -- every `INSERT` into a `GEOMETRY` column, `GEO_FROM_GEOJSON`,
  `CAST(... AS GEOMETRY)` and `GEO_AS_GEOJSON(geom)` -- copies a number verbatim
  when it is already its shortest round-trip form (at most 15 significant digits,
  no trailing zero, no exponent, not below 1e-6) and otherwise parses and formats it.
- `GEO_BBOX` and `GEO_ENVELOPE` scan the coordinates directly; the envelope text
  is written without a marshal/unmarshal round trip.
- `GEO_AS_WKT` writes WKT straight from the GeoJSON text, including the `Z` tag,
  zero-filled `z`, `EMPTY` and the `g`/`f` number rule.
- `ST_INTERSECTS`/`ST_DISJOINT` decode points, lines and polygons to typed values,
  and run the unchanged pairwise algorithms on them.

Each has a differential test against the previous implementation over random
documents (whitespace variants, exponents, trailing zeros, signed zeros, mixed
`z`, empty groups) plus a fuzz target (`FuzzCanonicalGeoJSON`, `FuzzFastJSONObject`).
Two thousand 17-vertex polygons, per full scan: `GEO_AS_GEOJSON` 28 → 2.8 ms,
`GEO_AS_WKT` 26 → 3.8 ms, `GEO_BBOX` 22 → 3.3 ms, `GEO_ENVELOPE` 27 → 3.5 ms,
`GEO_POLYGON_AREA` 21 → 4.0 ms, `ST_CONTAINS(poly, pt)` 24 → 3.8 ms, `GEO_LENGTH`
(32 vertices) 38 → 6.9 ms.

## Spatial index build

The first pass of the `GEO_SEARCH` grid index (bounding box and centroid of every
row) runs on several goroutines from 4,096 rows. Rows are independent and write
only their own slots, and the union box is merged afterwards, so the index is
identical to the sequential build (tested bit for bit). A cold build over 20,000
polygons went from 33 ms to 11 ms on a 12-core machine; warm queries are unchanged.

```sh
go test ./internal/engine -run '^$' -bench 'BenchmarkGeoSQL|BenchmarkGeoSQLRegion|BenchmarkGeoSearch' -benchmem
go test ./internal/engine -run 'Fast|Canonical|BBox|WKT|GeoObjectCache|GeoPolygon|TypedIntersects|SpatialIndexParallel|RangeIndexNext'
go test ./internal/engine -run '^$' -fuzz FuzzCanonicalGeoJSON -fuzztime 30s
```
