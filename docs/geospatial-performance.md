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
