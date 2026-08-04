# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SimonWaldherr/tinySQL.svg)](https://pkg.go.dev/github.com/SimonWaldherr/tinySQL)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)

TinySQL is an embeddable SQL database engine written in Go. It is designed for
learning database internals, local tools, tests, browser/WASM applications, and
single-process services that need a capable SQL layer without operating a
database server.

**Try it:** [browser playground](https://simonwaldherr.github.io/tinySQL/) ·
[interactive map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
· [video](https://youtu.be/W28-aBk3BL0)

> TinySQL is not a drop-in replacement for PostgreSQL, MySQL, or a clustered
> production database. Review the [limitations](#limitations) before using it
> for critical workloads.

## Contents

- [Quick start](#quick-start)
- [What it can do](#what-it-can-do)
- [Using SQL from Go](#using-sql-from-go)
- [GIS and GeoJSON](#gis-and-geojson)
- [Map tiles and MBTiles](#map-tiles-and-mbtiles)
- [Imports, exports, and optional build tags](#imports-exports-and-optional-build-tags)
- [Storage, transactions, and operations](#storage-transactions-and-operations)
- [Guides and development](#guides-and-development)
- [Limitations](#limitations)

## Quick start

Requires Go **1.26.5+**.

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Create an in-memory database, execute SQL, and read rows:

```go
package main

import (
	"context"
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
	ctx := context.Background()
	db := tinysql.NewDB()

	for _, query := range []string{
		`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`,
		`INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')`,
	} {
		stmt, err := tinysql.ParseSQL(query)
		if err != nil {
			panic(err)
		}
		if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
			panic(err)
		}
	}

	stmt, err := tinysql.ParseSQL(`SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		panic(err)
	}
	result, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		panic(err)
	}
	for _, row := range result.Rows {
		id, _ := tinysql.GetVal(row, "id")
		name, _ := tinysql.GetVal(row, "name")
		fmt.Println(id, name)
	}
}
```

For applications that already use `database/sql`, use
[`github.com/SimonWaldherr/tinySQL/driver`](./driver).

## What it can do

### SQL engine

- `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `RETURNING`, CTEs, subqueries,
  joins, grouping, window functions, `PIVOT`, `EXPLAIN`, and common
  SQLite-compatible `PRAGMA`s.
- Views, materialized views, triggers, table-valued functions, stored
  procedures, jobs, multi-tenancy, and system catalog views.
- Constraints including single-column primary keys, unique keys, foreign keys,
  `NOT NULL`, and literal defaults.
- Secondary indexes, exact persisted statistics through `ANALYZE`, and
  planner selectivity estimates.
- JSON, YAML, text, regex, math, date, URL, hash, bitmap, full-text, vector,
  hybrid-search, and RAG helper functions.

### Embedding and delivery

- Pure-Go, in-process API and a `database/sql` driver.
- Memory, WAL, disk, JSON, index, hybrid, and paged-index storage modes.
- Browser/WASM builds and a local-first SQL playground.
- CLI, HTTP server, file-query tools, and health/lifecycle hooks.
- Optional audit logging, RBAC, and encryption at rest for supported table-file
  backends.

### Data and maps

- CSV, TSV, JSON/NDJSON, XML, YAML, Excel, GeoJSON, TopoJSON, KML, OSM XML,
  routing graphs, Shapefiles, and MBTiles import paths.
- GeoJSON measurement, containment, relationship, editing, cleanup, region
  (dissolve/clip), and inspection functions, plus a spatial search index and
  choropleth classification (equal-interval, natural breaks, quantile) for
  location-based BI dashboards.
- GeoJSON and TopoJSON export from query results (`-mode geojson|topojson`),
  the format Power BI Shape Maps and most mapping tools prefer.
- Web Mercator tile addressing, MBTiles import/export, in-place tile access,
  and an optional XYZ tile endpoint.

See [FUNCTIONS.sql](./FUNCTIONS.sql) and
[example_showcase.sql](./example_showcase.sql) for the broader SQL reference.

## Using SQL from Go

`ParseSQL` + `Execute` is the basic API. Use the fluent builder when composing
queries programmatically:

```go
query := tinysql.Select(tinysql.Col("name")).
	From("users").
	Where(tinysql.Eq(tinysql.Col("active"), tinysql.Val(true))).
	OrderBy("name").
	Build()

result, err := tinysql.Execute(ctx, db, "default", query)
```

The builder supports projections, joins, CTEs, ordering, limits, expressions,
and `Exists`/`NotExists` predicates. See
[`ExampleExists`](./example_exists_test.go) for a runnable example.

### Transactions and triggers

The Go driver supports cross-statement transactions with `BEGIN`, `COMMIT`,
and `ROLLBACK` (or `BeginTx`). A transaction sees its own writes; a concurrent
write conflict is returned as a retryable `ErrTransactionConflict`.

Row triggers run for `BEFORE`/`AFTER` `INSERT`, `UPDATE`, and `DELETE`,
including `DELETE` without a `WHERE` clause. Direct multi-row mutations and
their trigger effects are statement-atomic.

```go
import (
	"context"

	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

db, err := tsqldriver.OpenInMemory("default")
if err != nil { panic(err) }
defer db.Close()

tx, err := db.BeginTx(context.Background(), nil)
if err != nil { panic(err) }
if _, err := tx.Exec(`INSERT INTO users VALUES (3, 'Lin')`); err != nil {
	_ = tx.Rollback()
	panic(err)
}
if err := tx.Commit(); err != nil { panic(err) }
```

`SAVEPOINT` and nested transactions are not implemented.

## GIS and GeoJSON

TinySQL stores geometry as ordinary GeoJSON, either in a `TEXT`/`JSON` column
or in the dedicated `GEOMETRY` column type, which validates on write (a bare
number or a `Feature`/`FeatureCollection` is rejected — a `GEOMETRY` column
holds a Geometry) and canonicalizes to stable, byte-identical text. Most
geometry functions return GeoJSON too, so results can be passed directly into
later SQL calls or drawn in a map client.

```sql
CREATE TABLE places (name TEXT, geometry GEOMETRY);
INSERT INTO places VALUES
  ('Berlin', GEO_POINT(13.4050, 52.5200)),
  ('Munich', GEO_POINT(11.5755, 48.1372));

SELECT GEO_DISTANCE(a.geometry, b.geometry) AS meters,
       GEO_BEARING(a.geometry, b.geometry) AS bearing,
       GEO_MIDPOINT(a.geometry, b.geometry) AS midpoint
FROM places a JOIN places b
  ON a.name = 'Berlin' AND b.name = 'Munich';
```

`GEOMETRY`/`GEOM` is additive — existing `TEXT`/`JSON` geometry columns and
the query above keep working unchanged. `GEOMETRY(SRID)`-style parameters are
not supported; use the bare keyword. `CAST(x AS GEOMETRY)` validates and
canonicalizes the same way a column write does.

### Measurement and predicates

| Task | Function | PostGIS-style alias |
| --- | --- | --- |
| Create/read a point | `GEO_POINT`, `GEO_LON`, `GEO_LAT` | `ST_POINT`, `ST_X`, `ST_Y` |
| Great-circle distance | `GEO_DISTANCE` | `ST_DISTANCE`, `HAVERSINE` |
| Radius check | `GEO_DWITHIN` | `ST_DWITHIN` |
| Bounding-box check | `GEO_WITHIN_BBOX` | `ST_WITHIN_BBOX` |
| Bearing / midpoint / destination | `GEO_BEARING`, `GEO_MIDPOINT`, `GEO_DESTINATION` | `ST_AZIMUTH`, `ST_MIDPOINT`, `ST_PROJECT` |
| Point in polygon/multipolygon | `GEO_WITHIN_POLYGON` | `ST_WITHIN`, `ST_CONTAINS` |
| Polygon area / line length | `GEO_POLYGON_AREA`, `GEO_LENGTH` | `ST_AREA`, `ST_LENGTH` |
| Any shared point (point/line/polygon, any combination) | `GEO_INTERSECTS` | `ST_INTERSECTS` |
| No shared point | `GEO_DISJOINT` | `ST_DISJOINT` |
| Same coordinates (order/rotation/winding independent) | `GEO_EQUALS` | `ST_EQUALS` |
| Circular buffer around a point | `GEO_BUFFER(point, meters[, segments])` | `ST_BUFFER` |
| Convex hull of a geometry's vertices | `GEO_CONVEX_HULL` | `ST_CONVEXHULL` |
| Bounding box as a polygon | `GEO_ENVELOPE` | `ST_ENVELOPE` |
| Point at a fraction along a line | `GEO_LINE_INTERPOLATE(line, fraction)` | `ST_LINE_INTERPOLATE_POINT` |
| Clip a geometry to a convex boundary | `GEO_CLIP(geometry, boundary[, allow_nonconvex])` | `ST_CLIP` |

Distance, bearing, destination, length, area, and buffer calculations operate
on the sphere; distances are meters and polygon areas are square meters.
Coordinates in raw four-number forms use `(lat, lon, lat, lon)`; GeoJSON
remains `[lon, lat]`. `GEO_WITHIN_POLYGON`/`ST_CONTAINS`/`GEO_POLYGON_AREA`
accept a GeoJSON `MultiPolygon` as well as a `Polygon` — membership in any
part counts as membership in the whole, and area sums every part.
`GEO_LINE_INTERPOLATE` splits by actual distance along the line, not by
vertex count. `GEO_CONVEX_HULL` computes the hull in plain lon/lat space
(a standard planar approximation, not a rigorous spherical hull).

`GEO_INTERSECTS`/`GEO_DISJOINT` cover point/line/polygon in any combination,
respecting polygon holes (a shape nested inside another polygon's hole is
disjoint from it). There is no `ST_TOUCHES`/`ST_CROSSES`/`ST_OVERLAPS`:
rigorously distinguishing boundary-only contact from interior overlap needs a
full DE-9IM computation, which is out of scope — a naive attempt would be
silently wrong on exactly the shared-edge/hole cases real GIS data hits
constantly. `GEO_EQUALS` is scoped to coordinate/shape equality (matching
after any rotation, reversal, or `Polygon`-vs-single-part-`MultiPolygon`
wrapping), not full OGC point-set equality — two polygons covering the same
area with different vertexization are not detected as equal.

`GEO_CLIP` uses Sutherland-Hodgman polygon clipping, which is only guaranteed
correct against a convex boundary; it validates convexity by default and
errors otherwise, with `allow_nonconvex=true` as an explicit best-effort
opt-out. `GEO_CLIP` supports Point/MultiPoint and Polygon/MultiPolygon
subjects; LineString clipping needs a different algorithm and isn't
supported.

### Geometry editing and quality

| Task | Function | PostGIS-style alias |
| --- | --- | --- |
| Simplify a geometry | `GEO_SIMPLIFY(geometry, tolerance[, method])` | `ST_SIMPLIFY` |
| Inspect bbox / centroid | `GEO_BBOX`, `GEO_CENTROID` | `ST_BBOX`, `ST_CENTROID` |
| Shift, scale, rotate | `GEO_AFFINE` | `ST_AFFINE` |
| Chaikin smoothing | `GEO_SMOOTH` | `ST_SMOOTH` |
| Remove polygon holes | `GEO_DROP_HOLES` | `ST_REMOVE_HOLES` |
| Clean duplicate vertices and close rings | `GEO_CLEAN` | `ST_CLEAN` |
| Snap x/y to a grid | `GEO_SNAP(geometry, gridSize)` | `ST_SNAPTOGRID` |
| Structural GeoJSON check | `GEO_IS_VALID` | `ST_ISVALID` |

`GEO_SIMPLIFY` accepts Douglas-Peucker (`dp`, the default),
`visvalingam-effective`, and `visvalingam-weighted`. Simplification, affine,
smoothing, cleanup, and snapping operate in **source coordinate units**.
`GEO_SNAP` and `GEO_CLEAN` reject a result that collapses a line or polygon
ring below the GeoJSON minimum vertex count.

`GEO_IS_VALID` checks supported GeoJSON structure and vertex requirements. It
does not yet detect topology issues such as self-intersections. There is no
R-tree, and an ordinary `WHERE GEO_DWITHIN(...)`/`GEO_WITHIN_BBOX(...)`
predicate is not planner-accelerated — it filters rows after ordinary index
narrowing. For BI-scale point tables, `GEO_SEARCH` (below) provides an
explicit, indexed alternative.

The [interactive map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
can edit built-in or uploaded GeoJSON locally, display source and result, tune
parameters, check validity, and download the result.

### Region operations, spatial search, and choropleth classification

Mapshaper-inspired region-editing verbs and BI-oriented helpers for turning
raw geometry into location-based KPIs and dashboards:

| Task | Function |
| --- | --- |
| Merge a group's polygons into one (shared-edge dissolve) | `GEO_DISSOLVE(geometry[, snap_grid_degrees])` |
| Same operation, aggregate-style names | `GEO_UNION_AGG`, `ST_UNION` |
| Bounding box across a group | `GEO_BBOX_AGG(geometry)` |
| (Optionally weighted) centroid across a group | `GEO_CENTROID_AGG(geometry[, weight])` |
| Indexed bbox/radius search over a table | `GEO_SEARCH(table, geom_col, 'bbox'\|'radius', ...)` |
| Equal-interval choropleth classification | `EQUAL_INTERVAL(n) OVER (ORDER BY kpi)` |
| Natural-breaks (Jenks) choropleth classification | `NATURAL_BREAKS(n) OVER (ORDER BY kpi)` |
| Quantile choropleth classification | `NTILE(n) OVER (ORDER BY kpi)` (already existed) |

```sql
-- Dissolve adjacent building footprints into one district boundary per region,
-- then bucket a KPI (e.g. building count) into 5 choropleth classes.
SELECT region, GEO_DISSOLVE(footprint) AS boundary, COUNT(*) AS buildings
FROM parcels
GROUP BY region;

SELECT region, buildings,
       NATURAL_BREAKS(5) OVER (ORDER BY buildings) AS class
FROM region_stats;
```

`GEO_DISSOLVE`/`GEO_UNION_AGG`/`ST_UNION` merge polygons by cancelling shared
directed edges — correct for topologically-clean, vertex-aligned adjacent
input (real GIS boundary data, or this project's own dissolve output fed back
in), not a general polygon-boolean-union for overlapping-but-misaligned
input. Points/lines in a group are concatenated into a `MultiPoint`/
`MultiLineString` instead of dissolved. `GEO_CENTROID_AGG`'s optional weight
combines with `GEO_CENTROID`'s own area/length weighting, so
`GEO_CENTROID_AGG(geom, population)` is a population-weighted centroid of
already-area-weighted per-row centroids.

`GEO_SEARCH` builds a lazy, per-table grid index (invalidated automatically
on writes) and is exact for Point columns; for polygon/line columns it
indexes by centroid, so a large shape whose edge — not its centroid — clips
into the query window is a false negative there. `ST_INTERSECTS` (above)
remains the exact, unindexed way to test shape overlap directly.

Quantile classification (`NTILE`) already existed; `EQUAL_INTERVAL` and
`NATURAL_BREAKS` are new. `NATURAL_BREAKS`'s Jenks optimization is O(rows ×
classes²) per partition (computed once, not per row) — fine for realistic
choropleth partition sizes (municipalities, postal codes, districts), not
tuned for tens of thousands of rows.

## Map tiles and MBTiles

The map demo fetches every tile with live SQL in WebAssembly. Its source is in
[`cmd/mbtilesdemo`](./cmd/mbtilesdemo).

Web maps use top-origin XYZ rows while MBTiles stores bottom-origin TMS rows.
Use `TILE_FLIP_Y` at the SQL boundary:

```sql
SELECT tile_data FROM tiles
WHERE zoom_level = 14
  AND tile_column = TILE_X(13.405, 14)
  AND tile_row = TILE_FLIP_Y(TILE_Y(52.520, 14), 14);
```

Other tile helpers include `TILE_ZXY`, `TILE_BBOX`, `TILE_LON`, `TILE_LAT`,
`TILE_QUADKEY`, `TILE_FROM_QUADKEY`, `TILE_PARENT`, `TILE_CONTAINS`, and
`TILE_COUNT`. Add an index before serving a regular tiles table:

```sql
CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
```

MBTiles import/export uses the optional `sqliteimport` build tag:

```go
importer.ImportMBTiles(ctx, db, "default", "tiles", "city.mbtiles",
	&importer.ImportOptions{CreateTable: true, BatchSize: 1000})

importer.ExportMBTiles(ctx, db, "default", "out.mbtiles",
	&importer.ExportMBTilesOptions{TileRowIsTMS: true})
```

For HTTP delivery, run `tinysqld -tiles`:

```text
GET /tiles/{tileset}/{z}/{x}/{y}.{ext}
GET /tiles/{tileset}.json
GET /tiles/{tileset}/metadata
```

Tile routes are intentionally unauthenticated because normal map clients
cannot attach a bearer token to tile requests. Put an authenticating proxy in
front if access restrictions are required. See the
[storage guide](./docs/storage-guide.md) for large, paged-index tilesets.

## Imports, exports, and optional build tags

The core engine has no SQLite or Shapefile runtime dependency. Enable those
read/write paths only in builds that need them:

```bash
# SQLite files and MBTiles via pure-Go modernc SQLite (also required for
# ModeSQLite — a real .sqlite file as tinySQL's native storage; see the
# storage guide)
go build -tags=sqliteimport ./...

# ESRI Shapefile and Shapefile ZIP imports
go build -tags=shapefile ./...

# Both profiles
go build -tags=sqliteimport,shapefile ./...
```

Without a tag, the corresponding import API remains available and returns a
feature-disabled error. The tags are not needed to serve a tileset already
loaded into TinySQL.

The `exporter` package writes result sets as CSV, TSV, JSON, NDJSON, XML, GOB,
SQL, Excel (XLSX), GeoJSON, or TopoJSON. It preserves binary values with
self-identifying encodings and can emit a table manifest with schema, row
count, and a typed-row SHA-256 fingerprint. See
[`ExampleExportJSON`](./exporter/example_test.go).

`ExportGeoJSON`/`ExportTopoJSON` turn a query result's geometry column (named
explicitly, or auto-detected when exactly one candidate exists) plus every
other selected column into a GeoJSON `FeatureCollection` or TopoJSON
`Topology` — the format Power BI Shape Maps, D3, and most mapping tools
expect, ready to drop a computed KPI straight onto a map. The `tinysql` CLI
exposes both as `-mode geojson`/`-mode topojson` (with `-geom-col` to
override auto-detection). TopoJSON export deduplicates whole shared-boundary
rings into one arc (referenced forward by one feature, reversed by its
neighbor) — cheap once every ring is hashed for the arcs table anyway, and
exactly what a `GEO_DISSOLVE`-produced or otherwise topologically-clean
dataset benefits from — but does not split a boundary that only partially
coincides with another (real topology-building tools like mapshaper do; this
is a documented v1 scope cut, not a bug). `ImportTopoJSON` (or `ImportFile`/
`.import` on a `.topojson` file) resolves arc references — including the
reversed (`~i`) convention and multi-arc-per-ring stitching from topologies
built by other tools — back into ordinary geometry rows.

## Storage, transactions, and operations

| Mode | Best fit |
| --- | --- |
| `ModeMemory` | Tests, browser/WASM, and temporary local data |
| `ModeWAL` | In-memory tables with write-ahead-log recovery |
| `ModeDisk` | Per-table GOB files with lazy loading |
| `ModeJSON` | Human-readable, diffable per-table files |
| `ModeIndex` / `ModeHybrid` | Disk-backed tables with bounded caching |
| `ModePagedIndex` | Large equality-lookup workloads such as MBTiles |
| `ModeSQLite` | A real `.sqlite` file, readable by any SQLite tool (requires the `sqliteimport` build tag) |

Use `OpenDB` with a `StorageConfig` for persistent storage. Health checks,
read-only operation, audit logging, and lifecycle helpers are available for
embedded services. Encryption at rest covers table files in supported disk
backends; see the [storage guide](./docs/storage-guide.md) for exact scope.

For high-repeat vector searches, `ConfigureVectorCache` can enable a bounded,
process-local result cache and anonymous shape/timing analytics. See the
[RAG guide](./docs/rag-guide.md) before combining vector metrics and reranking.

## Guides and development

| Guide | Use it for |
| --- | --- |
| [Developer integration](./docs/developer-integration.md) | Go, `database/sql`, and browser embedding |
| [CLI guide](./docs/cli-guide.md) | REPL, servers, and file-query tools |
| [Storage guide](./docs/storage-guide.md) | Backends, DSNs, read-only mode, large tilesets |
| [RAG guide](./docs/rag-guide.md) | Vector, hybrid retrieval, reranking, and context |
| [TinyGo guide](./docs/tinygo-guide.md) | TinyGo, embedded targets, and WASM |
| [Architecture](./docs/architecture.md) | Parser, executor, storage, and invariants |
| [Development guide](./docs/development-guide.md) | Tests, Make targets, and releasing demos |
| [Benchmarks](./BENCHMARKS.md) | Reproducible performance measurements |

Run the full test suite:

```bash
go test ./...
```

Build the browser playground:

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

## Limitations

- Single-process only: no built-in replication, clustering, sharding,
  distributed transactions, or failover.
- No composite primary/foreign keys, `CHECK`, `UPSERT`/`ON CONFLICT`,
  `SAVEPOINT`, `ATTACH`/`DETACH`, `VACUUM`, partial indexes, generated
  columns, or persistent ANN vector index files.
- Secondary indexes optimize equality/prefix seeks and numeric ranges; text
  and BLOB range predicates still scan. There is no R-tree, and an ordinary
  `WHERE GEO_DWITHIN(...)`-style predicate is not planner-accelerated; the
  explicit `GEO_SEARCH` table function is the indexed alternative for
  point-column bbox/radius queries at scale.
- `ModeIndex` and `ModeHybrid` still use a full-table legacy codec on cache
  miss; use SQLite or `ModePagedIndex` for strict large-tile serving needs.
- RBAC is coarse and single-table oriented. Encryption does not yet cover
  WAL-backed modes or metadata files.
- GIS validity is structural, not full topology validation (no
  self-intersection detection). `GEO_DISSOLVE`/`GEO_UNION_AGG` only handle
  topologically-clean, vertex-aligned adjacent polygons, not a general
  polygon-boolean-union; there is no CRS/projection transform support, and
  `ST_TOUCHES`/`ST_CROSSES`/`ST_OVERLAPS` (which need a full DE-9IM
  computation) are not implemented.

TinySQL is primarily an educational and embeddable SQL engine. It aims to make
the parser, planner, executor, storage backends, and practical extensions easy
to inspect, test, and adapt.
