# tinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SimonWaldherr/tinySQL.svg)](https://pkg.go.dev/github.com/SimonWaldherr/tinySQL)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)

tinySQL is an embeddable SQL database engine written in Go. It is designed for
learning database internals, local tools, tests, browser/WASM applications, and
single-process services that need a capable SQL layer without operating a
database server.

**Try it:** [browser playground](https://simonwaldherr.github.io/tinySQL/) ·
[interactive map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
· [video](https://youtu.be/W28-aBk3BL0)

> tinySQL is not a drop-in replacement for PostgreSQL, MySQL, or a clustered
> production database. Review the [limitations](#limitations) before using it
> for critical workloads.

## Contents

- [Quick start](#quick-start)
- [What it can do](#what-it-can-do)
- [Web, URL, and HTML helpers](#web-url-and-html-helpers)
- [Using SQL from Go](#using-sql-from-go)
- [GIS and GeoJSON](#gis-and-geojson)
- [Routing graphs and shortest paths](#routing-graphs-and-shortest-paths)
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

For large scans, `ExecSQLStream` exposes rows while the query is still
running instead of materializing the complete result first:

```go
stream, err := tinysql.ExecSQLStream(ctx, db, "default",
	`SELECT id, name FROM users WHERE active = true`)
if err != nil { panic(err) }
defer stream.Close() // important when iteration stops early

for stream.Next() {
	row := stream.Row()
	fmt.Println(row["id"], row["name"])
}
if err := stream.Err(); err != nil { panic(err) }
```

Simple table scans, filters, projections, index seeks, and `LIMIT`/`OFFSET`
stream incrementally. Operations that require the complete input—such as
`ORDER BY`, `GROUP BY`, `DISTINCT`, joins, and set operations—preserve exact
SQL semantics and start yielding after their result has been materialized.

For applications that already use `database/sql`, use
[`github.com/SimonWaldherr/tinySQL/driver`](./driver).

The public driver includes workload profiles that expose the underlying
bounded-storage, WAL, checkpoint and pool settings without hand-written DSNs:

```go
navCfg  := driver.OfflineNavigationOpenConfig("./nav-artifact") // index, bounded, read-only
ragCfg  := driver.RAGOpenConfig("./rag-artifact")               // hybrid retrieval working set
toolCfg := driver.EmbeddedToolOpenConfig("./tool.db")           // ACID WAL for database/sql

db, err := driver.OpenWithConfig(ctx, toolCfg)
```

For an offline navigator, call `ROUTE_WARM` before accepting route requests.
For a hybrid RAG service, `RAG_WARM(table, text_col, vector_col[, metric
[, index]])` builds its lexical and vector serving structures concurrently.
The embedded-tool profile is intended for replacing a networked SQL dependency
inside one Go process; tinySQL remains deliberately smaller than MySQL and is
not wire-protocol or full-dialect compatible.

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

### Stored procedures

Stored procedures are process-local Go handlers invoked with `CALL`. The
option-based API adds catalog metadata, positional argument validation,
parallel read-only execution and optional statement-level rollback for
multi-step writes. `ExecuteSQLArgs` binds nested SQL values safely:

```go
err := tinysql.RegisterStoredProcedureWithOptions("find_docs",
    tinysql.StoredProcedureOptions{
        Description: "Find matching documents",
        ReadOnly: true,
        Parameters: []tinysql.StoredProcedureParameter{
            {Name: "pattern", Required: true},
        },
    },
    func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
        return ctx.ExecuteSQLArgs(
            "SELECT id, title FROM docs WHERE title LIKE ? ORDER BY title",
            args[0],
        )
    })
```

Use `Atomic: true` for a mutating procedure whose nested statements must all
roll back when its handler returns an error. `sys.procedures` exposes argument
metadata, `read_only`, `atomic`, call/error counts and average runtime. The
native examples are in [`cmd/procedure_demo`](./cmd/procedure_demo); the WASM
playground registers the same reusable demo set.

### Web, URL, and HTML helpers

Web-oriented SQL can parse and resolve URLs, read query parameters, encode
query values or path segments, escape HTML, and render contextual-safe Go
templates from JSON data:

```sql
SELECT URL_QUERY_GET('https://example.org/search?q=geo+data', 'q');
SELECT URL_RESOLVE('https://example.org/a/b/', '../tiles/12/2200/1400.pbf');
SELECT URL_PATH_ENCODE('tiles/Altstadt Nord');
SELECT HTML_ESCAPE('<strong>untrusted</strong>');
SELECT HTML_TEMPLATE('<a href="{{.url}}">{{.label}}</a>',
                     '{"url":"/map","label":"Map & data"}');
```

`URL_PARSE` returns the URL components as JSON. `URL_ENCODE`/`URL_DECODE`
operate on query values, while `URL_PATH_ENCODE`/`URL_PATH_DECODE` operate on
one path segment. `HTML_TEMPLATE` caches up to 128 compiled templates and uses
`html/template` contextual escaping. `HTTP(url)` reuses pooled HTTP
connections, follows query cancellation and limits a successful response to
8 MiB.

### Data and maps

- CSV, TSV, JSON/NDJSON, XML, YAML, Excel, GeoJSON, TopoJSON, KML, OSM XML,
  routing graphs, Shapefiles, OGC GeoPackage, and MBTiles import paths.
- GeoJSON measurement, containment, relationship, editing, cleanup, region
  (dissolve/clip), and inspection functions, plus a spatial search index and
  choropleth classification (equal-interval, natural breaks, quantile) for
  location-based BI dashboards.
- WKT/EWKT and WKB/EWKB parsing and rendering, geohash encode/decode, and
  Web Mercator reprojection for interop with other GIS tools.
- GeoJSON and TopoJSON export from query results (`-mode geojson|topojson`),
  the format Power BI Shape Maps and most mapping tools prefer.
- Dijkstra shortest-path queries (`ROUTE_SHORTEST_PATH`/`ROUTE_DISTANCE`)
  directly over an edge table, directed or undirected.
- Web Mercator tile addressing, MBTiles import/export, in-place tile access
  (as tables or directly from SQL with `MBTILES_TILE`/`MBTILES_TILES`), and
  an optional XYZ tile endpoint.

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

### Geospatial interoperability

tinySQL supports OGC GeoPackage/GeoPackageBinary, OGC and ISO SQL/MM WKB,
GeoJSON, EPSG and OGC CRS identifiers, WMS axis rules, OGC TileMatrix math,
MBTiles, KML, and OSM XML. Common INSPIRE, AdV/BKG, and Bavarian CRS profiles
are included in the CRS helpers.

The detailed format matrix, implementation limits, and planned additions such
as GML 3.2, WFS/OGC API, XPlanGML, NAS, GeoDCAT-AP, and COG are documented in
[`docs/geospatial-standards.md`](./docs/geospatial-standards.md).

tinySQL stores geometry as ordinary GeoJSON, either in a `TEXT`/`JSON` column
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
disjoint from it). `GEO_EQUALS` is scoped to coordinate/shape equality
(matching after any rotation, reversal, or `Polygon`-vs-single-part-
`MultiPolygon` wrapping), not full OGC point-set equality — two polygons
covering the same area with different vertexization are not detected as
equal.

| Task | Function | PostGIS-style alias |
| --- | --- | --- |
| Boundary-only contact (no interior overlap) | `GEO_TOUCHES` | `ST_TOUCHES` |
| Every point of B is in-or-on A | `GEO_COVERS` | `ST_COVERS` |
| Every point of A is in-or-on B | `GEO_COVEREDBY` | `ST_COVEREDBY` |
| Polygon boundary length (sum of all rings) | `GEO_PERIMETER` | `ST_PERIMETER` |

`ST_TOUCHES`/`ST_COVERS`/`ST_COVEREDBY` are a documented approximation rather
than a full DE-9IM computation: exact for point/line arguments, and for
polygon arguments checked via each ring's vertices and edge midpoints rather
than a general polygon-arrangement algorithm. This is correct for the
adjacency and containment shapes real GIS data hits constantly (shared
edges, nested holes, one polygon fully inside another) but can in principle
miss a pathological case — a straight edge that dips into and back out of
the other polygon's interior between two vertices that are themselves
outside that interior, without crossing any edge along the way. `ST_CROSSES`/
`ST_OVERLAPS` and full topological equality remain out of scope for the same
reason `GEO_EQUALS` is scoped the way it is above.

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

### Interop: WKT, WKB, geohash, and reprojection

| Task | Function | PostGIS-style alias |
| --- | --- | --- |
| Parse WKT/EWKT text | `GEO_FROM_WKT(text[, srid])` | `ST_GEOMFROMTEXT`, `ST_GEOMFROMEWKT` |
| Typed WKT constructors (error on a type mismatch) | — | `ST_POINTFROMTEXT`, `ST_LINEFROMTEXT`, `ST_POLYGONFROMTEXT` |
| Render as WKT / EWKT | `GEO_AS_WKT`, `GEO_AS_EWKT` | `ST_ASTEXT`, `ST_ASEWKT` |
| Parse/render WKB (BLOB or hex text) | `GEO_FROM_WKB`, `GEO_AS_WKB` | `ST_GEOMFROMWKB`, `ST_ASBINARY` |
| Parse/render EWKB (adds an SRID header) | `GEO_FROM_WKB`, `GEO_AS_EWKB` | `ST_GEOMFROMEWKB`, `ST_ASEWKB` |
| Inspect an OGC GeoPackageBinary BLOB | `GPKG_SRID`, `GPKG_BBOX`, `GPKG_HEADER`, `GPKG_IS_EMPTY` | — |
| Extract WKB / decode WGS84 GeoPackageBinary | `GPKG_AS_WKB`, `GEO_FROM_GPKG` | `ST_GEOMFROMGPKG` |
| Canonicalize/validate a GeoJSON value | `GEO_FROM_GEOJSON` | `ST_GEOMFROMGEOJSON` |
| Render GeoJSON, optionally rounded | `GEO_AS_GEOJSON(geometry[, max_decimal_digits])` | `ST_ASGEOJSON` |
| Encode/decode a geohash | `GEO_GEOHASH_ENCODE(point[, precision])`, `GEO_GEOHASH_DECODE` | `ST_GEOHASH` |
| Geohash cell bounds / surrounding cells | `GEO_GEOHASH_BBOX`, `GEO_GEOHASH_NEIGHBORS` | — |
| Reproject to/from Web Mercator | `GEO_TRANSFORM(geometry, srid)` | `ST_TRANSFORM` |
| Normalize an EPSG/OGC CRS identifier | `CRS_NORMALIZE`, `CRS_URI`, `CRS_INFO` | — |
| Inspect normative CRS axis order | `CRS_AXIS_ORDER` | — |

WKT/WKB parsing accepts POINT/LINESTRING/POLYGON/MULTIPOINT/
MULTILINESTRING/MULTIPOLYGON/GEOMETRYCOLLECTION, both 2D and `Z`-tagged 3D
forms, ISO SQL/MM and EWKB `Z`/`M`/`ZM` encodings, `EMPTY`, and an optional
EWKT `SRID=...;`/EWKB SRID header — but only
for SRID 4326 (or 0/absent, treated as 4326): tinySQL's geo layer is WGS84
lon/lat throughout, so any other SRID is rejected with a pointer to
`ST_TRANSFORM` rather than silently mislabeled. An OGC `M` (measure)
ordinate is accepted on input and dropped, since a GeoJSON position has
nowhere to put it. `ST_TRANSFORM` itself only supports SRID 4326 (WGS84) and
3857 (Web Mercator, the projection this project's own `TILE_*` functions
already use) — a general reprojection engine needs a datum-shift parameter
database this project has no appetite for vendoring.

```sql
SELECT ST_ASTEXT(ST_GEOMFROMTEXT('POLYGON((13.3 52.4,13.5 52.4,13.5 52.6,13.3 52.6,13.3 52.4))')) AS wkt,
       ST_GEOHASH(ST_MAKEPOINT(13.405, 52.520), 8) AS geohash,
       ST_TRANSFORM(ST_MAKEPOINT(13.405, 52.520), 3857) AS web_mercator;

SELECT CRS_NORMALIZE('urn:ogc:def:crs:EPSG::25832') AS crs,
       CRS_AXIS_ORDER('http://www.opengis.net/def/crs/EPSG/0/3035') AS axes;
```

### Region operations, spatial search, and choropleth classification

Mapshaper-inspired region-editing verbs and BI-oriented helpers for turning
raw geometry into location-based KPIs and dashboards:

| Task | Function |
| --- | --- |
| Merge a group's polygons into one (shared-edge dissolve) | `GEO_DISSOLVE(geometry[, snap_grid_degrees])` |
| Same operation, aggregate-style names | `GEO_UNION_AGG`, `ST_UNION` |
| Bounding box across a group | `GEO_BBOX_AGG(geometry)` |
| (Optionally weighted) centroid across a group | `GEO_CENTROID_AGG(geometry[, weight])` |
| Indexed centroid bbox/radius search over a table | `GEO_SEARCH(table, geom_col, 'bbox'\|'radius', ...)` |
| Indexed GIS viewport/geometry-extent overlap | `GEO_SEARCH(table, geom_col, 'bbox_intersects', west, south, east, north)` |
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

`GEO_SEARCH` builds a lazy, per-table grid index (invalidated automatically on
writes). `bbox` and `radius` select by geometry centroid and are exact for
Point columns. Use `bbox_intersects` for GIS viewports: it retains a line or
polygon whenever its stored extent overlaps the viewport, then lets an outer
`ST_INTERSECTS` perform an exact shape test when bbox overlap alone is too
broad. The grid stores parsed centroids and extents once, so repeated DTK/DOP
layer requests do not reparse every GeoJSON value. In `bbox_intersects`, a
WGS84 bbox with `west > east` crosses the antimeridian; radius search handles
the same wrap automatically.

Quantile classification (`NTILE`) already existed; `EQUAL_INTERVAL` and
`NATURAL_BREAKS` are new. `NATURAL_BREAKS`'s Jenks optimization is O(rows ×
classes²) per partition (computed once, not per row) — fine for realistic
choropleth partition sizes (municipalities, postal codes, districts), not
tuned for tens of thousands of rows.

## Routing graphs and shortest paths

`importer.ImportRoutingGraph` loads a routing graph from JSON, NDJSON, or a
CSV edge list into ordinary tables — `<table>_nodes` (`node_id`, `lat`,
`lon`, `geometry`, `properties`) when the source has nodes, and `<table>`
(`edge_id`, `source`, `target`, `cost`, `distance`, `duration`, `mode`,
`geometry`, `properties`) for edges — recognizing the common
`source`/`from`/`source_id` and `target`/`to`/`target_id` column aliases.

The routing SQL functions run Dijkstra or coordinate-guided A* directly over
any such edge table — or any table at all with a source column, a target
column, and a non-negative numeric weight column, whether or not it came from
`ImportRoutingGraph`:

| Task | Function |
| --- | --- |
| Full shortest path as rows (one per node visited) | `ROUTE_SHORTEST_PATH(table, source_col, target_col, weight_col, start_id, end_id[, direction])` |
| Just the total cost (or `NULL` if unreachable) | `ROUTE_DISTANCE(table, source_col, target_col, weight_col, start_id, end_id[, direction])` |
| A*-guided total cost | `ROUTE_DISTANCE_ASTAR(edge_table, source_col, target_col, weight_col, node_table, node_id_col, lat_col, lon_col, start_id, end_id, min_cost_per_metre[, direction])` |
| A*-guided path, including remaining air distance | `ROUTE_SHORTEST_PATH_ASTAR(edge_table, source_col, target_col, weight_col, node_table, node_id_col, lat_col, lon_col, start_id, end_id, min_cost_per_metre[, direction])` |
| Air-line distance in metres | `ROUTE_AIR_DISTANCE(node_table, node_id_col, lat_col, lon_col, start_id, end_id)` |
| Many-to-many cost matrix | `ROUTE_DISTANCE_MATRIX(table, source_col, target_col, weight_col, source_ids_json, target_ids_json[, direction])` |
| Reachable nodes / service area | `ROUTE_REACHABLE(table, source_col, target_col, weight_col, start_id, max_cost[, direction[, limit]])` |
| Prebuild a routing graph | `ROUTE_WARM(table, source_col, target_col, weight_col[, direction])` |

```sql
CREATE TABLE roads (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64);
INSERT INTO roads VALUES ('e1','A','B',1), ('e2','B','C',2), ('e3','A','C',10);

SELECT * FROM ROUTE_SHORTEST_PATH('roads', 'source', 'target', 'cost', 'A', 'C');
-- seq | node_id | edge_id | leg_cost | total_cost | geometry
--   0 | A       | NULL    | NULL     | 0          | NULL
--   1 | B       | e1      | 1        | 1          | NULL
--   2 | C       | e2      | 2        | 3          | NULL
-- (the direct A->C edge costs 10 and is correctly passed over)

SELECT ROUTE_DISTANCE('roads', 'source', 'target', 'cost', 'A', 'C') AS meters;
```

The A* heuristic is the great-circle distance multiplied by
`min_cost_per_metre`. It must be a real lower bound for the selected edge
cost (for duration in milliseconds and a 130 km/h maximum speed, for example,
use at most `3600 / 130`, about `27.69` ms/m). tinySQL verifies the supplied
factor against every graph edge before using it, so an overestimating
heuristic errors instead of returning a non-optimal route. A factor of `0`
is always valid and deliberately falls back to Dijkstra while keeping the A*
interface. Coordinate tables and graph-to-coordinate bindings are versioned,
bounded, and shared across concurrent requests just like the graph cache.
`ROUTE_SHORTEST_PATH_ASTAR` adds `air_distance_to_goal_m` to each returned
step, which is useful for progress and detour-factor displays.

For dispatching, distance tables and other batch workloads,
`ROUTE_DISTANCE_MATRIX` forms the Cartesian product of its two JSON node-id
arrays. It groups equal sources, consumes cached pairs first, and resolves all
remaining targets for a source in one early-terminating Dijkstra run:

```sql
SELECT * FROM ROUTE_DISTANCE_MATRIX(
  'roads', 'source', 'target', 'cost', '["A","B"]', '["C","D"]');

SELECT * FROM ROUTE_REACHABLE(
  'roads', 'source', 'target', 'cost', 'A', 900000, 'directed', 1000);
```

`ROUTE_REACHABLE` returns nodes in increasing shortest-cost order, including
the start node at cost zero. `max_cost` makes it suitable for travel-time
service areas and isochrone input; the optional `limit` bounds both search and
result materialization for nearest-by-travel-cost queries.

After an import or bulk update, `ROUTE_WARM` can build the versioned CSR graph
before serving traffic. It returns node/edge counts, whether the graph was
already cached, and elapsed time:

```sql
SELECT * FROM ROUTE_WARM('roads', 'source', 'target', 'duration', 'directed');
```

`direction` is `'directed'` (the default — travel only `source` → `target`)
or `'undirected'` (travel either way along every edge). A node id may be
text or numeric; `ROUTE_SHORTEST_PATH`/`ROUTE_DISTANCE` error if `start_id`
or `end_id` does not appear as any row's `source`/`target` value (a likely
typo), but return an empty result / `NULL` respectively for a real,
unreachable destination. Edge weights must be non-negative — Dijkstra's own
precondition — a negative weight errors rather than silently producing a
wrong answer; a graph that genuinely needs negative weights needs
Bellman-Ford, which is out of scope. If the edge table has an `edge_id` and/
or `geometry` column, `ROUTE_SHORTEST_PATH` reports the edge actually taken
and its geometry alongside each step; both are `NULL` if the table does not
have those columns. Like `GEO_SEARCH`, routing builds its derived structure
lazily. The bounded process-wide graph cache is keyed by tenant, table,
source/target/weight columns, and direction; writes invalidate it through the
table version, while rollback and `DROP TABLE` purge it eagerly. Concurrent
first requests for the same graph share one build.

## Map tiles and MBTiles

For multi-gigabyte, read-mostly datasets, see
[`docs/mbtiles-artifacts.md`](docs/mbtiles-artifacts.md) for the bounded
`dataset.tinysql` importer, validated artifact format, TMS reader API and
SQLite comparison procedure.

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

Other tile helpers include `TILE_ZXY`, `TILE_BBOX`, `TILE_BBOX_3857`,
`TILE_LON`, `TILE_LAT`, `TILE_QUADKEY`, `TILE_FROM_QUADKEY`, `TILE_PARENT`,
`TILE_CONTAINS`, and `TILE_COUNT`. OGC/WMTS helpers expose projected
meters-per-pixel and scale denominators. `TILE_MATRIX_BBOX` and
`TILE_MATRIX_POSITION` work from the origin, cell size, tile dimensions and
corner declared by an OGC TileMatrix, including non-Web-Mercator CRSs.
`WMS_BBOX` applies WMS 1.3 CRS axis order for common OGC, INSPIRE,
ETRS89, AdV and DHDN identifiers; an explicit final `xy`/`yx` argument covers
other registered or private CRSs:

```sql
SELECT TILE_RESOLUTION(14) AS meters_per_pixel,
       WMTS_SCALE_DENOMINATOR(14) AS scale_denominator,
       WMS_BBOX(11, 48, 12, 49, 'EPSG:4326', '1.3.0') AS wms_bbox,
       TILE_MATRIX_BBOX(100000, 500000, 10, 256, 256, 2, 3, 'topLeft') AS native_crs_tile;

-- Minimal XYZ/TMS set for prefetching a DTK/DOP viewport. The optional final
-- argument is a hard materialization limit for offline/cache jobs.
SELECT z, x, y, tile_row, bbox_3857, resolution, quadkey
FROM TILE_COVER(11.0, 48.0, 12.0, 49.0, 14, 50000);
```

`TILE_COVER` supports antimeridian-crossing bounds (`west > east`) and fails
instead of silently truncating when the requested cover exceeds `max_tiles`.
Add an index before serving a regular tiles table:

```sql
CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
```

GeoPackage and MBTiles access use the optional `sqliteimport` build tag.
GeoPackage layer discovery reads the normative `gpkg_contents`,
`gpkg_geometry_columns`, and `gpkg_spatial_ref_sys` catalogs:

```go
info, err := importer.InspectGeoPackage(ctx, "features.gpkg")
result, err := importer.ImportGeoPackage(ctx, db, "default", "features",
	"features.gpkg", &importer.ImportOptions{
		CreateTable: true,
		GeoPackageLayer: info.Layers[0].TableName,
	})
```

`GeoPackageGeometryMode: "auto"` converts only EPSG:4326 feature geometry to
GeoJSON. Geometry in EPSG:25832, EPSG:3035, DHDN or another projected/native
CRS remains an intact GeoPackageBinary BLOB; use `"wkb"` to strip only its
container header. This deliberately prevents metres from being silently
treated as RFC 7946 longitude/latitude. GeoPackage SRS identifiers `0` and
`-1` remain undefined and are never treated as WGS84. Standard-WKB payloads
are checked against the header SRS and catalog geometry type; tolerant imports
skip invalid rows, while `StrictTypes: true` aborts the import.

MBTiles import/export uses the same build profile:

```go
importer.ImportMBTiles(ctx, db, "default", "tiles", "city.mbtiles",
	&importer.ImportOptions{CreateTable: true, BatchSize: 1000})

importer.ExportMBTiles(ctx, db, "default", "out.mbtiles",
	&importer.ExportMBTilesOptions{TileRowIsTMS: true})
```

For reading an `.mbtiles` file's tiles or metadata directly from a query —
no separate Go-side import/open call first — three SQL functions read the
source SQLite file in place, gated by the same `sqliteimport` build tag
(calling any of them in a build without the tag returns a clear "requires
the sqliteimport build tag" error rather than "unknown function"):

| Task | Function |
| --- | --- |
| One tile's raw bytes by XYZ z/x/y (indexed point lookup) | `MBTILES_TILE(file_path, z, x, y)` |
| All tiles (optionally restricted to a zoom range) as rows | `MBTILES_TILES(file_path[, min_zoom, max_zoom])` |
| The tileset's `name`/`value` metadata rows | `MBTILES_METADATA(file_path)` |

```sql
SELECT MBTILES_TILE('city.mbtiles', 14, TILE_X(13.405, 14), TILE_FLIP_Y(TILE_Y(52.520, 14), 14)) AS tile_data;

SELECT zoom_level, tile_column, tile_row, tile_size, tile_sha256
FROM MBTILES_TILES('city.mbtiles', 0, 8);

SELECT * FROM MBTILES_METADATA('city.mbtiles');
```

`MBTILES_TILE` returns `NULL` (not an error) for a tile the tileset does not
cover — a sparse tileset not covering a given tile is normal. `MBTILES_TILES`
without a zoom range is bounded to 200,000 tiles and errors, rather than
silently truncating, past that — narrow the range for a large tileset.
Neither persists anything into the database; each call re-reads the source
file, the same "correct first, cache later if it matters" trade-off
`ROUTE_SHORTEST_PATH` below makes for its own edge table.

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
# SQLite files, OGC GeoPackage, and MBTiles via pure-Go modernc SQLite (also required for
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
loaded into tinySQL.

The interoperability policy and standards/profile matrix are documented in
[`docs/geospatial-standards.md`](./docs/geospatial-standards.md).

The `exporter` package writes result sets as CSV, TSV, JSON, NDJSON, XML, GOB,
SQL, Excel (XLSX), GeoJSON, or TopoJSON. It preserves binary values with
self-identifying encodings and can emit a table manifest with schema, row
count, and a typed-row SHA-256 fingerprint. See
[`ExampleExportJSON`](./exporter/example_test.go).

GOB export reuses canonical result-row maps when no display-key rewrite is
needed, while XML is emitted one row at a time instead of materializing a
second result-sized document. Whole-database GOB snapshots and the basic WAL
use 64 KiB buffered streams to reduce small writes without changing their
on-disk format. The HTTP/gRPC server also provides the protobuf schema in
[`cmd/server/tinysql.proto`](./cmd/server/tinysql.proto); internal federation
and replication carry binary GOB payloads directly in protobuf byte fields.

CSV input is sampled only for type inference and then inserted in bounded
batches. JSON imports likewise bound their inference copy and use the same
batch path, including direct appends to paged-index storage. CSV, JSON, and
NDJSON exports write incrementally and keep only one output row in working
memory. Tune `ImportOptions.BatchSize` for the destination: larger batches
favor bulk throughput, while smaller batches reduce peak memory and
cancellation latency.

For JSON APIs, `ImportJSON` accepts both JSON arrays and NDJSON. It infers
primitive columns, preserves nested objects and arrays as `JSON` columns, and
can declare an imported identifier as the table primary key:

```go
result, err := tinysql.ImportJSON(ctx, db, "default", "events", input,
	&tinysql.ImportOptions{CreateTable: true, PrimaryKey: "id"})
```

Use `tinysql.ExportTableJSON(ctx, w, db, "default", "events", opts)` to
write a complete table as a JSON array without manually issuing `SELECT *`.

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
Whole-database backups copy a consistent in-memory image under the database
read lock, then release that lock before encoding, compression, filesystem
sync, or network output. A slow backup target therefore does not hold up
concurrent writers for the duration of the transfer.

For high-repeat vector searches, `ConfigureVectorCache` can enable a bounded,
process-local result cache and anonymous shape/timing analytics. See the
[RAG guide](./docs/rag-guide.md) before combining vector metrics and reranking.

## Guides and development

| Guide | Use it for |
| --- | --- |
| [Developer integration](./docs/developer-integration.md) | Go, `database/sql`, and browser embedding |
| [Go API stability](./docs/api-stability.md) | Compatibility guarantees, streaming, and upgrades |
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

tinySQL is primarily an educational and embeddable SQL engine. It aims to make
the parser, planner, executor, storage backends, and practical extensions easy
to inspect, test, and adapt.
