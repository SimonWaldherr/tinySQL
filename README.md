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

- CSV, TSV, JSON/NDJSON, XML, YAML, Excel, GeoJSON, KML, OSM XML, routing
  graphs, Shapefiles, and MBTiles import paths.
- GeoJSON measurement, containment, editing, cleanup, and inspection functions.
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

TinySQL uses ordinary GeoJSON stored in `TEXT` or `JSON` columns. Most geometry
functions return GeoJSON too, so results can be passed directly into later SQL
calls or drawn in a map client.

```sql
CREATE TABLE places (name TEXT, geometry JSON);
INSERT INTO places VALUES
  ('Berlin', GEO_POINT(13.4050, 52.5200)),
  ('Munich', GEO_POINT(11.5755, 48.1372));

SELECT GEO_DISTANCE(a.geometry, b.geometry) AS meters,
       GEO_BEARING(a.geometry, b.geometry) AS bearing,
       GEO_MIDPOINT(a.geometry, b.geometry) AS midpoint
FROM places a JOIN places b
  ON a.name = 'Berlin' AND b.name = 'Munich';
```

### Measurement and predicates

| Task | Function | PostGIS-style alias |
| --- | --- | --- |
| Create/read a point | `GEO_POINT`, `GEO_LON`, `GEO_LAT` | `ST_POINT`, `ST_X`, `ST_Y` |
| Great-circle distance | `GEO_DISTANCE` | `ST_DISTANCE`, `HAVERSINE` |
| Radius check | `GEO_DWITHIN` | `ST_DWITHIN` |
| Bounding-box check | `GEO_WITHIN_BBOX` | `ST_WITHIN_BBOX` |
| Bearing / midpoint / destination | `GEO_BEARING`, `GEO_MIDPOINT`, `GEO_DESTINATION` | `ST_AZIMUTH`, `ST_MIDPOINT`, `ST_PROJECT` |
| Point in polygon | `GEO_WITHIN_POLYGON` | `ST_WITHIN`, `ST_CONTAINS` |
| Polygon area / line length | `GEO_POLYGON_AREA`, `GEO_LENGTH` | `ST_AREA`, `ST_LENGTH` |

Distance, bearing, destination, length, and area calculations operate on the
sphere; distances are meters and polygon areas are square meters. Coordinates
in raw four-number forms use `(lat, lon, lat, lon)`; GeoJSON remains
`[lon, lat]`.

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
R-tree: spatial predicates currently filter rows after ordinary index narrowing.

The [interactive map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html)
can edit built-in or uploaded GeoJSON locally, display source and result, tune
parameters, check validity, and download the result.

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
# SQLite files and MBTiles via pure-Go modernc SQLite
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
or SQL. It preserves binary values with self-identifying encodings and can
emit a table manifest with schema, row count, and a typed-row SHA-256
fingerprint. See [`ExampleExportJSON`](./exporter/example_test.go).

## Storage, transactions, and operations

| Mode | Best fit |
| --- | --- |
| `ModeMemory` | Tests, browser/WASM, and temporary local data |
| `ModeWAL` | In-memory tables with write-ahead-log recovery |
| `ModeDisk` | Per-table GOB files with lazy loading |
| `ModeJSON` | Human-readable, diffable per-table files |
| `ModeIndex` / `ModeHybrid` | Disk-backed tables with bounded caching |
| `ModePagedIndex` | Large equality-lookup workloads such as MBTiles |

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
  and BLOB range predicates still scan. There is no R-tree.
- `ModeIndex` and `ModeHybrid` still use a full-table legacy codec on cache
  miss; use SQLite or `ModePagedIndex` for strict large-tile serving needs.
- RBAC is coarse and single-table oriented. Encryption does not yet cover
  WAL-backed modes or metadata files.
- GIS validity is structural, not full topology validation. Overlay, dissolve,
  buffer, projection transforms, and self-intersection repair are not yet
  implemented.

TinySQL is primarily an educational and embeddable SQL engine. It aims to make
the parser, planner, executor, storage backends, and practical extensions easy
to inspect, test, and adapt.
