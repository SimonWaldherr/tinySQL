# tinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SimonWaldherr/tinySQL.svg)](https://pkg.go.dev/github.com/SimonWaldherr/tinySQL)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)

tinySQL is an embeddable SQL database engine written in Go. It is useful for
learning database internals, local tools, tests, browser/WASM applications, and
single-process services that need SQL without operating a database server.

[Browser playground](https://simonwaldherr.github.io/tinySQL/) ·
[map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html) ·
[video](https://youtu.be/W28-aBk3BL0)

> tinySQL is not a drop-in replacement for PostgreSQL, MySQL, or a clustered
> production database. Review [limitations](#limitations) before using it for
> critical workloads.

## Start here

tinySQL requires Go **1.26.5+**.

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Create an in-memory database and run SQL:

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
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
		"INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')",
	} {
		if _, err := tinysql.ExecSQL(ctx, db, "default", sql); err != nil {
			panic(err)
		}
	}

	result, err := tinysql.ExecSQL(ctx, db, "default",
		"SELECT id, name FROM users ORDER BY id")
	if err != nil {
		panic(err)
	}
	for _, row := range result.Rows {
		fmt.Println(row["id"], row["name"])
	}
}
```

For database/sql, streaming, columnar results, transactions, and the query
builder, see [developer integration](docs/developer-integration.md) and the
[driver package](./driver).

## Choose a workflow

| Goal | Start with |
| --- | --- |
| Explore tinySQL interactively | [demo](cmd/demo/README.md), [repl](cmd/repl/README.md), or [tinysql](cmd/tinysql/README.md) |
| Query files or migrate data | [query_files](cmd/query_files/README.md), [fsql](cmd/fsql/README.md), or [migrate](cmd/migrate/README.md) |
| Embed a local SQL service | [server](cmd/server/README.md) or [tinysqld](cmd/tinysqld/README.md) |
| Build a browser app | [query_files_wasm](cmd/query_files_wasm/README.md), [wasm_browser](cmd/wasm_browser/README.md), or [wasm_node](cmd/wasm_node/README.md) |
| Use AI tooling or local RAG | [tinysql-mcp-server](cmd/tinysql-mcp-server/README.md) and the [RAG guide](docs/rag-guide.md) |
| Browse every runnable program | [command index](cmd/README.md) |

## Capabilities

| Area | Includes |
| --- | --- |
| SQL | DDL/DML, CTEs, joins, grouping, windows, views, triggers, table-valued functions, stored procedures, jobs, and common SQLite-compatible PRAGMAs |
| Data | CSV/TSV, JSON/NDJSON, XML, YAML, Excel, GeoJSON, TopoJSON, KML, OSM XML, routing graphs, Shapefiles, GeoPackage, and MBTiles |
| Search | Full-text, vector, hybrid search, RAG helpers, regex, JSON, URL, HTML, date, math, bitmap, and hash functions |
| Deployment | Pure-Go embedded API, database/sql driver, CLI, HTTP/gRPC server, browser/WASM builds, and multiple storage backends |

The complete SQL function reference is [FUNCTIONS.sql](FUNCTIONS.sql). Runnable
feature examples are in [example_showcase.sql](example_showcase.sql).

### GIS, routing, and tiles

Geometry is stored as GeoJSON or validated GEOMETRY; tinySQL includes
measurement, predicates, editing, spatial search, choropleth classification,
WKT/WKB, geohash, and Web Mercator helpers. Use the
[geospatial standards guide](docs/geospatial-standards.md) for formats, CRS
profiles, coordinate conventions, and interoperability limits.

Routing functions run Dijkstra or coordinate-guided A* over ordinary edge
tables. The [OSM routing guide](docs/osm-routing.md) explains graph import,
profiles, turn restrictions, and the HTTP demo.

For tiles, the TILE_*, MBTILES_*, and TILE_COVER functions support Web Mercator
and OGC TileMatrix workflows. tinysqld -tiles can publish an MBTiles-shaped
table; see its [README](cmd/tinysqld/README.md) and the
[storage guide](docs/storage-guide.md) for durable artifacts.

### Retrieval and RAG

Vector, full-text, and hybrid retrieval can be combined with metadata filters
and reranking. RAG_WARM and ROUTE_WARM prepare derived serving structures
before traffic. The [RAG guide](docs/rag-guide.md) covers schema design,
ingestion, evaluation, tuning, caching, and context expansion.

## Storage and optional imports

Open persistent databases with OpenDB and a StorageConfig:

| Mode | Best fit |
| --- | --- |
| ModeMemory | Tests, browser/WASM, and temporary data |
| ModeWAL | In-memory tables with write-ahead-log recovery |
| ModeDisk | Per-table GOB files with lazy loading |
| ModeJSON | Human-readable, diffable per-table JSON |
| ModeIndex / ModeHybrid | Disk-backed tables with bounded caching |
| ModePagedIndex | Large equality lookups such as MBTiles |
| ModeSQLite | A .sqlite file interoperable with SQLite tools; requires sqliteimport |

For example, ModeJSON persists each table as readable JSON:

```go
db, err := tinysql.OpenDB(tinysql.StorageConfig{
	Mode: tinysql.ModeJSON,
	Path: "./data",
})
if err != nil {
	panic(err)
}
defer db.Close()
```

The core has no SQLite or Shapefile runtime dependency. Build optional file
support only when needed:

```bash
go build -tags=sqliteimport ./...          # SQLite, GeoPackage, MBTiles
go build -tags=shapefile ./...             # ESRI Shapefile and ZIP imports
go build -tags=sqliteimport,shapefile ./...
```

See the [storage guide](docs/storage-guide.md) for DSNs, persistence,
read-only serving, backups, encryption scope, and large datasets.

## Services and operations

server provides HTTP and gRPC APIs, optional TLS and bearer authentication,
plus asynchronous read-only replicas. The [cluster guide](docs/cluster.md)
includes a primary/replica deployment and recovery workflow. tinysqld is the
durable DBMS entry point with health, scheduler, and optional tile endpoints.

Both are deliberately smaller than a general-purpose distributed database:
replication is asynchronous, and automatic failover, multi-primary writes, and
distributed transactions are not implemented.

## Guides

| Guide | Use it for |
| --- | --- |
| [Developer integration](docs/developer-integration.md) | Go API, database/sql, streaming, and browser embedding |
| [CLI guide](docs/cli-guide.md) | Shells, servers, and file-query tools |
| [Storage guide](docs/storage-guide.md) | Backends, DSNs, read-only mode, and large artifacts |
| [RAG guide](docs/rag-guide.md) | Vector, hybrid retrieval, reranking, and context |
| [Geospatial standards](docs/geospatial-standards.md) | GIS formats, CRS profiles, and interoperability |
| [SQL feature gaps](docs/sql-feature-gaps.md) | Supported SQL and current gaps |
| [Architecture](docs/architecture.md) | Parser, executor, storage, and invariants |
| [Development guide](docs/development-guide.md) | Tests, Make targets, and release workflow |
| [API stability](docs/api-stability.md) | Compatibility guarantees and upgrades |

## Develop

```bash
go test ./...
go vet ./...
```

The browser playground build lives in cmd/query_files_wasm:

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

## Limitations

- tinySQL is embedded and single-process. The server offers asynchronous
  primary/replica reads, not sharding, automatic failover, multi-primary
  writes, or distributed transactions.
- Composite primary/foreign keys, CHECK, target-bearing ON CONFLICT DO UPDATE,
  SAVEPOINT, ATTACH/DETACH, VACUUM, partial indexes, generated columns, and
  persistent ANN index files are not available.
- Secondary indexes accelerate equality/prefix seeks and numeric ranges. Use
  GEO_SEARCH for indexed point-column bbox/radius queries; ordinary
  WHERE GEO_DWITHIN(...) predicates are not planner-accelerated.
- GIS validity is structural rather than full topology validation.
  GEO_DISSOLVE/GEO_UNION_AGG require clean, vertex-aligned adjacent polygons;
  they are not general polygon-boolean union operations.
- RBAC is coarse and single-table oriented. Encryption does not cover
  WAL-backed modes or metadata files.

tinySQL is primarily an educational and embeddable engine. It keeps the parser,
planner, executor, storage backends, and practical extensions easy to inspect,
test, and adapt.
