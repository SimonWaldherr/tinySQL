# tinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SimonWaldherr/tinySQL.svg)](https://pkg.go.dev/github.com/SimonWaldherr/tinySQL)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)

An embedded SQL engine for Go, with persistent storage, vector/full-text search,
WebAssembly builds, and C, Python, Rust, and Swift bindings.

[Browser playground](https://simonwaldherr.github.io/tinySQL/) ·
[Documentation](docs/README.md)

## Get started

Requires Go **1.27.3+**:

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Create an in-memory database and query it with a bound parameter:

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

	if _, err := tinysql.ExecScript(ctx, db, "default", `
		CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
		INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace');`); err != nil {
		panic(err)
	}

	// Bind values with ?, $1 or :1 instead of formatting SQL text.
	result, err := tinysql.ExecSQLArgs(ctx, db, "default",
		"SELECT id, name FROM users WHERE id >= ? ORDER BY id", 1)
	if err != nil {
		panic(err)
	}
	for _, row := range result.Rows {
		fmt.Println(row["id"], row["name"])
	}
}
```

For `database/sql`, transactions, streaming, and the query builder, see
[Go integration](docs/developer-integration.md); for C, Python, Rust, and Swift,
see [language bindings](docs/language-bindings.md).

## Explore

| Area | Capabilities and guides |
| --- | --- |
| SQL | Joins, recursive CTEs, UNION, windows, views, triggers, and procedures. [Function reference](https://simonwaldherr.github.io/tinySQL/docs/reference/functions.html) ([SQL source](docs/reference/functions.sql)), [SQL examples](demos/sql/showcase.sql). |
| Retrieval | Exact flat or approximate IVF/HNSW vector search, BM25, hybrid ranking, and retrieval pre-filters. [RAG guide](docs/rag-guide.md). |
| Storage | In-memory, WAL, JSON, disk, indexed, and SQLite-backed modes; backups and read-only snapshots. [Storage guide](docs/storage-guide.md), [analytical snapshots](docs/analytical-snapshots.md). |
| Maps and files | Document/tabular/geospatial imports, geometry, tiles, and routing. [Geospatial guide](docs/geospatial-standards.md), [routing](docs/osm-routing.md), [file queries](cmd/query_files/README.md). |
| Applications | [CLI](docs/cli-guide.md), [HTTP/gRPC server](cmd/server/README.md), [browser workspace](cmd/query_files_wasm/README.md), and [command index](cmd/README.md). |

SQLite, GeoPackage, and MBTiles need the `sqliteimport` build tag; Shapefile
needs `shapefile`. The `tinysql_minimal` tag omits optional features for smaller
WASM builds; see the [browser build guide](cmd/wasm_browser/README.md).

Workload-specific measurements cover [SQL execution](docs/standard-query-performance.md),
[CTEs](docs/cte-select-performance.md), and [vector/RAG retrieval](docs/rag-serving-performance.md).

## Develop

```bash
go test ./...
go vet ./...
make docs-check
make test-bindings   # C ABI, Python and Rust
```

[Development](docs/development-guide.md) covers builds, tests, and benchmarks;
[architecture](docs/architecture.md) and [API stability](docs/api-stability.md)
describe internals and compatibility guarantees.

## Limitations

- Use one writer process per database. [Server replicas](docs/cluster.md) are
  asynchronous; sharding, automatic failover, multi-primary writes, and
  distributed transactions are not supported.
- Composite PRIMARY/UNIQUE/FOREIGN KEY, CHECK, target-bearing ON CONFLICT DO UPDATE,
  SAVEPOINT, ATTACH/DETACH, VACUUM, partial indexes, generated columns, and
  data-modifying CTE bodies are not supported.
- Secondary indexes accelerate equality/prefix seeks and numeric ranges. Use
  GEO_SEARCH for indexed point-column bbox/radius queries; ordinary
  WHERE GEO_DWITHIN(...) predicates are not planner-accelerated.
- GIS validity is structural rather than full topology validation.
  GEO_DISSOLVE/GEO_UNION_AGG require clean, vertex-aligned adjacent polygons;
  they are not general polygon-boolean union operations.
- RBAC is coarse and single-table oriented. Encryption does not cover
  WAL-backed modes or metadata files.

See [SQL feature gaps](docs/sql-feature-gaps.md) for details and current coverage.
