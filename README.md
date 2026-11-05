# tinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SimonWaldherr/tinySQL.svg)](https://pkg.go.dev/github.com/SimonWaldherr/tinySQL)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)

tinySQL brings an embeddable SQL engine to Go: useful for local tools, tests,
browser applications, and learning how databases work. Run it in-process, take
it to WebAssembly, or explore its parser, executor, and storage backends.

[Browser playground](https://simonwaldherr.github.io/tinySQL/) ·
[Map demo](https://simonwaldherr.github.io/tinySQL/tiles-demo.html) ·
[Video](https://youtu.be/W28-aBk3BL0) ·
[Documentation](docs/README.md)

> tinySQL is not a drop-in replacement for PostgreSQL, MySQL, or a clustered
> database. Review the [limitations](#limitations) for critical workloads.

## Get started

Requires Go **1.27.3+**:

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

Create an in-memory database, insert data, and query it with a bound parameter:

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
[developer integration](docs/developer-integration.md). Rust, Python, Swift,
and C are supported through the [language bindings](docs/language-bindings.md).

## Explore

- **SQL and data:** joins, CTEs, windows, views, triggers, procedures, and
  imports from common document, tabular, and geospatial formats. Browse the
  [function reference](docs/reference/functions.html) and [SQL showcase](demos/sql/showcase.sql).
- **Search and maps:** full-text, vector, and hybrid retrieval; geometry,
  routing, and tiles. See the [RAG](docs/rag-guide.md),
  [geospatial](docs/geospatial-standards.md), and [routing](docs/osm-routing.md) guides.
- **Tools and services:** try the [CLI](docs/cli-guide.md),
  [file query tools](cmd/query_files/README.md),
  [server](cmd/server/README.md), or [browser build](cmd/query_files_wasm/README.md).
  The [command index](cmd/README.md) lists the rest.
- **Storage:** choose in-memory, WAL, JSON, disk, indexed, or SQLite-backed
  storage. The [storage guide](docs/storage-guide.md) covers configuration,
  persistence, backups, and trade-offs. For column-oriented data exports, see
  [analytical snapshots](docs/analytical-snapshots.md).

SQLite, GeoPackage, and MBTiles support requires the `sqliteimport` build tag;
Shapefile support requires `shapefile`. For smaller WASM builds,
`tinysql_minimal` omits optional features; see
[browser build guide](cmd/wasm_browser/README.md) for details.

## Develop

```bash
go test ./...
go vet ./...
make test-bindings   # C ABI, Python and Rust
```

See the [development guide](docs/development-guide.md) for tests, benchmarks,
and build targets; the [architecture guide](docs/architecture.md) for internals;
and [API stability](docs/api-stability.md) for compatibility guarantees.

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

See [SQL feature gaps](docs/sql-feature-gaps.md) for details and current coverage.
