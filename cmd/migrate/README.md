# tinySQL Data Migration Tool

Part of [tinySQL](../../README.md). migrate moves structured files and external
databases through an in-memory tinySQL workspace, where you can query, join,
filter, and aggregate data before exporting it.

It supports CSV/TSV, JSON/NDJSON, YAML, XML, GeoJSON/TopoJSON, KML, OSM XML,
routing graphs, and optional GeoPackage, MBTiles, and Shapefile formats. External
database support covers MySQL/MariaDB, PostgreSQL, SQLite, and SQL Server.

## Build

```bash
make build-migrate
# or
cd cmd/migrate
go build -o ../../bin/migrate .
go test ./...

# SQLite, GeoPackage, and MBTiles support
go build -tags=sqliteimport -o ../../bin/migrate .
```

Run migrate help or migrate <command> -h for the complete current flag list.

## Commands

| Command | Purpose |
| --- | --- |
| web | Browser workspace for files, SQL, connections, and export |
| interactive or repl | Interactive migration shell |
| import-file | Load a file into tinySQL and optionally query it |
| import-db | Copy an external table or query into tinySQL |
| export-file | Write a tinySQL table or query as CSV or JSON |
| export-db | Write a tinySQL table or query to an external database |
| pipeline | Run a file of load, connect, import, SQL, and export steps |

## Files

Load one file and query it immediately:

```bash
migrate import-file -file users.csv \
  -query "SELECT * FROM users WHERE age > 25"

migrate import-file -file data.csv -table customers \
  -query "SELECT * FROM customers" -output out.json -format json

migrate export-file -files users.csv,orders.json \
  -query "SELECT u.name, COUNT(o.id) AS orders FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name" \
  -output summary.csv
```

The table name defaults to the input filename without its extension. CSV and
JSON import use fuzzy mode by default. It can repair common malformed input but
may skip up to 100 invalid rows; use -verbose to inspect warnings or
-fuzzy=false when every row must be accepted or rejected explicitly.

| Format | Extensions | Notes |
| --- | --- | --- |
| CSV | .csv, .tsv, .txt | Delimiter and header detection; fuzzy repair is available |
| JSON | .json, .jsonl, .ndjson | Objects, arrays, and line-delimited input |
| YAML / XML | .yaml, .yml, .xml | Structured importer |
| GIS | .geojson, .topojson, .kml, .osm | GeoJSON, topology, KML, and OSM XML |
| GeoPackage / MBTiles | .gpkg, .gpkx, .geopackage, .mbtiles | Requires sqliteimport |
| Shapefile | .shp, .zip | Requires the shapefile build tag |
| Routing graph | .rg, .routinggraph, .graph.json | Node/edge graph interchange |

A multi-layer GeoPackage requires the public Go importer with an explicit
GeoPackageLayer; the CLI will not choose a layer silently. Projected geometry
stays in its native GeoPackageBinary form unless a safe conversion is selected.
See the [geospatial standards guide](../../docs/geospatial-standards.md).

## External databases

```bash
migrate import-db \
  -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" \
  -source-table users -table users

migrate export-db \
  -dsn "sqlite:///tmp/output.db" \
  -files users.csv -table users -target users_backup
```

| Database | DSN example |
| --- | --- |
| PostgreSQL | `postgres://user:pass@localhost:5432/mydb?sslmode=disable` |
| MySQL/MariaDB | `mysql://user:pass@tcp(localhost:3306)/mydb` |
| SQLite | `sqlite:///tmp/data.db` |
| SQL Server | `sqlserver://user:pass@localhost:1433?database=mydb` |

Full-mode imports append rows, and full-mode exports append to the target. Check
the selected target before using a production database.

For delete-aware upsert synchronization, choose incremental mode, provide a
stable key, and persist the tinySQL workspace across runs:

```bash
migrate import-db \
  -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" \
  -source-table users -table users \
  -mode incremental -key-col id -db-file users.snapshot

migrate export-db \
  -dsn "postgres://user:pass@localhost/reporting?sslmode=disable" \
  -files users.csv -table users -target users \
  -mode incremental -key-col id
```

Incremental mode accepts a plain table, not a source query. It tracks keys,
row hashes, and an optional watermark in a durable state file. Use
-allow-hash-identity only when a full-row synthetic identity is suitable.

## Pipeline and interactive shell

```bash
migrate pipeline -script migration.sql
migrate interactive
```

A pipeline is one command per line; normal SQL is run against tinySQL:

```sql
load data/users.csv AS users
connect pg postgres://user:pass@localhost/mydb?sslmode=disable
import pg "SELECT * FROM products WHERE active = true" AS products
CREATE TABLE summary (name TEXT, total FLOAT)
INSERT INTO summary SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name
COPY SELECT * FROM summary INTO pg.customer_summary
```

The interactive shell supports the same load, connect, import, export, COPY,
tables, and SQL operations. Type help for its command list.

## Web workspace

```bash
migrate web -files data/users.csv,data/orders.json
# http://localhost:8080
```

The browser API provides POST /api/query, GET /api/tables,
GET /api/connections, POST /api/connect, POST /api/disconnect,
POST /api/import-file, POST /api/import-db, and POST /api/export.

The web mode listens on all interfaces by default and has no authentication or
TLS. Bind it only to a trusted local interface or place it behind appropriate
authentication and HTTPS before accepting remote traffic.
