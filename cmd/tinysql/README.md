# tinySQL CLI (`tinysql`)

Part of [tinySQL](../../README.md). See the root guide for SQL, GIS, storage,
and import capabilities.

SQLite-style CLI for tinySQL: REPL, inline SQL, piped scripts,
`sqlite-utils`-style subcommands.

## Build

```bash
go build -o tinysql ./cmd/tinysql

# Adds local SQLite, OGC GeoPackage, and MBTiles file support.
go build -tags=sqliteimport -o tinysql ./cmd/tinysql
```

## Usage

```
tinysql [FLAGS] [<database>] [SQL...]
tinysql <subcommand> [subcommand-flags] <database> [args...]
```

`<database>` is a file path or `:memory:` (default). A file-backed database is
created if missing and saved on exit and after each dirty statement.
Flags must come **before** `<database>` and before trailing SQL: Go's standard
flag parser stops at the first positional argument. Subcommands are matched on
the first argument, so they too must precede their flags.

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-tenant` | Tenant/schema name | `default` |
| `-mode` | Output mode: `column`, `list`, `csv`, `json`, `jsonl`/`ndjson`, `table`, `geojson`, `topojson` | `column` |
| `-header` | Include column headers | `true` |
| `-echo` | Echo SQL before execution | `false` |
| `-cmd` | Run this SQL and exit | — |
| `-batch` | Force batch mode (error if no SQL) | `false` |
| `-output` | Write output to this file, not stdout | — |
| `-geom-col` | Geometry column for `-mode geojson\|topojson` | auto-detected |
| `-version` | Print the build version and exit | — |
| `-storage` | Backend: `auto`, `memory`, `wal`, `disk`, `json`, `index`, `hybrid`, `advanced_wal`, `paged_index`, `sqlite` | `auto` |
| `-read-only` | Reject mutating statements | `false` |
| `-memory-limit` | Cache limit for `index`/`hybrid`, e.g. `256MiB` | backend default |
| `-wal-sync` | WAL durability: `full` or `normal` | `full` |
| `-sync-on-mutate` | Flush durable storage after every mutation | `false` |
| `-compress` | Compress disk-backed tables/checkpoints | `false` |

SQL source precedence: `-cmd`, then trailing positional SQL, then stdin when
piped. With none of those and no `-batch`, the REPL starts.

`-storage auto` preserves the original single-file snapshot workflow.
Selecting a concrete storage backend uses the package's `OpenDB` lifecycle;
callers get its durability and cache settings, and the CLI flushes it on exit.

`tinysql shell [FLAGS] [DATABASE]` (also `tinysql repl`) starts the integrated
interactive shell. The separate `cmd/repl` binary remains available for
existing automation, but new scripts and documentation should prefer the main
`tinysql` tool.

## Streaming output

For `SELECT` statements, `-mode list`, `csv`, `json`, `jsonl`, and `ndjson`
consume the engine stream directly. `jsonl` and `ndjson` emit one complete JSON
object per line, which is ideal for pipes and long-running exports. `json`
emits one valid JSON array while rows arrive. `column`, `table`, `geojson`, and
`topojson` retain materialized output because they need whole-result layout or
topology construction.

The engine preserves exact semantics: queries requiring a global result first
(for example `ORDER BY`, `GROUP BY`, joins, or `DISTINCT`) begin output after
their result is materialized. Ctrl+C cancels an in-flight batch or REPL query;
in the REPL it returns to the prompt rather than terminating a goroutine.

## REPL

Prompt is `tinysql> ` (`   ...> ` while a statement is incomplete). Ctrl+C
discards a partial statement; on an empty buffer it exits. While a query is
running, Ctrl+C cancels that query and returns to the prompt.

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.exit` / `.quit` | Exit |
| `.tables` | List table names |
| `.schema ?TABLE?` | Show CREATE statements |
| `.mode MODE` | Set output mode (no arg: print current) |
| `.headers on\|off` | Toggle header display |
| `.timer on\|off` | Toggle SQL timer (no arg: print state) |
| `.nullvalue STRING` | Print STRING for NULL values |
| `.read FILENAME` | Execute SQL in FILENAME |
| `.save FILENAME` | Write in-memory database to FILENAME |
| `.dump [TABLE...]` | Dump tables as INSERT statements |
| `.import FILE [TABLE]` | Import a file into a table (CSV, JSON, GeoJSON, TopoJSON, KML, OSM XML, GeoPackage, MBTiles, ...) |
| `.count [TABLE...]` | Show row counts |
| `.stats` | Database statistics |

## Subcommands

Subcommand flags must precede the database argument.

```bash
./tinysql tables mydb.dat  # -tenant NAME, -json
./tinysql schema mydb.dat  # CREATE statements; -tenant NAME
./tinysql query -mode csv mydb.dat "SELECT * FROM users"  # -tenant, -mode (default table)
./tinysql insert mydb.dat users '{"id":1,"name":"Alice"}' # JSON rows; -tenant NAME
```

## Examples

```bash
./tinysql --version
./tinysql shell -storage disk data/
cat setup.sql | ./tinysql mydb.dat
./tinysql -cmd "INSERT INTO log VALUES (NOW(), 'ping')" mydb.dat
./tinysql -output report.txt -mode json mydb.dat "SELECT * FROM sales"
./tinysql -mode jsonl -storage hybrid -memory-limit 256MiB data/ "SELECT * FROM sales"
```

## Geospatial interoperability

CRS identifiers, WMS bounding boxes, and TileMatrix coordinates can be
calculated directly in SQL:

```sql
SELECT CRS_NORMALIZE('urn:ogc:def:crs:EPSG::25832'),
       CRS_AXIS_ORDER('http://www.opengis.net/def/crs/EPSG/0/3035');

SELECT WMS_BBOX(11, 48, 12, 49, 'EPSG:4326', '1.3.0'),
       TILE_MATRIX_BBOX(100000, 500000, 10, 256, 256, 2, 3, 'topLeft');
```

`.import features.gpkg features` imports a single-layer OGC GeoPackage in a
`sqliteimport` build. Use the public Go importer when a package has multiple
feature layers and select one with `GeoPackageLayer`. Projected geometry is
kept as GeoPackageBinary by default instead of being mislabeled as GeoJSON.
See the [standards guide](../../docs/geospatial-standards.md) for supported
profiles and format boundaries.
