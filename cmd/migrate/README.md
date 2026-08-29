# tinySQL Data Migration Tool

Part of [tinySQL](../../README.md). See the root guide for supported import
formats, export behavior, and optional build tags.

CLI for data pipelines. Moves data between CSV/TSV/JSON/YAML/XML files,
tinySQL, and external databases (MySQL/MariaDB, PostgreSQL, SQLite, MS SQL
Server), using tinySQL as an in-memory hub: data can be queried, joined,
filtered, and aggregated with SQL before it is routed to any output target.
Includes a REPL, pipeline scripts, and a web UI. All HTML/CSS/JS is embedded,
so the binary is self-contained.

## Build

```bash
make build-migrate
# or
cd cmd/migrate && go build -o ../../bin/migrate .

cd cmd/migrate && go test ./...
```

## Commands

| Command | Description |
|---------|-------------|
| `web` | Start the web interface |
| `interactive` (alias `repl`) | Start the REPL |
| `import-file` | Import a CSV/TSV/JSON/YAML/XML file into tinySQL |
| `import-db` | Import from an external database into tinySQL |
| `export-file` | Export tinySQL data to a CSV/JSON file |
| `export-db` | Export tinySQL data to an external database |
| `pipeline` | Run a multi-step pipeline script |
| `help` | Show help |

### web

Browser-based SQL editor with file upload, connection management, and export.

```bash
migrate web [options]
migrate web -files data/users.csv,data/orders.json
migrate web -addr 0.0.0.0:8080   # bind all interfaces (Docker / remote)
```

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | Listen address (host:port) | `:8080` |
| `-files` | Comma-separated files to pre-load | |
| `-verbose` | Verbose logging | `false` |

Default URL: http://localhost:8080

API endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI |
| `/api/query` | POST | Execute SQL query |
| `/api/tables` | GET | List loaded tables |
| `/api/connections` | GET | List active connections |
| `/api/connect` | POST | Register external database |
| `/api/disconnect` | POST | Close a connection |
| `/api/import-file` | POST | Upload and import a file |
| `/api/import-db` | POST | Import from external database |
| `/api/export` | POST | Export query results (JSON/CSV) |

### import-file

```bash
migrate import-file -file users.csv -query "SELECT * FROM users WHERE age > 25"

# custom table name, JSON output to a file
migrate import-file -file data.csv -table customers \
  -query "SELECT * FROM customers" -output out.json -format json

migrate import-file users.csv          # positional file argument
migrate import-file -file users.yaml -table users
```

| Flag | Description | Default |
|------|-------------|---------|
| `-file` | Path to CSV/TSV/JSON/YAML/XML file (or positional arg) | Required |
| `-table` | Target table name | Filename without extension |
| `-query` | SQL query to run after import | |
| `-output` | Output file for query results | stdout |
| `-format` | Output format: `table`, `json`, `csv` | `table` |
| `-fuzzy` | Enable fuzzy import for malformed files | `true` |
| `-verbose` | Verbose output | `false` |

### import-db

```bash
migrate import-db -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" \
  -source-table users -table users

migrate import-db -dsn "sqlite://data.db" -source-table orders -table orders \
  -tinyquery "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer"
```

| Flag | Description | Default |
|------|-------------|---------|
| `-dsn` | Source database connection string | Required |
| `-query` | SQL query to run on the source database | |
| `-source-table` | Source table name (alternative to `-query`) | |
| `-table` | Target table name in tinySQL | Source table name or `imported` |
| `-tinyquery` | SQL query to run on tinySQL after import | |
| `-output` | Output file for query results | stdout |
| `-format` | Output format: `table`, `json`, `csv` | `table` |
| `-verbose` | Verbose output | `false` |

### export-file

```bash
migrate export-file -files users.csv,orders.csv \
  -query "SELECT u.name, COUNT(o.id) AS order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name" \
  -output summary.csv
```

| Flag | Description | Default |
|------|-------------|---------|
| `-files` | Comma-separated input files to load first | |
| `-query` | SQL query selecting the data to export | |
| `-table` | Table to export (all rows) | |
| `-output` | Output file path | Required |
| `-format` | Output format: `csv`, `json` | Auto from extension |
| `-verbose` | Verbose output | `false` |

### export-db

```bash
migrate export-db -dsn "sqlite://output.db" -files users.csv \
  -table users -target users_backup

migrate export-db -dsn "mysql://user:pass@tcp(localhost:3306)/mydb" \
  -files data.csv -query "SELECT * FROM data WHERE status = 'active'" \
  -target active_records
```

| Flag | Description | Default |
|------|-------------|---------|
| `-dsn` | Target database connection string | Required |
| `-files` | Comma-separated input files to load first | |
| `-query` | SQL query selecting the data to export | |
| `-table` | Source table name in tinySQL | |
| `-target` | Target table name in the external database | Source table name or `exported` |
| `-create` | Create the target table if it does not exist | `true` |
| `-verbose` | Verbose output | `false` |

Rows are appended to the target table; existing rows are not removed. Verify
the target name before running against a production database.

### pipeline

```bash
migrate pipeline -script migration.sql
```

| Flag | Description | Default |
|------|-------------|---------|
| `-script` | Path to pipeline script (or positional arg) | Required |
| `-verbose` | Verbose output | `false` |

Script format:

```sql
-- Comments start with -- or #
# also a comment

load data/users.csv AS users
load data/orders.json AS orders

connect pg postgres://user:pass@localhost/mydb?sslmode=disable
connect mysql mysql://user:pass@tcp(localhost:3306)/dest

import pg "SELECT * FROM products WHERE active = true" AS products

CREATE TABLE summary (name TEXT, total FLOAT)
INSERT INTO summary SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name

export mysql summary
COPY SELECT * FROM summary WHERE total > 100 INTO mysql.high_value_customers
```

### interactive

```bash
migrate interactive
```

| Command | Description |
|---------|-------------|
| `connect <name> <dsn>` | Register an external database connection |
| `disconnect <name>` | Close a connection |
| `connections` (or `show connections`) | List active connections |
| `load <file> [AS <table>]` | Load a file into tinySQL (alias `import file`) |
| `import <conn> <table>` | Import a table from an external database |
| `import <conn> "<query>" AS <table>` | Import query results as a table |
| `export file <query> TO <file>` | Export query results to a file |
| `export <conn> <table> [AS <target>]` | Export a tinySQL table to a database |
| `COPY SELECT ... INTO <conn>.<table>` | Cross-database query and transfer |
| `tables` (or `show tables`, `.tables`) | Show loaded tinySQL tables |
| `help` (or `h`) | Show help |
| `exit` (or `quit`, `q`) | Exit |

Anything else is executed as SQL against tinySQL.

```text
migrate> load users.csv
✓ Loaded users.csv as table 'users' (42µs)

migrate> connect mydb sqlite://output.db
✓ Connected 'mydb' (sqlite)

migrate> COPY SELECT * FROM users WHERE age > 25 INTO mydb.senior_users
✓ Copied 2 rows to mydb.senior_users (3ms)
```

## DSN formats

The driver is derived from the URI scheme:

| Database | DSN format |
|----------|------------|
| PostgreSQL | `postgres://admin:secret@localhost:5432/mydb?sslmode=disable` |
| MySQL/MariaDB | `mysql://root:pass@tcp(localhost:3306)/mydb` |
| SQLite | `sqlite:///tmp/data.db` |
| MS SQL Server | `sqlserver://sa:pass@localhost:1433?database=mydb` |

Without a scheme prefix, common patterns are auto-detected:

- Paths ending in `.db`, `.sqlite`, `.sqlite3` → SQLite
- DSNs containing `tcp(` or `@/` → MySQL
- DSNs containing `sslmode=` or `host=` → PostgreSQL

## File formats

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV | `.csv`, `.tsv`, `.txt` | Auto-delimiter detection, header inference, type coercion |
| JSON | `.json`, `.jsonl`, `.ndjson` | Array of objects, line-delimited JSON, nested structures |
| YAML | `.yaml`, `.yml` | Sequence of mappings or a single mapping |
| XML | `.xml` | Simple row-based XML; attributes or child elements become columns |

Fuzzy import (`-fuzzy`, default on) applies to CSV and JSON and recovers from
inconsistent column counts, unmatched quotes, numbers with thousand
separators, mixed-type columns, and invalid UTF-8. It skips rows it cannot
parse (up to 100), so an import can silently lose data — run with `-verbose`,
or with `-fuzzy=false` to fail loudly instead. YAML and XML always go through
the structured auto-importer.

Part of the [tinySQL](https://github.com/SimonWaldherr/tinySQL) project.
