# tinySQL Data Migration Tool

A smart CLI tool for data pipelines and processing. Transfer data between CSV/JSON files, tinySQL, and external databases (MySQL/MariaDB, PostgreSQL, SQLite, MS SQL Server). Uses tinySQL as a central query hub with cross-database transfer support.

## Features

- **Web Interface**: Browser-based SQL editor with dark theme, file upload, connection management, and data export — all embedded in a single binary
- **File Import/Export**: Load CSV and JSON files into tinySQL, export query results to files
- **External Database Connectivity**: Connect to MySQL/MariaDB, PostgreSQL, SQLite, and MS SQL Server
- **Cross-Database Transfers**: Move data between any combination of files and databases
- **Inter-Database Queries**: `COPY SELECT ... INTO <connection>.<table>` syntax for routing query results to external databases
- **Interactive REPL**: Named connection management, ad-hoc queries, live data exploration
- **Pipeline Scripts**: Run multi-step migration workflows from script files
- **Fuzzy Import**: Intelligent parsing for malformed CSV/JSON with auto-detection of delimiters, type inference, and error recovery
- **Single Binary Deployment**: All HTML, CSS, and JS are embedded — no external files needed

## Quick Start

### Build

```bash
# Using Make
make build-migrate

# Or directly with Go
cd cmd/migrate && go build -o migrate .
```

### Import a CSV and Query It

```bash
migrate import-file -file users.csv -query "SELECT * FROM users WHERE age > 25"
```

### Export Query Results to JSON

```bash
migrate import-file -file users.csv -query "SELECT name, email FROM users" -output results.json -format json
```

### Interactive Mode

```bash
migrate interactive
```

### Web Interface

```bash
# Start web server on port 8080
migrate web

# Custom port and pre-load files
migrate web -addr :3000 -files users.csv,orders.json
```

Then open **http://localhost:8080** in your browser.

## Commands

| Command | Description |
|---------|-------------|
| `web` | Start web interface for data migration |
| `interactive` | Start interactive REPL for data migration |
| `import-file` | Import a CSV/JSON file into tinySQL |
| `import-db` | Import data from an external database into tinySQL |
| `export-file` | Export tinySQL data to a CSV/JSON file |
| `export-db` | Export tinySQL data to an external database |
| `pipeline` | Run a multi-step migration pipeline from a script |
| `help` | Show help message |

## Command Reference

### web

Start the web interface — a browser-based SQL editor with file upload, database connection management, and data export. All assets are embedded in the binary for single-file deployment.

```bash
migrate web [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | Listen address (host:port) | `:8080` |
| `-files` | Comma-separated files to pre-load | |
| `-verbose` | Verbose logging | `false` |

**API Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI (HTML/CSS/JS) |
| `/api/query` | POST | Execute SQL query |
| `/api/tables` | GET | List loaded tables |
| `/api/connections` | GET | List active connections |
| `/api/connect` | POST | Register external database |
| `/api/disconnect` | POST | Close a connection |
| `/api/import-file` | POST | Upload and import a file |
| `/api/import-db` | POST | Import from external database |
| `/api/export` | POST | Export query results (JSON/CSV) |

**Examples:**

```bash
# Start with pre-loaded data
migrate web -files data/users.csv,data/orders.json

# Custom port
migrate web -addr :3000

# Bind to all interfaces (for Docker / remote access)
migrate web -addr 0.0.0.0:8080
```

### import-file

Import a CSV or JSON file into tinySQL, optionally query and output results.

```bash
migrate import-file [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-file` | Path to CSV/JSON file (or positional arg) | Required |
| `-table` | Target table name | Filename without extension |
| `-query` | SQL query to execute after import | |
| `-output` | Output file for query results | stdout |
| `-format` | Output format: `table`, `json`, `csv` | `table` |
| `-fuzzy` | Enable fuzzy import for malformed files | `true` |
| `-verbose` | Verbose output | `false` |

**Examples:**

```bash
# Simple import and query
migrate import-file -file users.csv -query "SELECT * FROM users WHERE age > 25"

# Import with custom table name, export as JSON
migrate import-file -file data.csv -table customers -query "SELECT * FROM customers" -output out.json -format json

# Positional file argument
migrate import-file users.csv
```

### import-db

Import data from an external database into tinySQL.

```bash
migrate import-db [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-dsn` | Database connection string | Required |
| `-query` | SQL query to run on source database | |
| `-source-table` | Source table name (alternative to `-query`) | |
| `-table` | Target table name in tinySQL | Source table name or `imported` |
| `-tinyquery` | SQL query to run on tinySQL after import | |
| `-output` | Output file for query results | stdout |
| `-format` | Output format: `table`, `json`, `csv` | `table` |
| `-verbose` | Verbose output | `false` |

**Examples:**

```bash
# Import a table from PostgreSQL
migrate import-db -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" -source-table users -table users

# Import with a custom query
migrate import-db -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" \
  -query "SELECT * FROM users WHERE active = true" -table active_users

# Import and then query in tinySQL
migrate import-db -dsn "sqlite://data.db" -source-table orders -table orders \
  -tinyquery "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer"
```

### export-file

Export tinySQL data to a CSV or JSON file.

```bash
migrate export-file [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-files` | Comma-separated input files to load first | |
| `-query` | SQL query to select data for export | |
| `-table` | Table name to export (all rows) | |
| `-output` | Output file path | Required |
| `-format` | Output format: `csv`, `json` (auto-detected) | Auto from extension |
| `-verbose` | Verbose output | `false` |

**Examples:**

```bash
# Load CSV, filter, export to JSON
migrate export-file -files users.csv -query "SELECT * FROM users WHERE age > 30" -output filtered.json

# Load multiple files, join, export
migrate export-file -files users.csv,orders.csv \
  -query "SELECT u.name, COUNT(o.id) AS order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name" \
  -output summary.csv
```

### export-db

Export tinySQL data to an external database.

```bash
migrate export-db [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-dsn` | Target database connection string | Required |
| `-files` | Comma-separated input files to load first | |
| `-query` | SQL query to select data for export | |
| `-table` | Source table name in tinySQL | |
| `-target` | Target table name in external database | Source table name or `exported` |
| `-create` | Create target table if it doesn't exist | `true` |
| `-verbose` | Verbose output | `false` |

**Examples:**

```bash
# Load CSV and export to SQLite
migrate export-db -dsn "sqlite://output.db" -files users.csv -table users -target users_backup

# Export with a query
migrate export-db -dsn "mysql://user:pass@tcp(localhost:3306)/mydb" \
  -files data.csv -query "SELECT * FROM data WHERE status = 'active'" -target active_records
```

### pipeline

Run a multi-step migration workflow from a script file.

```bash
migrate pipeline -script migration.sql
```

| Flag | Description | Default |
|------|-------------|---------|
| `-script` | Path to pipeline script (or positional arg) | Required |
| `-verbose` | Verbose output | `false` |

**Pipeline Script Format:**

```sql
-- Comments start with -- or #
# This is also a comment

-- Load files
load data/users.csv AS users
load data/orders.json AS orders

-- Connect to external databases
connect pg postgres://user:pass@localhost/mydb?sslmode=disable
connect mysql mysql://user:pass@tcp(localhost:3306)/dest

-- Import from external database
import pg "SELECT * FROM products WHERE active = true" AS products

-- Run SQL queries in tinySQL
CREATE TABLE summary (name TEXT, total FLOAT)
INSERT INTO summary SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name

-- Export results
export mysql summary
COPY SELECT * FROM summary WHERE total > 100 INTO mysql.high_value_customers
```

### interactive

Start an interactive REPL session for data migration.

```bash
migrate interactive
```

**Interactive Commands:**

| Command | Description |
|---------|-------------|
| `connect <name> <dsn>` | Register an external database connection |
| `disconnect <name>` | Close a connection |
| `connections` | List active connections |
| `load <file> [AS <table>]` | Load a CSV/JSON file into tinySQL |
| `import <conn> <table>` | Import a table from an external database |
| `import <conn> "<query>" AS <table>` | Import query results as a table |
| `export file <query> TO <file>` | Export query results to a file |
| `export <conn> <table> [AS <target>]` | Export a tinySQL table to an external database |
| `COPY SELECT ... INTO <conn>.<table>` | Cross-database query and transfer |
| `tables` | Show loaded tinySQL tables |
| `help` | Show help |
| `exit` | Exit the program |

**Example Session:**

```
migrate> load users.csv
✓ Loaded users.csv as table 'users' (42µs)

migrate> SELECT * FROM users WHERE age > 25
name   age  email
─────  ───  ─────────────────
Alice  30   alice@example.com
Carol  28   carol@example.com

(2 rows in 15µs)

migrate> connect mydb sqlite://output.db
✓ Connected 'mydb' (sqlite)

migrate> COPY SELECT * FROM users WHERE age > 25 INTO mydb.senior_users
✓ Copied 2 rows to mydb.senior_users (3ms)

migrate> exit
Goodbye!
```

## DSN Formats

The tool auto-detects the database driver from the DSN URI scheme:

| Database | DSN Format | Example |
|----------|------------|---------|
| PostgreSQL | `postgres://user:pass@host:port/dbname?opts` | `postgres://admin:secret@localhost:5432/mydb?sslmode=disable` |
| MySQL/MariaDB | `mysql://user:pass@tcp(host:port)/dbname` | `mysql://root:pass@tcp(localhost:3306)/mydb` |
| SQLite | `sqlite://path/to/file.db` | `sqlite:///tmp/data.db` |
| MS SQL Server | `sqlserver://user:pass@host:port?database=name` | `sqlserver://sa:pass@localhost:1433?database=mydb` |

Auto-detection also works for common patterns without explicit scheme prefixes:
- Paths ending in `.db`, `.sqlite`, `.sqlite3` → SQLite
- DSNs containing `tcp(` or `@/` → MySQL
- DSNs containing `sslmode=` or `host=` → PostgreSQL

## Supported File Formats

| Format | Extensions | Features |
|--------|-----------|----------|
| CSV | `.csv`, `.tsv`, `.txt` | Auto-delimiter detection, header inference, type coercion |
| JSON | `.json`, `.jsonl`, `.ndjson` | Array of objects, line-delimited JSON, nested structures |

Fuzzy import (enabled by default) handles:
- Inconsistent column counts
- Unmatched quotes
- Numbers with thousand separators
- Mixed-type columns
- Invalid UTF-8 characters

## Building

```bash
# Build with Make
make build-migrate

# Build directly
cd cmd/migrate && go build -o ../../bin/migrate .

# Run tests
cd cmd/migrate && go test -v ./...
```

## Architecture

The tool uses tinySQL as a central in-memory SQL engine:

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  CSV/JSON   │────▶│              │────▶│  CSV/JSON    │
│  Files      │     │              │     │  Files       │
└─────────────┘     │              │     └──────────────┘
                    │   tinySQL    │
┌─────────────┐     │   Engine     │     ┌──────────────┐
│  PostgreSQL │────▶│              │────▶│  MySQL       │
│  SQLite     │     │  (in-memory  │     │  MS SQL      │
│  MySQL      │     │   SQL hub)   │     │  PostgreSQL  │
│  MS SQL     │────▶│              │────▶│  SQLite      │
└─────────────┘     └──────────────┘     └──────────────┘
```

Data flows through tinySQL where it can be queried, joined, filtered, and aggregated using standard SQL before being routed to any output target.

## Contributing

This tool is part of the [tinySQL](https://github.com/SimonWaldherr/tinySQL) project by Simon Waldherr.
