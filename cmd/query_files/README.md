# tinySQL File Query Tool

Part of [tinySQL](../../README.md). See the root guide for SQL features,
import/export behavior, and project limitations.

Query CSV, JSON, and XML files with SQL, either one-shot from the command line
or in an interactive shell.

## Build

```bash
go build -o query_files .
```

## Usage

```bash
# One-shot query
./query_files -query "SELECT * FROM users WHERE age > 25" users.csv

# Join across files and formats
./query_files -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" users.csv orders.json

# Output formats: table (default), json, csv
./query_files -query "SELECT name, age FROM users" -output json users.csv

# Explicit CSV delimiter
./query_files -query "SELECT * FROM data" -delimiter ";" data.csv

# Parallel loading of many files
./query_files -parallel -workers 8 \
  -query "SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name" \
  users.csv orders.json

# Interactive shell over a directory: multiple queries, table listing, timings
./query_files -interactive ./data
```

Table names default to the filename without extension, so a query over
`users.csv` selects `FROM users`. Redirect output to convert formats:

```bash
./query_files -query "SELECT * FROM users" -output json users.csv > users.json
```

The engine supports WHERE, ORDER BY, GROUP BY, JOIN (including LEFT JOIN), and
aggregations (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`):

```sql
SELECT city, COUNT(*) AS total, AVG(age) AS avg_age
FROM users
GROUP BY city
ORDER BY total DESC
```

## Command line options

| Flag | Description | Default |
|------|-------------|---------|
| `-query` | SQL query to execute (required in CLI mode) | — |
| `-output` | Output format: `table`, `json`, `csv` | `table` |
| `-delimiter` | CSV delimiter: `auto`, `comma`, `semicolon`, `tab`, `pipe`, or a single character | `auto` |
| `-table` | Custom table name (only valid with a single input file) | filename without extension |
| `-interactive` | Run in interactive terminal mode | `false` |
| `-verbose` | Print timing and statistics | `false` |
| `-fuzzy` | Tolerate malformed CSV/JSON files | `true` |
| `-cache` | Enable query result caching | `true` |
| `-cache-size` | Query cache capacity (ignored with `-cache=false`) | `256` |
| `-parallel` | Load input files concurrently | `false` |
| `-workers` | Number of parallel load workers (with `-parallel`) | `4` |
| `-query-timeout` | Per-query timeout (`0` = no timeout) | `30s` |

## File format support

- **CSV**: header detection, auto-delimiter detection (comma, semicolon, tab,
  pipe) or explicit override, type inference (INT, FLOAT, BOOL, TEXT), fuzzy
  mode for inconsistent files.
- **JSON**: array-of-objects or single object, flexible schema handling.
- **XML**: automatic record detection, element-to-column mapping, attribute
  support.

## Sample inputs

```csv
name,age,city
John,25,New York
Alice,30,Paris
Bob,22,London
```

```json
[
  {"id": 1, "user_id": 1, "amount": 100.50},
  {"id": 2, "user_id": 2, "amount": 75.25}
]
```

```xml
<?xml version="1.0"?>
<products>
  <product>
    <name>Laptop</name>
    <price>999.99</price>
    <category>Electronics</category>
  </product>
</products>
```

Errors are reported for unsupported file formats, SQL syntax errors, type
conversion failures, and missing or unreadable files.
