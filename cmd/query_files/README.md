# tinySQL File Query Tool

Query CSV, JSON, and XML files using SQL syntax from the command line or an interactive shell, powered by the tinySQL engine.

## Features

- **Multiple File Formats**: CSV (with auto-delimiter detection), JSON, and XML
- **Multiple Output Formats**: Table, JSON, and CSV
- **CLI Mode**: One-shot query execution for scripting and automation
- **Interactive Mode**: Live SQL shell with table listing and execution statistics
- **Parallel Loading**: Load multiple files concurrently with `-parallel`
- **Fuzzy Import**: Tolerant parsing for malformed or inconsistent files
- **Query Caching**: Optional result caching for repeated queries
- **Full SQL Support**: WHERE, ORDER BY, GROUP BY, JOIN, aggregations, and more

## Quick Start

### Build

```bash
go build -o query_files .
```

### Run a query

```bash
./query_files -query "SELECT * FROM users LIMIT 10" users.csv
```

### Interactive shell

```bash
./query_files -interactive data/
```

## Usage Examples

### Command Line Mode

#### Basic Query
```bash
./query_files -query "SELECT * FROM users WHERE age > 25" users.csv
```

#### Multiple Files with JOIN
```bash
./query_files -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" users.csv orders.json
```

#### Different Output Formats
```bash
# JSON output
./query_files -query "SELECT name, age FROM users" -output json users.csv

# CSV output
./query_files -query "SELECT * FROM users ORDER BY age" -output csv users.json

# Table output (default)
./query_files -query "SELECT name, COUNT(*) as count FROM users GROUP BY name" users.xml
```

#### Parallel loading of many files
```bash
./query_files -parallel -workers 8 \
  -query "SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name" \
  users.csv orders.json
```

### Interactive Mode
```bash
./query_files -interactive ./data
```

In interactive mode you can run multiple queries, list loaded tables, and see timing statistics.

## SQL Query Examples

### Basic Queries
```sql
-- Select all data
SELECT * FROM users LIMIT 10

-- Filter with WHERE
SELECT name, age FROM users WHERE age > 25

-- Order results
SELECT * FROM users ORDER BY age DESC

-- Count records
SELECT COUNT(*) as total FROM users
```

### Aggregations
```sql
-- Group by with count
SELECT city, COUNT(*) as count FROM users GROUP BY city

-- Multiple aggregates
SELECT
  city,
  COUNT(*) as total,
  AVG(age) as avg_age,
  MIN(age) as min_age,
  MAX(age) as max_age
FROM users
GROUP BY city
```

### Joins (multiple files)
```sql
-- Inner join
SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id

-- Left join with aggregation
SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name
```

## Command Line Options

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
| `-cache-size` | Query cache capacity | `256` |
| `-parallel` | Load input files concurrently | `false` |
| `-workers` | Number of parallel load workers | `4` |
| `-query-timeout` | Per-query timeout (`0` = no timeout) | `30s` |

## File Format Support

### CSV Files
- Automatic header detection
- Auto-delimiter detection (comma, semicolon, tab, pipe) or explicit override
- Type inference (INT, FLOAT, BOOL, TEXT)
- Fuzzy mode for inconsistent or malformed files

### JSON Files
- Array-of-objects format
- Single-object format
- Flexible schema handling

### XML Files
- Automatic record detection
- Element-to-column mapping
- Attribute support

## Sample Data Files

**users.csv**
```csv
name,age,city
John,25,New York
Alice,30,Paris
Bob,22,London
```

**orders.json**
```json
[
  {"id": 1, "user_id": 1, "amount": 100.50},
  {"id": 2, "user_id": 2, "amount": 75.25}
]
```

**products.xml**
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

## Practical Examples

### Data Analysis
```bash
# Top customers by total spend
./query_files -query "SELECT customer, SUM(amount) as total FROM orders GROUP BY customer ORDER BY total DESC LIMIT 5" orders.csv

# Sales by region
./query_files -query "SELECT region, AVG(sales) as avg_sales FROM sales_data GROUP BY region" sales.json
```

### Data Transformation
```bash
# Convert CSV to JSON
./query_files -query "SELECT * FROM users" -output json users.csv > users_output.json

# Filter and export specific columns
./query_files -query "SELECT name, email FROM contacts WHERE active = true" -output csv contacts.xml > active_contacts.csv
```

### Custom Delimiter
```bash
./query_files -query "SELECT * FROM data" -delimiter ";" data.csv
```

## Error Handling

Clear error messages are provided for:
- Unsupported file formats
- SQL syntax errors
- Type conversion issues
- Missing files or insufficient permissions
