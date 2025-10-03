# tinySQL File Query Tool - SQL Editor

Query CSV, JSON, and XML files using SQL syntax with a modern web interface powered by tinySQL engine.

## Features

- **Modern Web Interface**: Full-featured SQL editor with VS Code-inspired design
- **Multiple File Formats**: Support for CSV, JSON, and XML files
- **Real-time SQL Execution**: Execute queries instantly and see results
- **File Upload**: Drag and drop or upload files directly in the browser
- **Visual Table Browser**: Browse loaded tables with column information
- **Multiple Output Formats**: Table, JSON, and CSV output
- **CLI Support**: Command-line interface for scripting and automation
- **Full SQL Support**: WHERE, ORDER BY, GROUP BY, JOIN, aggregations, and more
- **Fast & Lightweight**: In-memory database with optional persistence

## Quick Start

### Build

```bash
go build -o sql-editor
```

### Start Web Interface

```bash
# Start web server on default port (8080)
./sql-editor -web

# Specify custom port and data directory
./sql-editor -web -port 8090 -datadir ./data

# Pre-load files on startup
./sql-editor -web -datadir . users.csv orders.json
```

Then open your browser to **http://localhost:8080** (or your specified port)

### Web Interface Features

- **SQL Editor**: Write and execute SQL queries with syntax highlighting
- **Table Browser**: View all loaded tables with column names and row counts
- **File Upload**: Upload CSV, JSON, or XML files directly
- **Query Examples**: Pre-built query examples to get started
- **Keyboard Shortcuts**: 
  - `Ctrl/Cmd + Enter`: Execute query
  - `Tab`: Insert 2 spaces for indentation
- **Results Display**: Beautiful table view with type-aware formatting

## Usage Examples

### Web Mode (Recommended)

Start the server and use the web interface:
```bash
./sql-editor -web -port 8090 -datadir ./mydata
```

### Command Line Mode

#### Basic Query
```bash
./sql-editor -query "SELECT * FROM users WHERE age > 25" users.csv
```

#### Multiple Files with JOIN
```bash
./sql-editor -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" users.csv orders.json
```

#### Different Output Formats
```bash
# JSON output
./sql-editor -query "SELECT name, age FROM users" -output json users.csv

# CSV output  
./sql-editor -query "SELECT * FROM users ORDER BY age" -output csv users.json

# Table output (default)
./sql-editor -query "SELECT name, COUNT(*) as count FROM users GROUP BY name" users.xml
```

### Interactive Mode
```bash
./sql-editor -interactive data/
```

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

-- Average calculation
SELECT AVG(age) as avg_age FROM users

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

### Joins (if loading multiple tables)
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

```
Usage:
  ./query_files [options] file1 [file2 ...]

Options:
  -web                Start web server mode (recommended)
  -port int          Web server port (default: 8080)
  -datadir string    Data directory for web server (default: ".")
  -query string      SQL query to execute (CLI mode)
  -table string      Table name (default: filename without extension)
  -delimiter string  CSV delimiter (default: ",")
  -interactive       Run in interactive terminal mode
  -verbose           Verbose output
  -output string     Output format: table, json, csv (default: "table")
```

### Features Demo
- Load CSV, JSON, and XML files
- Execute complex SQL queries
- View results with proper formatting
- Export results to different formats

## File Format Support

### CSV Files
- Automatic header detection
- Type inference (int, float, bool, text)
- Custom delimiter support
- Empty value handling

### JSON Files
- Array of objects
- Single object
- Nested structure flattening
- Flexible schema

### XML Files
- Automatic record detection
- Attribute extraction
- Content parsing
- Multi-level structures

## Contributing

This tool is built on top of [tinySQL](https://github.com/SimonWaldherr/tinySQL) by Simon Waldherr.

---

**Made with ❤️ using Go and tinySQL**

### Custom Options
```bash
# Custom CSV delimiter
./query_files -query "SELECT * FROM data" -delimiter ";" data.csv

# Custom table name
./query_files -query "SELECT * FROM mytable" -table mytable data.csv

# Verbose output
./query_files -query "SELECT * FROM users" -verbose users.csv
```

## File Format Support

### CSV Files
- Automatic header detection
- Configurable delimiter
- Type inference for columns

### JSON Files
- Array of objects format
- Nested object flattening
- Flexible schema handling

### XML Files
- Automatic structure detection
- Element-to-column mapping
- Attribute support

## Examples

### Sample Data Files

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

### Sample Queries

```bash
# Basic filtering
./query_files -query "SELECT name, age FROM users WHERE age > 25" users.csv

# Aggregation
./query_files -query "SELECT city, COUNT(*) as population FROM users GROUP BY city" users.csv

# Ordering
./query_files -query "SELECT * FROM users ORDER BY age DESC" users.csv

# Pattern matching
./query_files -query "SELECT * FROM users WHERE name LIKE 'A%'" users.csv

# Joins across file formats
./query_files -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.name = o.customer" users.csv orders.json

# Complex aggregations
./query_files -query "SELECT category, AVG(price) as avg_price FROM products GROUP BY category HAVING avg_price > 100" products.xml
```

## Advanced Features

### Type Inference
The tool automatically detects column types:
- Integers (INT)
- Floating-point numbers (FLOAT)
- Booleans (BOOL)
- Text/Strings (TEXT)
- JSON objects (JSON)

### Interactive Mode
In interactive mode, you can:
- Load multiple files
- Execute multiple queries
- See available tables and columns
- Get query execution statistics

### Output Formats
- **table**: Human-readable table format (default)
- **json**: JSON array format
- **csv**: CSV format with headers

## Building

```bash
go build -o query_files ./cmd/query_files
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-query` | SQL query to execute | Required |
| `-output` | Output format (table, json, csv) | table |
| `-delimiter` | CSV delimiter character | `,` |
| `-table` | Custom table name | filename without extension |
| `-interactive` | Run in interactive mode | false |
| `-verbose` | Enable verbose output | false |

## Error Handling

The tool provides clear error messages for:
- File format issues
- SQL syntax errors
- Type conversion problems
- Missing files or permissions

## Performance

- Efficient in-memory processing
- Optimized for files up to several GB
- Streaming support for large files
- Query optimization through tinySQL engine

## Examples in Practice

### Data Analysis
```bash
# Find top customers by total orders
./query_files -query "SELECT customer, SUM(amount) as total FROM orders GROUP BY customer ORDER BY total DESC LIMIT 5" orders.csv

# Compare sales by region
./query_files -query "SELECT region, AVG(sales) as avg_sales FROM sales_data WHERE date >= '2023-01-01' GROUP BY region" sales.json
```

### Data Transformation
```bash
# Convert CSV to JSON
./query_files -query "SELECT * FROM users" -output json users.csv > users_output.json

# Filter and export specific columns
./query_files -query "SELECT name, email FROM contacts WHERE active = true" -output csv contacts.xml > active_contacts.csv
```

This tool demonstrates the power and flexibility of the tinySQL engine for real-world data processing tasks.