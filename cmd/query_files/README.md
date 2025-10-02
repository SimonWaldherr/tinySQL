# tinySQL File Query Tool

Query CSV, JSON, and XML files using SQL syntax with the power of tinySQL engine.

## Quick Start

Run the demo to see the tool in action:
```bash
./demo.sh          # Basic examples
./advanced_demo.sh  # Comprehensive feature demonstration
```

## Features

- **Multiple File Formats**: Support for CSV, JSON, and XML files
- **SQL Querying**: Full SQL syntax support including WHERE, ORDER BY, GROUP BY, JOIN
- **Type Inference**: Automatic detection of column types from data
- **Multiple Output Formats**: Table, JSON, and CSV output
- **Interactive Mode**: Query multiple files interactively
- **Flexible Table Naming**: Automatic table names from filenames

## Usage

### Basic Query
```bash
./query_files -query "SELECT * FROM users WHERE age > 25" users.csv
```

### Multiple Files with JOIN
```bash
./query_files -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" users.csv orders.json
```

### Different Output Formats
```bash
# JSON output
./query_files -query "SELECT name, age FROM users" -output json users.csv

# CSV output  
./query_files -query "SELECT * FROM users ORDER BY age" -output csv users.json

# Table output (default)
./query_files -query "SELECT name, COUNT(*) as count FROM users GROUP BY name" users.xml
```

### Interactive Mode
```bash
./query_files -interactive data/
```

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