# TinySQL

TinySQL is a lightweight, educational SQL database engine written in pure Go. It implements a subset of SQL features using only Go's standard library, making it perfect for learning database internals and for applications that need a simple embedded SQL database.

## Features

### Supported SQL Operations

#### Data Definition Language (DDL)
- `CREATE TABLE` - Create regular and temporary tables
- `CREATE TEMP TABLE ... AS SELECT` - Create temporary tables from query results
- `DROP TABLE` - Drop tables

#### Data Manipulation Language (DML)
- `INSERT` - Insert rows with explicit columns or all columns
- `UPDATE` - Update rows with WHERE conditions
- `DELETE` - Delete rows with WHERE conditions

#### Data Query Language (DQL)
- `SELECT` - Query data with comprehensive support for:
  - `*` wildcard and specific column selection
  - `WHERE` clauses with complex expressions
  - `JOIN` operations (INNER, LEFT OUTER, RIGHT OUTER)
  - `GROUP BY` and `HAVING` clauses
  - `ORDER BY` with ASC/DESC sorting
  - `LIMIT` and `OFFSET` for pagination
  - `DISTINCT` for unique results

### Data Types
- `INT` - Integer numbers
- `FLOAT` - Floating-point numbers
- `TEXT` - String values
- `BOOL` - Boolean values (true/false)
- `JSON` - JSON objects and arrays

### Advanced Features
- **Multi-tenancy** - Support for multiple tenants in a single database
- **NULL semantics** - Proper 3-valued logic with NULL support
- **Aggregate functions** - COUNT, SUM, AVG, MIN, MAX
- **Built-in functions**:
  - `COALESCE(val1, val2, ...)` - Return first non-NULL value
  - `NULLIF(val1, val2)` - Return NULL if values are equal
  - `JSON_GET(json, 'path')` - Extract values from JSON with path syntax
- **IS [NOT] NULL** predicates
- **database/sql driver** - Compatible with Go's standard database interface

## Installation

```bash
go get github.com/simonwaldherr/tinysql
```

## Quick Start

### Using the Engine Directly

```go
package main

import (
    "context"
    "fmt"
    tsql "tinysql"
)

func main() {
    // Create a new database instance
    db := tsql.NewDB()

    // Create table
    parser := tsql.NewParser(`CREATE TABLE users (id INT, name TEXT, active BOOL)`)
    stmt, err := parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    _, err = tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Insert data
    parser = tsql.NewParser(`INSERT INTO users VALUES (1, 'Alice', true)`)
    stmt, err = parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    _, err = tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Query data
    parser = tsql.NewParser(`SELECT * FROM users`)
    stmt, err = parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    result, err := tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Print results
    for _, col := range result.Cols {
        fmt.Printf("%s\t", col)
    }
    fmt.Println()

    for _, row := range result.Rows {
        for _, col := range result.Cols {
            if val, ok := tsql.GetVal(row, col); ok {
                fmt.Printf("%v\t", val)
            }
        }
        fmt.Println()
    }
}
```

### Using the database/sql Driver

```go
package main

import (
    "database/sql"
    "fmt"
    
    _ "tinysql/internal/driver"
)

func main() {
    // Open database connection
    db, err := sql.Open("tinysql", "mem://?tenant=default")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create table and insert data
    _, err = db.Exec(`CREATE TABLE users (id INT, name TEXT, email TEXT)`)
    if err != nil {
        panic(err)
    }

    _, err = db.Exec(`INSERT INTO users VALUES (?, ?, ?)`, 1, "Alice", "alice@example.com")
    if err != nil {
        panic(err)
    }

    // Query data
    rows, err := db.Query(`SELECT id, name, email FROM users WHERE id = ?`, 1)
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name, email string
        err := rows.Scan(&id, &name, &email)
        if err != nil {
            panic(err)
        }
        fmt.Printf("ID: %d, Name: %s, Email: %s\n", id, name, email)
    }
}
```

## Running the REPL

TinySQL comes with an interactive REPL (Read-Eval-Print Loop) for testing SQL queries:

```bash
# Run the interactive REPL
go run ./cmd

# Run built-in demo
go run ./cmd --demo
```

## Examples

### Basic CRUD Operations

```sql
-- Create table
CREATE TABLE products (id INT, name TEXT, price FLOAT, active BOOL);

-- Insert data
INSERT INTO products (id, name, price, active) VALUES (1, 'Laptop', 999.99, true);
INSERT INTO products VALUES (2, 'Mouse', 29.99, true);
INSERT INTO products VALUES (3, 'Keyboard', 79.99, false);

-- Query data
SELECT * FROM products WHERE active = true;
SELECT name, price FROM products WHERE price > 50 ORDER BY price DESC;

-- Update data
UPDATE products SET price = 899.99 WHERE id = 1;

-- Delete data
DELETE FROM products WHERE active = false;
```

### Advanced Queries with JOINs

```sql
-- Setup tables
CREATE TABLE customers (id INT, name TEXT, email TEXT);
CREATE TABLE orders (id INT, customer_id INT, amount FLOAT, status TEXT);

-- Insert test data
INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com');
INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com');
INSERT INTO orders VALUES (101, 1, 250.00, 'PAID');
INSERT INTO orders VALUES (102, 1, 150.00, 'PENDING');
INSERT INTO orders VALUES (103, 2, 300.00, 'PAID');

-- JOIN queries
SELECT c.name, o.amount, o.status 
FROM customers c 
INNER JOIN orders o ON c.id = o.customer_id;

-- Aggregate with GROUP BY
SELECT c.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_spent
FROM customers c 
LEFT JOIN orders o ON c.id = o.customer_id 
GROUP BY c.id, c.name;
```

### Working with JSON Data

```sql
-- Create table with JSON column
CREATE TABLE events (id INT, data JSON);

-- Insert JSON data
INSERT INTO events VALUES (1, '{"type": "click", "element": "button", "user": {"id": 123, "name": "Alice"}}');
INSERT INTO events VALUES (2, '{"type": "view", "page": "/home", "user": {"id": 456, "name": "Bob"}}');

-- Query JSON data
SELECT id, JSON_GET(data, 'type') AS event_type FROM events;
SELECT id, JSON_GET(data, 'user.name') AS user_name FROM events;
```

### NULL Handling and Functions

```sql
CREATE TABLE users (id INT, name TEXT, email TEXT, phone TEXT);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);
INSERT INTO users VALUES (2, 'Bob', NULL, '555-1234');
INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', '555-5678');

-- NULL checks
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE phone IS NULL;

-- COALESCE function
SELECT name, COALESCE(email, phone, 'No contact info') AS contact FROM users;

-- NULLIF function
SELECT name, NULLIF(email, '') AS email FROM users;
```

## Architecture

TinySQL is organized into several internal packages:

- **`internal/storage`** - Database storage layer, table management
- **`internal/engine`** - SQL parsing and execution engine
- **`internal/driver`** - database/sql driver implementation

The main package (`tinysql.go`) provides a clean public API that forwards calls to the appropriate internal packages.

## DSN (Data Source Name) Format

When using the database/sql driver:

- **In-memory database**: `mem://?tenant=<tenant_name>`
- **File-based database**: `file:/path/to/db.dat?tenant=<tenant_name>&autosave=1`

Parameters:
- `tenant` - Tenant name for multi-tenancy (required)
- `autosave` - Auto-save to file (optional, for file-based databases)

## Limitations

TinySQL is designed for educational purposes and small applications. It has the following limitations:

- No MVCC (Multi-Version Concurrency Control)
- No transactions (beyond basic single-statement consistency)
- No WAL (Write-Ahead Logging)
- No advanced indexing
- Limited concurrency support
- No foreign key constraints
- Simplified type coercion rules

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests multiple times to check consistency
go test -v -count=3 ./...
```

## Contributing

This is an educational project. Contributions that improve code clarity, add comprehensive examples, or enhance the learning experience are welcome.

## Educational Goals

TinySQL demonstrates:

- SQL parsing and AST construction
- Query execution and optimization basics
- Database storage concepts
- Go's database/sql driver interface
- 3-valued logic (NULL semantics)
- JSON data handling in SQL
- Multi-tenancy patterns

Perfect for computer science students, developers learning database internals, or anyone who wants to understand how SQL databases work under the hood.
