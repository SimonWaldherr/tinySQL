# TinySQL

[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)


TinySQL is a lightweight, educational SQL database engine written in pure Go. It implements a comprehensive subset of SQL features using only Go's standard library, making it perfect for learning database internals and for applications that need a simple embedded SQL database.


## Features

### Supported SQL Operations

#### Data Definition Language (DDL)
- `CREATE TABLE` - Create regular and temporary tables with advanced constraints
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

#### Basic Types
- `INT` - Integer numbers
- `FLOAT` - Floating-point numbers
- `TEXT` - String values
- `BOOL` - Boolean values (true/false)
- `JSON` - JSON objects and arrays

#### Advanced Types
- `DATE` - Date values (YYYY-MM-DD format)
- `DATETIME` - Date and time values (YYYY-MM-DD HH:MM:SS format)
- `DURATION` - Time duration values (HH:MM:SS format)
- `COMPLEX` - Complex number values (real+imaginary format)
- `POINTER` - References to other table entries

### Advanced Features
- **Multi-tenancy** - Support for multiple tenants in a single database
- **NULL semantics** - Proper 3-valued logic with NULL support
- **Query Compilation** - Parse once, execute many times for better performance
- **GOB Persistence** - Save and load entire database state
- **Web Interface** - Interactive browser-based query interface
- **Constraint Support** - PRIMARY KEY, FOREIGN KEY, UNIQUE constraints
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
    parser := tsql.NewParser(`CREATE TABLE employees (
        id INT PRIMARY KEY,
        name TEXT UNIQUE,
        birth_date DATE,
        hire_datetime DATETIME,
        work_duration DURATION,
        location COMPLEX,
        profile JSON,
        manager_id POINTER REFERENCES employees
    )`)
    stmt, err := parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    _, err = tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Insert data with new types
    parser = tsql.NewParser(`INSERT INTO employees VALUES (
        1, 'Alice Johnson', '1990-05-15', '2022-01-10 09:00:00', 
        '08:30:00', '3.14+2.71i', 
        '{"role": "Senior Dev", "skills": ["Go", "SQL"]}', NULL
    )`)
    stmt, err = parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    _, err = tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Query data
    parser = tsql.NewParser(`SELECT * FROM employees`)
    stmt, err = parser.ParseStatement()
    if err != nil {
        panic(err)
    }
    result, err := tsql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    // Print results
    for _, row := range result.Rows {
        if name, ok := tsql.GetVal(row, "name"); ok {
            if profile, ok := tsql.GetVal(row, "profile"); ok {
                fmt.Printf("Name: %v, Profile: %v\n", name, profile)
            }
        }
    }
}
```

### Using Query Compilation for Better Performance

```go
package main

import (
    "context"
    "fmt"
    tsql "tinysql"
)

func main() {
    db := tsql.NewDB()
    cache := tsql.NewQueryCache(100) // Cache up to 100 queries
    
    // Setup table
    setupSQL := "CREATE TABLE users (id INT, name TEXT, email TEXT)"
    compiled, err := tsql.Compile(cache, setupSQL)
    if err != nil {
        panic(err)
    }
    
    // Execute compiled query
    _, err = tsql.ExecuteCompiled(context.Background(), db, "default", compiled)
    if err != nil {
        panic(err)
    }
    
    // Compile frequently used queries (like regexp.Compile)
    insertQuery := tsql.MustCompile(cache, "INSERT INTO users VALUES (?, ?, ?)")
    selectQuery := tsql.MustCompile(cache, "SELECT * FROM users WHERE id = ?")
    
    fmt.Printf("Cached queries: %d\n", cache.Size())
    
    // Reuse compiled queries - no parsing overhead!
    for i := 1; i <= 1000; i++ {
        // insertQuery can be reused efficiently
        _ = insertQuery
    }
}
```

### GOB Persistence - Save and Load Database

```go
package main

import (
    "context"
    "fmt"
    tsql "tinysql"
)

func main() {
    // Create and populate database
    db := tsql.NewDB()
    
    // Add some data...
    parser := tsql.NewParser(`CREATE TABLE products (id INT, name TEXT, price FLOAT)`)
    stmt, _ := parser.ParseStatement()
    tsql.Execute(context.Background(), db, "default", stmt)
    
    parser = tsql.NewParser(`INSERT INTO products VALUES (1, 'Laptop', 999.99)`)
    stmt, _ = parser.ParseStatement()
    tsql.Execute(context.Background(), db, "default", stmt)
    
    // Save database to file
    err := tsql.SaveToFile(db, "mydb.gob")
    if err != nil {
        panic(err)
    }
    fmt.Println("Database saved to mydb.gob")
    
    // Later... load database from file
    loadedDB, err := tsql.LoadFromFile("mydb.gob")
    if err != nil {
        panic(err)
    }
    
    // Verify data was preserved
    tables := loadedDB.ListTables("default")
    fmt.Printf("Loaded database with %d tables\n", len(tables))
}
```
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

## Running the Interface

TinySQL provides three ways to interact with the database:

### Web Interface

Launch the interactive web interface:

```bash
# Start web interface on http://localhost:8080
go run ./cmd --web
```

Features:
- **Interactive Query Editor** with syntax highlighting
- **Pre-built Example Queries** showcasing all features
- **Schema Visualization** - explore table structures
- **Real-time Results** with formatted output
- **Error Handling** with helpful messages

### Interactive REPL

```bash
# Run the interactive REPL
go run ./cmd

# Example REPL session:
sql> CREATE TABLE users (id INT, name TEXT);
(ok)

sql> INSERT INTO users VALUES (1, 'Alice');
(ok)

sql> SELECT * FROM users;
id  name 
--  -----
1   Alice
```

### Built-in Demo

```bash
# Run comprehensive demo showcasing all features
go run ./cmd --demo
```

## Examples

### Basic CRUD Operations

```sql
-- Create table with constraints
CREATE TABLE products (
    id INT PRIMARY KEY,
    name TEXT UNIQUE,
    price FLOAT,
    active BOOL,
    created_date DATE
);

-- Insert data
INSERT INTO products (id, name, price, active, created_date) 
VALUES (1, 'Laptop', 999.99, true, '2024-01-15');

INSERT INTO products VALUES 
(2, 'Mouse', 29.99, true, '2024-02-01'),
(3, 'Keyboard', 79.99, false, '2024-01-20');

-- Query data
SELECT * FROM products WHERE active = true;
SELECT name, price FROM products WHERE price > 50 ORDER BY price DESC;
SELECT name, price FROM products WHERE created_date > '2024-01-20';

-- Update data  
UPDATE products SET price = 899.99 WHERE id = 1;

-- Delete data
DELETE FROM products WHERE active = false;
```

### Working with Constraints & References

```sql
-- Create departments table with constraints
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name TEXT UNIQUE,
    budget FLOAT
);

-- Create employees table with foreign key and pointer references
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT UNIQUE,
    manager_id POINTER REFERENCES employees,
    dept_id INT FOREIGN KEY REFERENCES departments(id)
);

-- Insert department data
INSERT INTO departments VALUES 
(1, 'Engineering', 500000.0),
(2, 'Marketing', 250000.0);

-- Insert employee data with references
INSERT INTO employees VALUES 
(1, 'Alice Johnson', 'alice@company.com', NULL, 1),      -- No manager
(2, 'Bob Smith', 'bob@company.com', 1, 1),              -- Alice is manager
(3, 'Carol Davis', 'carol@company.com', 1, 2);          -- Alice is manager, Marketing dept

-- Query with relationships
SELECT 
    e.name as employee,
    m.name as manager,
    d.name as department
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
LEFT JOIN departments d ON e.dept_id = d.id;
```

### Advanced Queries with JOINs

```sql
-- Setup tables with new data types
CREATE TABLE customers (
    id INT PRIMARY KEY, 
    name TEXT, 
    email TEXT,
    registration_date DATE,
    profile JSON
);

CREATE TABLE orders (
    id INT, 
    customer_id INT FOREIGN KEY REFERENCES customers(id), 
    amount FLOAT, 
    order_datetime DATETIME,
    processing_duration DURATION,
    status TEXT,
    metadata JSON
);

-- Insert test data
INSERT INTO customers VALUES 
(1, 'John Doe', 'john@example.com', '2024-01-15', '{"vip": true, "preferences": ["electronics"]}'),
(2, 'Jane Smith', 'jane@example.com', '2024-02-01', '{"vip": false, "preferences": ["books", "clothing"]}');

INSERT INTO orders VALUES 
(101, 1, 250.00, '2024-03-01 10:30:00', '00:15:30', 'PAID', '{"payment_method": "credit_card"}'),
(102, 1, 150.00, '2024-03-05 14:20:00', '00:12:15', 'PENDING', '{"payment_method": "paypal"}'),
(103, 2, 300.00, '2024-03-02 09:45:00', '00:18:45', 'PAID', '{"payment_method": "debit_card"}');

-- Complex JOIN with JSON extraction
SELECT 
    c.name,
    JSON_GET(c.profile, 'vip') as is_vip,
    o.amount, 
    o.order_datetime,
    o.processing_duration,
    JSON_GET(o.metadata, 'payment_method') as payment_method
FROM customers c 
INNER JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'PAID'
ORDER BY o.amount DESC;

-- Aggregate with GROUP BY and new data types
SELECT 
    c.name, 
    COUNT(o.id) AS order_count, 
    SUM(o.amount) AS total_spent,
    AVG(o.amount) AS avg_order,
    MIN(o.order_datetime) AS first_order,
    MAX(o.order_datetime) AS last_order
FROM customers c 
LEFT JOIN orders o ON c.id = o.customer_id 
GROUP BY c.id, c.name
HAVING SUM(o.amount) > 200;
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

## Performance Features

### Query Compilation and Caching

TinySQL includes a built-in query compilation system similar to `regexp.Compile()` that significantly improves performance for repeated queries:

```go
package main

import (
    "fmt"
    "time"
    "github.com/simonwaldherr/tinySQL"
)

func performanceBenchmark() {
    db := tinysql.NewDatabase()
    cache := tinysql.NewQueryCache(100) // Cache up to 100 compiled queries
    
    // Create test table
    db.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
    
    // Insert test data
    for i := 1; i <= 1000; i++ {
        db.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d, 'User%d', %d)", 
            i, i, 20+(i%50)))
    }
    
    query := "SELECT * FROM users WHERE age > 30 AND name LIKE 'User1%'"
    
    // Method 1: Traditional parsing (slower)
    start := time.Now()
    for i := 0; i < 100; i++ {
        db.Execute(query)
    }
    traditionalTime := time.Since(start)
    
    // Method 2: Compiled query (faster)
    compiled := cache.Compile(db, query)
    start = time.Now()
    for i := 0; i < 100; i++ {
        compiled.Execute()
    }
    compiledTime := time.Since(start)
    
    fmt.Printf("Traditional: %v\n", traditionalTime)
    fmt.Printf("Compiled: %v\n", compiledTime)
    fmt.Printf("Speedup: %.2fx\n", float64(traditionalTime)/float64(compiledTime))
}
```

### Persistence Performance

```go
func persistenceBenchmark() {
    db := tinysql.NewDatabase()
    
    // Create large dataset
    db.Execute("CREATE TABLE benchmark (id INT, data TEXT, timestamp DATETIME, value COMPLEX)")
    
    start := time.Now()
    for i := 1; i <= 10000; i++ {
        db.Execute(fmt.Sprintf(
            "INSERT INTO benchmark VALUES (%d, 'data_%d', '2024-01-01 12:00:00', '3.14+2.71i')",
            i, i))
    }
    insertTime := time.Since(start)
    
    // Save to file
    start = time.Now()
    err := tinysql.SaveToFile(db, "benchmark.gob")
    saveTime := time.Since(start)
    
    // Load from file
    start = time.Now()
    loadedDb, err := tinysql.LoadFromFile("benchmark.gob")
    loadTime := time.Since(start)
    
    fmt.Printf("Insert 10k rows: %v\n", insertTime)
    fmt.Printf("Save to file: %v\n", saveTime)
    fmt.Printf("Load from file: %v\n", loadTime)
    
    // Verify data integrity
    result := loadedDb.Execute("SELECT COUNT(*) FROM benchmark")
    fmt.Printf("Rows loaded: %v\n", result)
}
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
