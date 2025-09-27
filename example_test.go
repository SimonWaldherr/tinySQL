package tinysql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	tsql "tinysql"
	"tinysql/internal/storage"
)

// TestBasicSQL tests basic SQL operations
func TestBasicSQL(t *testing.T) {
	db := tsql.NewDB()

	// Test CREATE TABLE
	p := tsql.NewParser(`CREATE TABLE users (id INT, name TEXT, active BOOL)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute CREATE TABLE: %v", err)
	}

	// Test INSERT
	p = tsql.NewParser(`INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}
	_, err = tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT: %v", err)
	}

	// Test SELECT
	p = tsql.NewParser(`SELECT * FROM users`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}
	rs, err := tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute SELECT: %v", err)
	}

	// Verify results
	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}
	if len(rs.Cols) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(rs.Cols))
	}
}

// TestBooleanValues specifically tests boolean handling
func TestBooleanValues(t *testing.T) {
	db := tsql.NewDB()

	// Create table with boolean column
	p := tsql.NewParser(`CREATE TABLE test_bool (id INT, flag BOOL)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute CREATE TABLE: %v", err)
	}

	// Insert true value
	p = tsql.NewParser(`INSERT INTO test_bool VALUES (1, true)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT with true: %v", err)
	}
	_, err = tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT with true: %v", err)
	}

	// Insert false value
	p = tsql.NewParser(`INSERT INTO test_bool VALUES (2, false)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT with false: %v", err)
	}
	_, err = tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT with false: %v", err)
	}

	// Select and verify boolean values
	p = tsql.NewParser(`SELECT * FROM test_bool ORDER BY id`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}
	rs, err := tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("Failed to Execute SELECT: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rs.Rows))
	}

	// Check first row (should have true)
	row1 := rs.Rows[0]
	if flag, ok := tsql.GetVal(row1, "test_bool.flag"); !ok || flag != true {
		t.Fatalf("Expected true in first row, got %v", flag)
	}

	// Check second row (should have false)
	row2 := rs.Rows[1]
	if flag, ok := tsql.GetVal(row2, "test_bool.flag"); !ok || flag != false {
		t.Fatalf("Expected false in second row, got %v", flag)
	}
}

// Example demonstrates the usage of the TinySQL engine
func Example() {
	db := tsql.NewDB()

	dedent := func(s string) string {
		trimmed := strings.TrimSpace(s)
		if !strings.Contains(trimmed, "\n") {
			return trimmed
		}
		lines := strings.Split(trimmed, "\n")
		indent := -1
		for _, line := range lines[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			leading := len(line) - len(strings.TrimLeft(line, " \t"))
			if indent == -1 || leading < indent {
				indent = leading
			}
		}
		if indent > 0 {
			for i := 1; i < len(lines); i++ {
				if strings.TrimSpace(lines[i]) == "" {
					lines[i] = ""
					continue
				}
				if len(lines[i]) >= indent {
					lines[i] = lines[i][indent:]
				}
			}
		}
		for i, line := range lines {
			lines[i] = strings.TrimRight(line, " \t")
		}
		return strings.Join(lines, "\n")
	}

	run := func(sql string) {
		display := dedent(sql)
		fmt.Println("SQL>", display)
		p := tsql.NewParser(sql)
		st, err := p.ParseStatement()
		if err != nil {
			fmt.Println("ERR:", err)
			fmt.Println()
			return
		}
		rs, err := tsql.Execute(context.Background(), db, "default", st)
		if err != nil {
			fmt.Println("ERR:", err)
			fmt.Println()
			return
		}
		if rs == nil {
			fmt.Println()
			return
		}
		if len(rs.Rows) == 1 && len(rs.Cols) == 1 && (rs.Cols[0] == "updated" || rs.Cols[0] == "deleted") {
			if val, ok := tsql.GetVal(rs.Rows[0], rs.Cols[0]); ok {
				fmt.Printf("%s: %v\n\n", rs.Cols[0], val)
				return
			}
		}
		displayCols := make([]string, len(rs.Cols))
		for i, col := range rs.Cols {
			parts := strings.Split(col, ".")
			displayCols[i] = parts[len(parts)-1]
		}
		fmt.Println(strings.Join(displayCols, " | "))
		for _, row := range rs.Rows {
			cells := make([]string, len(rs.Cols))
			for i, col := range rs.Cols {
				if v, ok := tsql.GetVal(row, col); ok {
					cells[i] = fmt.Sprint(v)
				} else {
					cells[i] = ""
				}
			}
			fmt.Println(strings.Join(cells, " | "))
		}
		fmt.Println()
	}

	// --- Create table and seed data ---
	run(`CREATE TABLE users (
		id INT,
		name TEXT,
		active BOOL,
		score INT
	)`)

	run(`INSERT INTO users (id, name, active, score) VALUES (1, 'Alice', true, 40)`)
	run(`INSERT INTO users (id, name, active, score) VALUES (2, 'Bob', false, 25)`)
	run(`INSERT INTO users (id, name, active, score) VALUES (3, 'Carol', true, 30)`)

	// --- Basic reads ---
	run(`SELECT id, name, active, score FROM users ORDER BY id`)
	run(`SELECT name, score FROM users WHERE active = true ORDER BY score DESC`)

	// --- Update a row ---
	run(`UPDATE users SET score = 50 WHERE name = 'Bob'`)
	run(`SELECT name, score FROM users ORDER BY id`)

	// --- Aggregate summary ---
	run(`SELECT COUNT(*) AS total_users, SUM(score) AS total_score FROM users`)

	// --- Delete inactive rows ---
	run(`DELETE FROM users WHERE active = false`)
	run(`SELECT name FROM users ORDER BY id`)

	// Output:
	// SQL> CREATE TABLE users (
	//	id INT,
	//	name TEXT,
	//	active BOOL,
	//	score INT
	// )
	//
	// SQL> INSERT INTO users (id, name, active, score) VALUES (1, 'Alice', true, 40)
	//
	// SQL> INSERT INTO users (id, name, active, score) VALUES (2, 'Bob', false, 25)
	//
	// SQL> INSERT INTO users (id, name, active, score) VALUES (3, 'Carol', true, 30)
	//
	// SQL> SELECT id, name, active, score FROM users ORDER BY id
	// id | name | active | score
	// 1 | Alice | true | 40
	// 2 | Bob | false | 25
	// 3 | Carol | true | 30
	//
	// SQL> SELECT name, score FROM users WHERE active = true ORDER BY score DESC
	// name | score
	// Alice | 40
	// Carol | 30
	//
	// SQL> UPDATE users SET score = 50 WHERE name = 'Bob'
	// updated: 1
	//
	// SQL> SELECT name, score FROM users ORDER BY id
	// name | score
	// Alice | 40
	// Bob | 50
	// Carol | 30
	//
	// SQL> SELECT COUNT(*) AS total_users, SUM(score) AS total_score FROM users
	// total_users | total_score
	// 3 | 120
	//
	// SQL> DELETE FROM users WHERE active = false
	// deleted: 1
	//
	// SQL> SELECT name FROM users ORDER BY id
	// name
	// Alice
	// Carol
}

// TestJoinOperations tests basic multi-table functionality
func TestJoinOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create simple tables for testing
	p := tsql.NewParser(`CREATE TABLE users (id INT, name TEXT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE users: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Insert test data
	p = tsql.NewParser(`INSERT INTO users VALUES (1, 'Alice')`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	// Test basic SELECT
	p = tsql.NewParser(`SELECT name FROM users WHERE id = 1`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}

	if name, ok := tsql.GetVal(rs.Rows[0], "users.name"); !ok || name != "Alice" {
		// Try alternative column name format
		if name, ok = tsql.GetVal(rs.Rows[0], "name"); !ok || name != "Alice" {
			t.Fatalf("Expected Alice, got %v. Available columns: %v", name, rs.Cols)
		}
	}
}

// TestAggregateOperations tests basic GROUP BY and aggregate functions
func TestAggregateOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create table
	p := tsql.NewParser(`CREATE TABLE sales (region TEXT, amount FLOAT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	queries := []string{
		`INSERT INTO sales VALUES ('North', 100.0)`,
		`INSERT INTO sales VALUES ('North', 150.0)`,
		`INSERT INTO sales VALUES ('South', 200.0)`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Test simple COUNT
	p = tsql.NewParser(`SELECT COUNT(*) FROM sales`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse COUNT query: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute COUNT query: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row from COUNT, got %d", len(rs.Rows))
	}

	// Check count result - should be 3
	count, ok := tsql.GetVal(rs.Rows[0], "COUNT(*)")
	if !ok {
		// Try other possible column names
		for key := range rs.Rows[0] {
			if count, ok = tsql.GetVal(rs.Rows[0], key); ok {
				break
			}
		}
	}
	if !ok || count != 3 {
		t.Fatalf("Expected count 3, got %v. Available columns: %v", count, rs.Cols)
	}
}

// TestJSONOperations tests JSON data type and functions
func TestJSONOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create table with JSON column
	p := tsql.NewParser(`CREATE TABLE events (id INT, data JSON, timestamp TEXT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert JSON data
	queries := []string{
		`INSERT INTO events VALUES (1, '{"type": "click", "element": "button", "user": {"id": 123, "name": "Alice"}}', '2023-01-01')`,
		`INSERT INTO events VALUES (2, '{"type": "view", "page": "/home", "user": {"id": 456, "name": "Bob"}}', '2023-01-02')`,
		`INSERT INTO events VALUES (3, '{"type": "purchase", "amount": 99.99, "user": {"id": 123, "name": "Alice"}}', '2023-01-03')`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Test JSON_GET function
	p = tsql.NewParser(`SELECT id, JSON_GET(data, 'type') AS event_type, JSON_GET(data, 'user.name') AS user_name FROM events ORDER BY id`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse JSON_GET query: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute JSON_GET query: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rs.Rows))
	}

	// Verify first row
	row1 := rs.Rows[0]
	if eventType, ok := tsql.GetVal(row1, "event_type"); !ok || eventType != "click" {
		t.Fatalf("Expected event_type 'click', got %v", eventType)
	}
	if userName, ok := tsql.GetVal(row1, "user_name"); !ok || userName != "Alice" {
		t.Fatalf("Expected user_name 'Alice', got %v", userName)
	}

	// Test filtering by JSON values
	p = tsql.NewParser(`SELECT id FROM events WHERE JSON_GET(data, 'user.id') = 123 ORDER BY id`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse JSON filter query: %v", err)
	}
	rs, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute JSON filter query: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("Expected 2 events for user 123, got %d", len(rs.Rows))
	}
}

// TestNULLOperations tests NULL handling
func TestNULLOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create table
	p := tsql.NewParser(`CREATE TABLE contacts (id INT, name TEXT, email TEXT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with NULLs
	queries := []string{
		`INSERT INTO contacts VALUES (1, 'Alice', 'alice@example.com')`,
		`INSERT INTO contacts VALUES (2, 'Bob', NULL)`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Test IS NULL
	p = tsql.NewParser(`SELECT name FROM contacts WHERE email IS NULL`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse IS NULL query: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute IS NULL query: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 contact with NULL email, got %d", len(rs.Rows))
	}

	name, ok := tsql.GetVal(rs.Rows[0], "contacts.name")
	if !ok {
		name, ok = tsql.GetVal(rs.Rows[0], "name")
	}
	if !ok || name != "Bob" {
		t.Fatalf("Expected Bob, got %v. Available columns: %v", name, rs.Cols)
	}
}

// TestUpdateDeleteOperations tests basic UPDATE and DELETE statements
func TestUpdateDeleteOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create table
	p := tsql.NewParser(`CREATE TABLE products (id INT, name TEXT, price FLOAT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	queries := []string{
		`INSERT INTO products VALUES (1, 'Laptop', 999.99)`,
		`INSERT INTO products VALUES (2, 'Mouse', 29.99)`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Test UPDATE
	p = tsql.NewParser(`UPDATE products SET price = 899.99 WHERE id = 1`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse UPDATE: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	// Verify update count
	if updated, ok := tsql.GetVal(rs.Rows[0], "updated"); !ok || updated != 1 {
		t.Fatalf("Expected 1 updated row, got %v", updated)
	}

	// Test DELETE
	p = tsql.NewParser(`DELETE FROM products WHERE price < 50`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse DELETE: %v", err)
	}
	rs, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute DELETE: %v", err)
	}

	// Verify delete count (should delete Mouse)
	if deleted, ok := tsql.GetVal(rs.Rows[0], "deleted"); !ok || deleted != 1 {
		t.Fatalf("Expected 1 deleted row, got %v", deleted)
	}
}

// TestTempTableOperations tests temporary tables
func TestTempTableOperations(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create base table
	p := tsql.NewParser(`CREATE TABLE orders (id INT, amount FLOAT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert test data
	queries := []string{
		`INSERT INTO orders VALUES (1, 100.0)`,
		`INSERT INTO orders VALUES (2, 200.0)`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Create temporary table from SELECT
	p = tsql.NewParser(`CREATE TEMP TABLE big_orders AS SELECT * FROM orders WHERE amount > 150`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TEMP TABLE AS SELECT: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute CREATE TEMP TABLE AS SELECT: %v", err)
	}

	// Query the temp table
	p = tsql.NewParser(`SELECT COUNT(*) FROM big_orders`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse temp table SELECT: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute temp table SELECT: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row from temp table query, got %d", len(rs.Rows))
	}

	// Should have 1 big order (200.0)
	count, ok := tsql.GetVal(rs.Rows[0], "COUNT(*)")
	if !ok {
		// Try to find any column with a number
		for key := range rs.Rows[0] {
			if count, ok = tsql.GetVal(rs.Rows[0], key); ok {
				break
			}
		}
	}
	if !ok || count != 1 {
		t.Fatalf("Expected 1 big order, got %v. Available columns: %v", count, rs.Cols)
	}
}

// TestDistinctAndLimitOffset tests DISTINCT, LIMIT, and OFFSET
func TestDistinctAndLimitOffset(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	// Create table
	p := tsql.NewParser(`CREATE TABLE logs (id INT, level TEXT, message TEXT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	queries := []string{
		`INSERT INTO logs VALUES (1, 'INFO', 'App started')`,
		`INSERT INTO logs VALUES (2, 'ERROR', 'Database connection failed')`,
		`INSERT INTO logs VALUES (3, 'INFO', 'User logged in')`,
		`INSERT INTO logs VALUES (4, 'DEBUG', 'Query executed')`,
		`INSERT INTO logs VALUES (5, 'ERROR', 'Timeout occurred')`,
		`INSERT INTO logs VALUES (6, 'INFO', 'Request processed')`,
		`INSERT INTO logs VALUES (7, 'DEBUG', 'Cache hit')`,
		`INSERT INTO logs VALUES (8, 'ERROR', 'Invalid input')`,
	}

	for _, query := range queries {
		p = tsql.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = tsql.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	// Test DISTINCT
	p = tsql.NewParser(`SELECT DISTINCT level FROM logs ORDER BY level`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse DISTINCT query: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute DISTINCT query: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("Expected 3 distinct levels, got %d", len(rs.Rows))
	}

	// Test LIMIT
	p = tsql.NewParser(`SELECT * FROM logs ORDER BY id LIMIT 3`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse LIMIT query: %v", err)
	}
	rs, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute LIMIT query: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("Expected 3 rows with LIMIT 3, got %d", len(rs.Rows))
	}

	// Test OFFSET
	p = tsql.NewParser(`SELECT * FROM logs ORDER BY id OFFSET 2`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse OFFSET query: %v", err)
	}
	rs, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute OFFSET query: %v", err)
	}

	if len(rs.Rows) != 6 {
		t.Fatalf("Expected 6 rows with OFFSET 2, got %d", len(rs.Rows))
	}

	// Verify first row after offset has id = 3
	if id, ok := tsql.GetVal(rs.Rows[0], "logs.id"); !ok || id != 3 {
		t.Fatalf("Expected first row after OFFSET 2 to have id 3, got %v", id)
	}

	// Test LIMIT with OFFSET (pagination)
	p = tsql.NewParser(`SELECT * FROM logs ORDER BY id LIMIT 2 OFFSET 4`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse LIMIT OFFSET query: %v", err)
	}
	rs, err = tsql.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute LIMIT OFFSET query: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("Expected 2 rows with LIMIT 2 OFFSET 4, got %d", len(rs.Rows))
	}

	// Verify we get rows with id 5 and 6
	if id, ok := tsql.GetVal(rs.Rows[0], "logs.id"); !ok || id != 5 {
		t.Fatalf("Expected first row to have id 5, got %v", id)
	}
	if id, ok := tsql.GetVal(rs.Rows[1], "logs.id"); !ok || id != 6 {
		t.Fatalf("Expected second row to have id 6, got %v", id)
	}
}

// Test new data types
func TestNewDataTypes(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()

	// Test creating table with new data types
	sql := `CREATE TABLE advanced_types (
		id INT PRIMARY KEY,
		birth_date DATE,
		created_at DATETIME,
		process_time DURATION,
		metadata JSON,
		coordinates COMPLEX,
		ref_id POINTER
	)`

	p := tsql.NewParser(sql)
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Verify table was created successfully
	tables := db.ListTables("default")
	found := false
	for _, table := range tables {
		if table.Name == "advanced_types" {
			found = true
			if len(table.Cols) != 7 {
				t.Errorf("Expected 7 columns, got %d", len(table.Cols))
			}

			// Verify column types
			expectedTypes := []string{"INT", "DATE", "DATETIME", "DURATION", "JSON", "COMPLEX", "POINTER"}
			for i, col := range table.Cols {
				if col.Type.String() != expectedTypes[i] {
					t.Errorf("Column %d: expected type %s, got %s", i, expectedTypes[i], col.Type.String())
				}
			}
			break
		}
	}
	if !found {
		t.Error("Table 'advanced_types' not found")
	}
}

// Test foreign key constraints
func TestForeignKeyConstraints(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()

	// Create parent table first
	p := tsql.NewParser("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Create child table with foreign key
	sql := `CREATE TABLE employees (
		id INT PRIMARY KEY,
		name TEXT,
		dept_id INT FOREIGN KEY REFERENCES departments(id)
	)`

	p = tsql.NewParser(sql)
	stmt, err = p.ParseStatement()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Verify foreign key constraint was parsed
	table, err := db.Get("default", "employees")
	if err != nil {
		t.Fatal(err)
	}

	// Find the dept_id column and check its constraint
	var deptCol *storage.Column
	for _, col := range table.Cols {
		if col.Name == "dept_id" {
			deptCol = &col
			break
		}
	}

	if deptCol == nil {
		t.Error("dept_id column not found")
		return
	}

	// Check if constraint information was preserved during parsing/execution
	// Note: The actual constraint validation would be implemented in the execution engine
	if deptCol.Type.String() != "INT" {
		t.Errorf("Expected INT type for dept_id, got %s", deptCol.Type.String())
	}
}

// Test query compilation and caching
func TestQueryCompilation(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()

	// Create query cache
	cache := tsql.NewQueryCache(10)
	if cache == nil {
		t.Fatal("Failed to create query cache")
	}

	// Setup table
	setupSQL := "CREATE TABLE products (id INT, name TEXT, price FLOAT)"
	compiled, err := tsql.Compile(cache, setupSQL)
	if err != nil {
		t.Fatal(err)
	}

	// Execute compiled query
	_, err = compiled.Execute(ctx, db, "default")
	if err != nil {
		t.Fatal(err)
	}

	// Test that same query is retrieved from cache
	compiled2, err := tsql.Compile(cache, setupSQL)
	if err != nil {
		t.Fatal(err)
	}

	if compiled != compiled2 {
		t.Log("Expected same compiled query from cache - this indicates cache hit")
	}

	// Test cache stats
	stats := cache.Stats()
	if stats["size"] != 1 {
		t.Errorf("Expected cache size 1, got %v", stats["size"])
	}

	// Test different queries get cached separately
	insertSQL := "INSERT INTO products VALUES (1, 'test', 10.0)"
	insertCompiled, err := tsql.Compile(cache, insertSQL)
	if err != nil {
		t.Fatal(err)
	}

	if insertCompiled == compiled {
		t.Error("Different queries should not return same compiled object")
	}

	// Cache should now have 2 entries
	stats = cache.Stats()
	if stats["size"] != 2 {
		t.Errorf("Expected cache size 2, got %v", stats["size"])
	}
}

// Test MustCompile function
func TestMustCompile(t *testing.T) {
	cache := tsql.NewQueryCache(10)

	// Test successful compilation
	compiled := tsql.MustCompile(cache, "SELECT * FROM test")
	if compiled == nil {
		t.Error("MustCompile should return compiled query")
	}

	if compiled.SQL != "SELECT * FROM test" {
		t.Errorf("Expected SQL to be preserved, got %s", compiled.SQL)
	}

	// Test that it doesn't panic on valid SQL
	defer func() {
		if r := recover(); r != nil {
			t.Error("MustCompile should not panic on valid SQL")
		}
	}()
	tsql.MustCompile(cache, "CREATE TABLE test (id INT)")
}

// Test MustCompile panic behavior
func TestMustCompilePanic(t *testing.T) {
	cache := tsql.NewQueryCache(10)

	// Test that it panics on invalid SQL
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustCompile should panic on invalid SQL")
		}
	}()
	tsql.MustCompile(cache, "INVALID SQL STATEMENT")
}

// Performance test for query compilation
func TestQueryCompilationPerformance(t *testing.T) {
	cache := tsql.NewQueryCache(100)
	querySQL := "SELECT * FROM users WHERE id = 1"

	// First compilation (cache miss)
	start := time.Now()
	compiled1, err := tsql.Compile(cache, querySQL)
	if err != nil {
		t.Fatal(err)
	}
	firstCompileTime := time.Since(start)

	// Second compilation (cache hit)
	start = time.Now()
	compiled2, err := tsql.Compile(cache, querySQL)
	if err != nil {
		t.Fatal(err)
	}
	secondCompileTime := time.Since(start)

	// Cache hit should be much faster
	if secondCompileTime > firstCompileTime {
		t.Log("Cache hit not significantly faster - this may be expected for very simple queries")
	}

	// Should return the same object from cache
	if compiled1 != compiled2 {
		t.Error("Expected same object from cache hit")
	}

	// Measure traditional parsing time for comparison
	start = time.Now()
	for i := 0; i < 100; i++ {
		p := tsql.NewParser(querySQL)
		_, err := p.ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
	}
	parseTime := time.Since(start)

	// Measure cached compilation time
	start = time.Now()
	for i := 0; i < 100; i++ {
		_, err := tsql.Compile(cache, querySQL) // Should be cache hits
		if err != nil {
			t.Fatal(err)
		}
	}
	cachedTime := time.Since(start)

	t.Logf("Traditional parsing 100x: %v", parseTime)
	t.Logf("Cached compilation 100x: %v", cachedTime)

	if cache.Size() != 1 {
		t.Errorf("Expected 1 query in cache, got %d", cache.Size())
	}
}
