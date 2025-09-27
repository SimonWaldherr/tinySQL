package tinysql_test

import (
	"fmt"
	"strings"
	"testing"

	tsql "github.com/simonwaldherr/tinysql"
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
	_, err = tsql.Execute(db, st)
	if err != nil {
		t.Fatalf("Failed to Execute CREATE TABLE: %v", err)
	}

	// Test INSERT
	p = tsql.NewParser(`INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}
	_, err = tsql.Execute(db, st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT: %v", err)
	}

	// Test SELECT
	p = tsql.NewParser(`SELECT * FROM users`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}
	rs, err := tsql.Execute(db, st)
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
	_, err = tsql.Execute(db, st)
	if err != nil {
		t.Fatalf("Failed to Execute CREATE TABLE: %v", err)
	}

	// Insert true value
	p = tsql.NewParser(`INSERT INTO test_bool VALUES (1, true)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT with true: %v", err)
	}
	_, err = tsql.Execute(db, st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT with true: %v", err)
	}

	// Insert false value
	p = tsql.NewParser(`INSERT INTO test_bool VALUES (2, false)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT with false: %v", err)
	}
	_, err = tsql.Execute(db, st)
	if err != nil {
		t.Fatalf("Failed to Execute INSERT with false: %v", err)
	}

	// Select and verify boolean values
	p = tsql.NewParser(`SELECT * FROM test_bool ORDER BY id`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}
	rs, err := tsql.Execute(db, st)
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
		rs, err := tsql.Execute(db, st)
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
