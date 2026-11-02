package tinysql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestFeatureSupport tests the requested SQL features
//
//nolint:gocyclo // Integration test enumerates many feature scenarios in one suite.
func TestFeatureSupport(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create test table
	p := engine.NewParser(`CREATE TABLE test_table (id INT, name TEXT, value FLOAT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with more variety for testing
	queries := []string{
		`INSERT INTO test_table VALUES (1, '  alice  ', 100.0)`,
		`INSERT INTO test_table VALUES (2, '  bob  ', 200.0)`,
		`INSERT INTO test_table VALUES (3, '  charlie  ', 150.0)`,
		`INSERT INTO test_table VALUES (4, '  alice  ', 300.0)`,
		`INSERT INTO test_table VALUES (5, '000alice000', 250.0)`,
		`INSERT INTO test_table VALUES (6, '###bob###', 350.0)`,
		`INSERT INTO test_table VALUES (7, NULL, 400.0)`,
	}

	for _, query := range queries {
		p = engine.NewParser(query)
		st, err = p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	t.Run("UNION_ALL", func(t *testing.T) {
		p := engine.NewParser(`SELECT name FROM test_table WHERE id = 1 UNION ALL SELECT name FROM test_table WHERE id = 2`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse UNION ALL query: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute UNION ALL query: %v", err)
		}
		if len(rs.Rows) != 2 {
			t.Fatalf("Expected 2 rows from UNION ALL, got %d", len(rs.Rows))
		}
	})

	t.Run("DISTINCT", func(t *testing.T) {
		p := engine.NewParser(`SELECT DISTINCT name FROM test_table`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse DISTINCT query: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute DISTINCT query: %v", err)
		}
		if len(rs.Rows) != 6 {
			t.Fatalf("Expected 6 distinct names (including NULL), got %d", len(rs.Rows))
		}
	})

	t.Run("INNER_SELECT", func(t *testing.T) {
		// Test subquery in WHERE clause
		p := engine.NewParser(`SELECT * FROM test_table WHERE id IN (SELECT id FROM test_table WHERE value > 150)`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Logf("Subquery not supported yet: %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("Subquery execution failed: %v", err)
			} else {
				t.Logf("Subquery returned %d rows", len(rs.Rows))
			}
		}
	})

	t.Run("CTE", func(t *testing.T) {
		// Test Common Table Expression
		p := engine.NewParser(`WITH high_values AS (SELECT * FROM test_table WHERE value > 150) SELECT * FROM high_values`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Logf("CTE not supported yet: %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("CTE execution failed: %v", err)
			} else {
				t.Logf("CTE returned %d rows", len(rs.Rows))
			}
		}
	})

	t.Run("STRING_FUNCTIONS", func(t *testing.T) {
		// Test basic LTRIM, RTRIM, TRIM
		queries := []struct {
			sql      string
			funcName string
		}{
			{`SELECT LTRIM(name) FROM test_table WHERE id = 1`, "LTRIM"},
			{`SELECT RTRIM(name) FROM test_table WHERE id = 1`, "RTRIM"},
			{`SELECT TRIM(name) FROM test_table WHERE id = 1`, "TRIM"},
		}

		for _, query := range queries {
			p := engine.NewParser(query.sql)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("%s not supported yet: %v", query.funcName, err)
			} else {
				rs, err := engine.Execute(ctx, db, "default", st)
				if err != nil {
					t.Logf("%s execution failed: %v", query.funcName, err)
				} else {
					if len(rs.Rows) > 0 && len(rs.Cols) > 0 {
						t.Logf("%s returned: %v", query.funcName, rs.Rows[0][rs.Cols[0]])
					}
				}
			}
		}

		// Test TRIM functions with custom cutset
		customQueries := []struct {
			sql      string
			funcName string
		}{
			{`SELECT LTRIM(name, '0') FROM test_table WHERE id = 5`, "LTRIM with cutset"},
			{`SELECT RTRIM(name, '0') FROM test_table WHERE id = 5`, "RTRIM with cutset"},
			{`SELECT TRIM(name, '0') FROM test_table WHERE id = 5`, "TRIM with cutset"},
			{`SELECT TRIM(name, '#') FROM test_table WHERE id = 6`, "TRIM with # cutset"},
		}

		for _, query := range customQueries {
			p := engine.NewParser(query.sql)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("%s not supported yet: %v", query.funcName, err)
			} else {
				rs, err := engine.Execute(ctx, db, "default", st)
				if err != nil {
					t.Logf("%s execution failed: %v", query.funcName, err)
				} else {
					if len(rs.Rows) > 0 && len(rs.Cols) > 0 {
						t.Logf("%s returned: %v", query.funcName, rs.Rows[0][rs.Cols[0]])
					}
				}
			}
		}
	})

	t.Run("NULL_FUNCTIONS", func(t *testing.T) {
		// Test NULLIF and IS NULL functionality
		nullQueries := []struct {
			sql      string
			funcName string
		}{
			{`SELECT NULLIF(name, '  alice  ') FROM test_table WHERE id = 1`, "NULLIF (match)"},
			{`SELECT NULLIF(name, '  bob  ') FROM test_table WHERE id = 1`, "NULLIF (no match)"},
			{`SELECT name FROM test_table WHERE name IS NULL`, "IS NULL"},
			{`SELECT name FROM test_table WHERE name IS NOT NULL`, "IS NOT NULL"},
			{`SELECT ISNULL(name) FROM test_table WHERE id = 7`, "ISNULL function (TRUE)"},
			{`SELECT ISNULL(name) FROM test_table WHERE id = 1`, "ISNULL function (FALSE)"},
			{`SELECT COALESCE(name, 'default') FROM test_table WHERE id = 7`, "COALESCE"},
		}

		for _, query := range nullQueries {
			p := engine.NewParser(query.sql)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("%s not supported yet: %v", query.funcName, err)
			} else {
				rs, err := engine.Execute(ctx, db, "default", st)
				if err != nil {
					t.Logf("%s execution failed: %v", query.funcName, err)
				} else {
					t.Logf("%s returned %d rows", query.funcName, len(rs.Rows))
					if len(rs.Rows) > 0 && len(rs.Cols) > 0 && rs.Rows[0][rs.Cols[0]] != nil {
						t.Logf("%s first result: %v", query.funcName, rs.Rows[0][rs.Cols[0]])
					}
				}
			}
		}
	})

	t.Run("REGEXP", func(t *testing.T) {
		// Test REGEXP function
		p := engine.NewParser(`SELECT * FROM test_table WHERE name REGEXP '^.*alice.*$'`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Logf("REGEXP not supported yet: %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("REGEXP execution failed: %v", err)
			} else {
				t.Logf("REGEXP returned %d rows", len(rs.Rows))
			}
		}
	})
	t.Run("GROUP_BY_ORDER_BY", func(t *testing.T) {
		p := engine.NewParser(`SELECT name, COUNT(*) as rows FROM test_table GROUP BY name ORDER BY COUNT(*) DESC`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse GROUP BY/ORDER BY query: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute GROUP BY/ORDER BY query: %v", err)
		}
		if len(rs.Rows) == 0 {
			t.Fatalf("Expected grouped rows, got 0")
		}
		// Check that result is sorted descending by count (int or int64)
		var lastInt, lastInt64 any
		lastInt = int(1 << 30)
		lastInt64 = int64(1 << 62)
		for _, row := range rs.Rows {
			v, ok := row["rows"]
			if !ok {
				t.Fatalf("Missing 'rows' column in result")
			}
			switch cnt := v.(type) {
			case int:
				if cnt > lastInt.(int) {
					t.Fatalf("Result not sorted descending: %d > %d", cnt, lastInt)
				}
				lastInt = cnt
			case int64:
				if cnt > lastInt64.(int64) {
					t.Fatalf("Result not sorted descending: %d > %d", cnt, lastInt64)
				}
				lastInt64 = cnt
			default:
				t.Fatalf("'rows' column is not int or int64: %T", v)
			}
		}
	})

	t.Run("SUBSELECT_GROUP_BY_ORDER_BY", func(t *testing.T) {
		p := engine.NewParser(`SELECT * FROM (SELECT name, COUNT(*) as rows FROM test_table GROUP BY name) x ORDER BY rows DESC`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse subselect GROUP BY/ORDER BY query: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute subselect GROUP BY/ORDER BY query: %v", err)
		}
		if len(rs.Rows) == 0 {
			t.Fatalf("Expected grouped rows from subselect, got 0")
		}
		// Debug: log available columns/keys in the subselect result
		t.Logf("subselect result cols: %v", rs.Cols)
		if len(rs.Rows) > 0 {
			t.Logf("subselect first row keys: %v", rs.Rows[0])
		}
		// Check that result is sorted descending by rows (int or int64)
		var lastInt, lastInt64 any
		lastInt = int(1 << 30)
		lastInt64 = int64(1 << 62)
		for _, row := range rs.Rows {
			v, ok := row["rows"]
			if !ok {
				t.Fatalf("Missing 'rows' column in subselect result")
			}
			switch cnt := v.(type) {
			case int:
				if cnt > lastInt.(int) {
					t.Fatalf("Subselect result not sorted descending: %d > %d", cnt, lastInt)
				}
				lastInt = cnt
			case int64:
				if cnt > lastInt64.(int64) {
					t.Fatalf("Subselect result not sorted descending: %d > %d", cnt, lastInt64)
				}
				lastInt64 = cnt
			default:
				t.Fatalf("'rows' column is not int or int64: %T", v)
			}
		}
	})
}

// TestDropTable tests the DROP TABLE functionality
func TestDropTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create table
	p := engine.NewParser(`CREATE TABLE test_drop (id INT, name TEXT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	_, err = engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	tables := db.ListTables("default")
	found := false
	for _, table := range tables {
		if table.Name == "test_drop" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Table test_drop was not created")
	}

	// Drop table
	p = engine.NewParser(`DROP TABLE test_drop`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse DROP TABLE: %v", err)
	}
	_, err = engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify table is gone
	tables = db.ListTables("default")
	for _, table := range tables {
		if table.Name == "test_drop" {
			t.Fatal("Table test_drop was not dropped")
		}
	}
}

// TestJoinOperations tests various JOIN types
func TestJoinOperations(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create tables
	queries := []string{
		`CREATE TABLE users (id INT, name TEXT)`,
		`CREATE TABLE orders (id INT, user_id INT, amount FLOAT)`,
		`INSERT INTO users VALUES (1, 'alice')`,
		`INSERT INTO users VALUES (2, 'bob')`,
		`INSERT INTO users VALUES (3, 'charlie')`,
		`INSERT INTO orders VALUES (1, 1, 100.0)`,
		`INSERT INTO orders VALUES (2, 1, 200.0)`,
		`INSERT INTO orders VALUES (3, 2, 150.0)`,
	}

	for _, query := range queries {
		p := engine.NewParser(query)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	t.Run("INNER_JOIN", func(t *testing.T) {
		// Simple test to trigger JOIN code paths even if they fail
		p := engine.NewParser(`SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Logf("INNER JOIN parse failed (expected): %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("INNER JOIN execution failed (may be expected): %v", err)
			} else {
				t.Logf("INNER JOIN returned %d rows", len(rs.Rows))
			}
		}
	})

	t.Run("LEFT_JOIN", func(t *testing.T) {
		p := engine.NewParser(`SELECT name, amount FROM users u LEFT JOIN orders o ON u.id = o.user_id`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse LEFT JOIN: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute LEFT JOIN: %v", err)
		}
		// Just verify it executes, JOIN implementation may vary
		if len(rs.Rows) < 3 {
			t.Fatalf("Expected at least 3 rows from LEFT JOIN, got %d", len(rs.Rows))
		}
	})

	t.Run("RIGHT_JOIN", func(t *testing.T) {
		p := engine.NewParser(`SELECT name, amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse RIGHT JOIN: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute RIGHT JOIN: %v", err)
		}
		// Just verify it executes, JOIN implementation may vary
		if len(rs.Rows) < 3 {
			t.Fatalf("Expected at least 3 rows from RIGHT JOIN, got %d", len(rs.Rows))
		}
	})
}

// TestSetOperations tests EXCEPT and INTERSECT
func TestSetOperations(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Setup test data
	queries := []string{
		`CREATE TABLE set1 (id INT, value TEXT)`,
		`CREATE TABLE set2 (id INT, value TEXT)`,
		`INSERT INTO set1 VALUES (1, 'a')`,
		`INSERT INTO set1 VALUES (2, 'b')`,
		`INSERT INTO set1 VALUES (3, 'c')`,
		`INSERT INTO set2 VALUES (2, 'b')`,
		`INSERT INTO set2 VALUES (3, 'c')`,
		`INSERT INTO set2 VALUES (4, 'd')`,
	}

	for _, query := range queries {
		p := engine.NewParser(query)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", query, err)
		}
		_, err = engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute %s: %v", query, err)
		}
	}

	t.Run("EXCEPT", func(t *testing.T) {
		p := engine.NewParser(`SELECT * FROM set1 EXCEPT SELECT * FROM set2`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse EXCEPT: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute EXCEPT: %v", err)
		}
		// EXCEPT should return set1 - set2, verify it executes
		t.Logf("EXCEPT returned %d rows", len(rs.Rows))
	})

	t.Run("INTERSECT", func(t *testing.T) {
		p := engine.NewParser(`SELECT * FROM set1 INTERSECT SELECT * FROM set2`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse INTERSECT: %v", err)
		}
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Failed to execute INTERSECT: %v", err)
		}
		// INTERSECT should return common rows, verify it executes
		t.Logf("INTERSECT returned %d rows", len(rs.Rows))
	})
}

// TestStorageOperations tests storage-level operations
func TestStorageOperations(t *testing.T) {
	db := storage.NewDB()

	// Test DeepClone
	t.Run("DeepClone", func(t *testing.T) {
		cloned := db.DeepClone()
		if cloned == nil {
			t.Fatal("DeepClone returned nil")
		}
	})

	// Test Drop operation
	t.Run("Drop", func(t *testing.T) {
		// Create a table first
		table := storage.NewTable("test_table", []storage.Column{
			{Name: "id", Type: storage.IntType},
			{Name: "name", Type: storage.TextType},
		}, false)
		db.Put("default", table)

		// Drop it
		err := db.Drop("default", "test_table")
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		// Verify it's gone
		_, err = db.Get("default", "test_table")
		if err == nil {
			t.Fatal("Table should not exist after drop")
		}
	})

	// Test file persistence
	t.Run("FilePersistence", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_db_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())
		tmpFile.Close()

		// Create table with data
		table := storage.NewTable("persistent_table", []storage.Column{
			{Name: "id", Type: storage.IntType},
			{Name: "data", Type: storage.TextType},
		}, false)
		table.Rows = append(table.Rows, []any{1, "test"})
		db.Put("default", table)

		// Test basic operations for coverage
		_, err = table.ColIndex("id")
		if err != nil {
			t.Fatalf("Failed to get column index: %v", err)
		}
	})
}

// TestQueryCache tests the query cache functionality
func TestQueryCache(t *testing.T) {
	cache := engine.NewQueryCache(10)

	// Test cache operations
	t.Run("CacheOperations", func(t *testing.T) {
		sql := "SELECT * FROM test"

		// Cache the statement
		cache.Compile(sql)

		// Verify cache size
		if cache.Size() != 1 {
			t.Fatalf("Expected cache size 1, got %d", cache.Size())
		}

		// Get stats
		stats := cache.Stats()
		if len(stats) == 0 {
			t.Fatal("Expected non-empty stats")
		}

		// Clear cache
		cache.Clear()
		if cache.Size() != 0 {
			t.Fatalf("Expected cache size 0 after clear, got %d", cache.Size())
		}
	})
}

// TestEdgeCases tests various edge cases for better coverage
func TestEdgeCases(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	t.Run("NumericConversions", func(t *testing.T) {
		// Test various numeric values to improve coverage of numeric() function
		p := engine.NewParser(`CREATE TABLE numbers (id INT, val FLOAT)`)
		st, _ := p.ParseStatement()
		engine.Execute(ctx, db, "default", st)

		queries := []string{
			`INSERT INTO numbers VALUES (1, 3.14)`,
			`INSERT INTO numbers VALUES (2, 42)`,
			`INSERT INTO numbers VALUES (3, 0)`,
			`INSERT INTO numbers VALUES (4, -1.5)`,
		}

		for _, query := range queries {
			p := engine.NewParser(query)
			st, _ := p.ParseStatement()
			engine.Execute(ctx, db, "default", st)
		}

		// Test operations that use numeric conversion
		p = engine.NewParser(`SELECT val * 2 FROM numbers WHERE val > 0`)
		st, _ = p.ParseStatement()
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Numeric operations failed: %v", err)
		}
		if len(rs.Rows) < 2 {
			t.Fatalf("Expected at least 2 positive numbers")
		}
	})

	t.Run("BooleanOperations", func(t *testing.T) {
		// Test various boolean values to improve coverage of truthy() function
		p := engine.NewParser(`CREATE TABLE bools (id INT, flag BOOL)`)
		st, _ := p.ParseStatement()
		engine.Execute(ctx, db, "default", st)

		queries := []string{
			`INSERT INTO bools VALUES (1, TRUE)`,
			`INSERT INTO bools VALUES (2, FALSE)`,
			`INSERT INTO bools VALUES (3, NULL)`,
		}

		for _, query := range queries {
			p := engine.NewParser(query)
			st, _ := p.ParseStatement()
			engine.Execute(ctx, db, "default", st)
		}

		// Test boolean operations
		p = engine.NewParser(`SELECT * FROM bools WHERE flag = TRUE`)
		st, _ = p.ParseStatement()
		rs, err := engine.Execute(ctx, db, "default", st)
		if err != nil {
			t.Fatalf("Boolean operations failed: %v", err)
		}
		if len(rs.Rows) != 1 {
			t.Fatalf("Expected 1 true value, got %d", len(rs.Rows))
		}
	})
}

// TestEdgeCasesAdvanced tests more advanced edge cases for maximum coverage
//
//nolint:gocyclo // Edge case suite intentionally drives many branches without refactoring core code.
func TestEdgeCasesAdvanced(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create test table
	p := engine.NewParser(`CREATE TABLE advanced_test (id INT, name TEXT, value FLOAT, flag BOOL)`)
	st, _ := p.ParseStatement()
	engine.Execute(ctx, db, "default", st)

	// Insert diverse test data
	queries := []string{
		`INSERT INTO advanced_test VALUES (1, 'test1', 1.5, TRUE)`,
		`INSERT INTO advanced_test VALUES (2, 'test2', 2.0, FALSE)`,
		`INSERT INTO advanced_test VALUES (3, 'test3', NULL, NULL)`,
		`INSERT INTO advanced_test VALUES (4, '', 0.0, FALSE)`,
	}

	for _, query := range queries {
		p := engine.NewParser(query)
		st, _ := p.ParseStatement()
		engine.Execute(ctx, db, "default", st)
	}

	// Test various WHERE conditions to improve coverage
	t.Run("ComplexWhere", func(t *testing.T) {
		testQueries := []string{
			`SELECT * FROM advanced_test WHERE id > 1 AND id < 4`,
			`SELECT * FROM advanced_test WHERE name != 'test1'`,
			`SELECT * FROM advanced_test WHERE value >= 1.5`,
			`SELECT * FROM advanced_test WHERE flag = TRUE OR flag IS NULL`,
			`SELECT * FROM advanced_test WHERE name LIKE 'test%'`,
			`SELECT * FROM advanced_test WHERE value BETWEEN 1.0 AND 2.0`,
		}

		for _, query := range testQueries {
			p := engine.NewParser(query)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("Query failed to parse (may be expected): %s - %v", query, err)
				continue
			}
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("Query failed to execute (may be expected): %s - %v", query, err)
				continue
			}
			t.Logf("Query '%s' returned %d rows", query, len(rs.Rows))
		}
	})

	// Test expression evaluation edge cases
	t.Run("ExpressionEdgeCases", func(t *testing.T) {
		testQueries := []string{
			`SELECT id + value FROM advanced_test WHERE id = 1`,
			`SELECT id * 2 FROM advanced_test WHERE id = 2`,
			`SELECT id - 1 FROM advanced_test WHERE id = 3`,
			`SELECT value / 2.0 FROM advanced_test WHERE value > 0`,
			`SELECT NOT flag FROM advanced_test WHERE flag IS NOT NULL`,
			`SELECT id % 2 FROM advanced_test WHERE id > 0`,
		}

		for _, query := range testQueries {
			p := engine.NewParser(query)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("Expression query failed to parse: %s - %v", query, err)
				continue
			}
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("Expression query failed to execute: %s - %v", query, err)
				continue
			}
			t.Logf("Expression query '%s' returned %d rows", query, len(rs.Rows))
		}
	})

	// Test aggregate functions
	t.Run("AggregateFunctions", func(t *testing.T) {
		testQueries := []string{
			`SELECT COUNT(*) FROM advanced_test`,
			`SELECT COUNT(name) FROM advanced_test`,
			`SELECT SUM(value) FROM advanced_test`,
			`SELECT AVG(value) FROM advanced_test`,
			`SELECT MIN(value) FROM advanced_test`,
			`SELECT MAX(value) FROM advanced_test`,
			`SELECT COUNT(DISTINCT name) FROM advanced_test`,
		}

		for _, query := range testQueries {
			p := engine.NewParser(query)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("Aggregate query failed to parse: %s - %v", query, err)
				continue
			}
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("Aggregate query failed to execute: %s - %v", query, err)
				continue
			}
			t.Logf("Aggregate query '%s' returned %d rows", query, len(rs.Rows))
		}
	})

	// Test ORDER BY and LIMIT
	t.Run("OrderLimit", func(t *testing.T) {
		testQueries := []string{
			`SELECT * FROM advanced_test ORDER BY id`,
			`SELECT * FROM advanced_test ORDER BY id DESC`,
			`SELECT * FROM advanced_test ORDER BY name ASC`,
			`SELECT * FROM advanced_test LIMIT 2`,
			`SELECT * FROM advanced_test ORDER BY id LIMIT 1`,
		}

		for _, query := range testQueries {
			p := engine.NewParser(query)
			st, err := p.ParseStatement()
			if err != nil {
				t.Logf("Order/Limit query failed to parse: %s - %v", query, err)
				continue
			}
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("Order/Limit query failed to execute: %s - %v", query, err)
				continue
			}
			t.Logf("Order/Limit query '%s' returned %d rows", query, len(rs.Rows))
		}
	})

	// Test UPDATE and DELETE operations
	t.Run("UpdateDelete", func(t *testing.T) {
		// Test UPDATE
		p := engine.NewParser(`UPDATE advanced_test SET value = 5.0 WHERE id = 1`)
		st, err := p.ParseStatement()
		if err != nil {
			t.Logf("UPDATE parse failed: %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("UPDATE execution failed: %v", err)
			} else {
				t.Logf("UPDATE affected %d rows", len(rs.Rows))
			}
		}

		// Test DELETE
		p = engine.NewParser(`DELETE FROM advanced_test WHERE id = 4`)
		st, err = p.ParseStatement()
		if err != nil {
			t.Logf("DELETE parse failed: %v", err)
		} else {
			rs, err := engine.Execute(ctx, db, "default", st)
			if err != nil {
				t.Logf("DELETE execution failed: %v", err)
			} else {
				t.Logf("DELETE affected %d rows", len(rs.Rows))
			}
		}
	})
}

// TestStorageAdvanced tests more storage layer functionality
func TestStorageAdvanced(t *testing.T) {
	db := storage.NewDB()

	// Test table name case sensitivity
	t.Run("CaseSensitivity", func(t *testing.T) {
		table1 := storage.NewTable("TestTable", []storage.Column{
			{Name: "ID", Type: storage.IntType},
		}, false)

		err := db.Put("default", table1)
		if err != nil {
			t.Fatalf("Failed to put table: %v", err)
		}

		// Try to get with different case
		_, err = db.Get("default", "testtable")
		if err != nil {
			t.Logf("Case-insensitive lookup failed (expected): %v", err)
		}

		_, err = db.Get("default", "TestTable")
		if err != nil {
			t.Fatalf("Case-sensitive lookup failed: %v", err)
		}
	})

	// Test column index functionality
	t.Run("ColumnIndex", func(t *testing.T) {
		table := storage.NewTable("IndexTest", []storage.Column{
			{Name: "FirstCol", Type: storage.IntType},
			{Name: "SecondCol", Type: storage.TextType},
		}, false)

		// Test valid column index
		idx, err := table.ColIndex("FirstCol")
		if err != nil {
			t.Fatalf("Failed to get valid column index: %v", err)
		}
		if idx != 0 {
			t.Fatalf("Expected index 0, got %d", idx)
		}

		// Test case-insensitive lookup
		idx, err = table.ColIndex("secondcol")
		if err != nil {
			t.Fatalf("Failed to get case-insensitive column index: %v", err)
		}
		if idx != 1 {
			t.Fatalf("Expected index 1, got %d", idx)
		}

		// Test invalid column
		_, err = table.ColIndex("NonExistent")
		if err == nil {
			t.Fatal("Expected error for non-existent column")
		}
	})

	// Test temporary tables
	t.Run("TemporaryTables", func(t *testing.T) {
		tempTable := storage.NewTable("TempTest", []storage.Column{
			{Name: "data", Type: storage.TextType},
		}, true)

		if !tempTable.IsTemp {
			t.Fatal("Expected temporary table to be marked as temporary")
		}

		err := db.Put("default", tempTable)
		if err != nil {
			t.Fatalf("Failed to put temporary table: %v", err)
		}
	})
}

func TestPlusOperatorStringConcat(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	p := engine.NewParser(`CREATE TABLE strings_demo (txt TEXT, num INT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	if _, err := engine.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	p = engine.NewParser(`INSERT INTO strings_demo VALUES ('hello', 42)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}
	if _, err := engine.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	query := `SELECT txt + ' world' AS greeting, txt + num AS mix, num + 8 AS sum FROM strings_demo`
	p = engine.NewParser(query)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse concat query: %v", err)
	}
	rs, err := engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute concat query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}
	row := rs.Rows[0]
	get := func(col string) any { return row[strings.ToLower(col)] }

	greeting, _ := get("greeting").(string)
	if greeting != "hello world" {
		t.Fatalf("Unexpected greeting result: %v", greeting)
	}
	mix, _ := get("mix").(string)
	if mix != "hello42" {
		t.Fatalf("Unexpected mix result: %v", mix)
	}
	// INT + integer literal stays an integer (SQLite semantics).
	if sum := get("sum"); sum != 50 {
		t.Fatalf("Unexpected numeric sum: %#v", sum)
	}
}

func TestTimestampFunctions(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	p := engine.NewParser(`CREATE TABLE dual (id INT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	if _, err := engine.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	p = engine.NewParser(`INSERT INTO dual VALUES (1)`)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}
	if _, err := engine.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	query := `SELECT GETDATE() AS gd,
	                 CURRENT_TIMESTAMP() AS ct,
	                 TODAY() AS today_val,
	                 FROM_TIMESTAMP(0) AS from_zero,
	                 FROM_TIMESTAMP('1.5') AS from_frac,
	                 TIMESTAMP('2023-05-01 00:00:00') AS ts_epoch
	          FROM dual`
	p = engine.NewParser(query)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse timestamp query: %v", err)
	}
	rs, err := engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute timestamp query: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}
	row := rs.Rows[0]
	get := func(col string) any { return row[strings.ToLower(col)] }

	if _, ok := get("gd").(time.Time); !ok {
		t.Fatalf("GETDATE result was not time.Time: %T", get("gd"))
	}
	if _, ok := get("ct").(time.Time); !ok {
		t.Fatalf("CURRENT_TIMESTAMP result was not time.Time: %T", get("ct"))
	}
	today, ok := get("today_val").(time.Time)
	if !ok {
		t.Fatalf("TODAY result was not time.Time: %T", get("today_val"))
	}
	if today.Hour() != 0 || today.Minute() != 0 || today.Second() != 0 {
		t.Fatalf("TODAY result not truncated to day: %v", today)
	}
	fromZero, ok := get("from_zero").(time.Time)
	if !ok {
		t.Fatalf("FROM_TIMESTAMP(0) result was not time.Time: %T", get("from_zero"))
	}
	if !fromZero.Equal(time.Unix(0, 0)) {
		t.Fatalf("FROM_TIMESTAMP(0) mismatch: %v", fromZero)
	}
	fromFrac, ok := get("from_frac").(time.Time)
	if !ok {
		t.Fatalf("FROM_TIMESTAMP('1.5') result was not time.Time: %T", get("from_frac"))
	}
	if fromFrac.Unix() != 1 || fromFrac.Nanosecond() != 500000000 {
		t.Fatalf("FROM_TIMESTAMP('1.5') mismatch: %v", fromFrac)
	}
	epochVal, ok := get("ts_epoch").(int64)
	if !ok {
		t.Fatalf("TIMESTAMP result was not int64: %T", get("ts_epoch"))
	}
	expected := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC).Unix()
	if epochVal != expected {
		t.Fatalf("TIMESTAMP value mismatch, expected %d got %d", expected, epochVal)
	}
}

func TestDistinctOnAndWindowFunctions(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	p := engine.NewParser(`CREATE TABLE t (a INT, b INT)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}
	if _, err := engine.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert rows with duplicates in column a and varying b
	inserts := []string{
		`INSERT INTO t VALUES (1, 10)`,
		`INSERT INTO t VALUES (1, 20)`,
		`INSERT INTO t VALUES (2, 5)`,
		`INSERT INTO t VALUES (2, 15)`,
		`INSERT INTO t VALUES (3, 7)`,
	}
	for _, q := range inserts {
		p = engine.NewParser(q)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("Failed to parse insert: %v", err)
		}
		if _, err := engine.Execute(ctx, db, "default", st); err != nil {
			t.Fatalf("Failed to execute insert: %v", err)
		}
	}

	// DISTINCT ON: pick highest b per a using ORDER BY b DESC
	q := `SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b DESC`
	p = engine.NewParser(q)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse DISTINCT ON query: %v", err)
	}
	rs, err := engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute DISTINCT ON query: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("DISTINCT ON expected 3 rows, got %d", len(rs.Rows))
	}
	// collect b values keyed by a
	got := map[int]int{}
	for _, r := range rs.Rows {
		a := r["a"].(int)
		b := r["b"].(int)
		got[a] = b
	}
	if got[1] != 20 || got[2] != 15 || got[3] != 7 {
		t.Fatalf("DISTINCT ON returned unexpected rows: %#v", got)
	}

	// Window functions: ROW_NUMBER and LAG
	q = `SELECT a, b, ROW_NUMBER() OVER (ORDER BY a, b) AS rn, LAG(b,1) OVER (ORDER BY a, b) AS prev FROM t ORDER BY a, b`
	p = engine.NewParser(q)
	st, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("Failed to parse window query: %v", err)
	}
	rs, err = engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("Failed to execute window query: %v", err)
	}
	if len(rs.Rows) != 5 {
		t.Fatalf("window query expected 5 rows, got %d", len(rs.Rows))
	}
	// verify row numbers and previous values
	// expected ordered rows: (1,10),(1,20),(2,5),(2,15),(3,7)
	expPrev := []any{nil, 10, 20, 5, 15}
	for i, r := range rs.Rows {
		rn := r["rn"].(int)
		if rn != i+1 {
			t.Fatalf("unexpected row number at %d: got %d want %d", i, rn, i+1)
		}
		prev := r["prev"]
		if expPrev[i] == nil {
			if prev != nil {
				t.Fatalf("expected nil prev at row %d, got %#v", i, prev)
			}
		} else {
			if prev == nil || prev.(int) != expPrev[i].(int) {
				t.Fatalf("unexpected prev at row %d: got %#v want %#v", i, prev, expPrev[i])
			}
		}
	}
}

// --- Bare tables ---

func TestBareTableNameSelectsAllRows(t *testing.T) {
	ctx := context.Background()
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	for _, query := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'Bob')",
	} {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatalf("parse %q: %v", query, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", query, err)
		}
	}

	stmt, err := ParseSQL("users")
	if err != nil {
		t.Fatalf("parse bare table name: %v", err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("execute bare table name: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	if got, ok := GetVal(rs.Rows[0], "name"); !ok || got != "Alice" {
		t.Fatalf("expected first name Alice, got %v (ok=%v)", got, ok)
	}
}

func TestBareQuotedTableNameSelectsAllRows(t *testing.T) {
	ctx := context.Background()
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	for _, query := range []string{
		`CREATE TABLE "order" (id INT)`,
		`INSERT INTO "order" VALUES (7)`,
	} {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatalf("parse %q: %v", query, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", query, err)
		}
	}

	stmt, err := ParseSQL(`"order";`)
	if err != nil {
		t.Fatalf("parse quoted bare table name: %v", err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("execute quoted bare table name: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if got, ok := GetVal(rs.Rows[0], "id"); !ok || got != 7 {
		t.Fatalf("expected id 7, got %v (ok=%v)", got, ok)
	}
}

func TestBareKeywordTableNameSelectsAllRows(t *testing.T) {
	ctx := context.Background()
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	for _, query := range []string{
		"CREATE TABLE order (id INT)",
		"INSERT INTO order VALUES (3)",
	} {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatalf("parse %q: %v", query, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", query, err)
		}
	}

	stmt, err := ParseSQL("order")
	if err != nil {
		t.Fatalf("parse bare keyword table name: %v", err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("execute bare keyword table name: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if got, ok := GetVal(rs.Rows[0], "id"); !ok || got != 3 {
		t.Fatalf("expected id 3, got %v (ok=%v)", got, ok)
	}
}

func TestBareTableNameRejectsExtraTokens(t *testing.T) {
	if _, err := ParseSQL("users where id = 1"); err == nil {
		t.Fatal("expected parse error for bare table name with extra tokens")
	}
}

// --- WHERE features ---

// execQuery is a helper that parses and executes a single SQL string.
func execQuery(t *testing.T, ctx context.Context, db *storage.DB, sql string) *engine.ResultSet {
	t.Helper()
	p := engine.NewParser(sql)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return rs
}

// setupWhereDB creates a fresh DB with a small table used by WHERE feature tests.
func setupWhereDB(t *testing.T) (*storage.DB, context.Context) {
	t.Helper()
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE words (id INT, word TEXT, score INT)`,
		`INSERT INTO words VALUES (1,  'apple',   10)`,
		`INSERT INTO words VALUES (2,  'Apricot',  20)`,
		`INSERT INTO words VALUES (3,  'banana',  30)`,
		`INSERT INTO words VALUES (4,  'blueberry', 40)`,
		`INSERT INTO words VALUES (5,  'cherry',  50)`,
		`INSERT INTO words VALUES (6,  'Cherry',  60)`,
		`INSERT INTO words VALUES (7,  'date',    70)`,
		`INSERT INTO words VALUES (8,  'elderberry', 80)`,
		`INSERT INTO words VALUES (9,  'fig',     90)`,
		`INSERT INTO words VALUES (10, 'grape',   100)`,
	} {
		execQuery(t, ctx, db, sql)
	}
	return db, ctx
}

// TestILIKE verifies case-insensitive LIKE matching.
func TestILIKE(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("basic match", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE 'cherry'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (cherry, Cherry), got %d", len(rs.Rows))
		}
	})

	t.Run("prefix wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE 'ap%'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (apple, Apricot), got %d", len(rs.Rows))
		}
	})

	t.Run("suffix wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE '%berry'`)
		// blueberry, elderberry (cherry ends in 'erry' not 'berry')
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (*berry), got %d: %v", len(rs.Rows), rs.Rows)
		}
	})

	t.Run("NOT ILIKE", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT ILIKE '%e%'`)
		// words without 'e' or 'E' (case-insensitive):
		// apple has 'e', Apricot no 'e', banana no, blueberry has 'e', cherry has 'e',
		// Cherry has 'e', date has 'e', elderberry has 'e', fig no, grape has 'e'
		// no 'e': Apricot, banana, fig → 3
		if len(rs.Rows) != 3 {
			t.Fatalf("expected 3 rows (Apricot, banana, fig), got %d", len(rs.Rows))
		}
	})
}

// TestGLOB verifies GLOB (* / ?) pattern matching.
func TestGLOB(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("star wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB 'b*'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (banana, blueberry), got %d", len(rs.Rows))
		}
	})

	t.Run("question mark wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB '???'`)
		// words of exactly 3 chars: fig
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (fig), got %d", len(rs.Rows))
		}
	})

	t.Run("case sensitive", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB 'Cherry'`)
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (Cherry exact), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT GLOB", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT GLOB '*berry'`)
		// berry-words: blueberry, elderberry → 10 - 2 = 8
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows (non-berry), got %d", len(rs.Rows))
		}
	})
}

// TestREGEXP verifies REGEXP / RLIKE infix operator.
func TestREGEXP(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("REGEXP basic", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word REGEXP '^[aA]'`)
		// apple, Apricot
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("RLIKE alias", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word RLIKE 'berry$'`)
		// blueberry, elderberry
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (blueberry, elderberry), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT REGEXP", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT REGEXP 'berry$'`)
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows, got %d", len(rs.Rows))
		}
	})
}

// TestSIMILARTO verifies the SIMILAR TO operator.
func TestSIMILARTO(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("percent wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word SIMILAR TO 'c%'`)
		// cherry (lowercase) — SIMILAR TO is case-sensitive
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (cherry), got %d", len(rs.Rows))
		}
	})

	t.Run("alternation", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word SIMILAR TO 'fig|date'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (fig, date), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT SIMILAR TO", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT SIMILAR TO '%berry'`)
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows, got %d", len(rs.Rows))
		}
	})
}

// TestEXISTS verifies the EXISTS (subquery) predicate.
func TestEXISTS(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE items (id INT, name TEXT)`,
		`CREATE TABLE tags (item_id INT, tag TEXT)`,
		`INSERT INTO items VALUES (1, 'Alpha')`,
		`INSERT INTO items VALUES (2, 'Beta')`,
		`INSERT INTO tags VALUES (1, 'hot')`,
	} {
		execQuery(t, ctx, db, sql)
	}

	t.Run("EXISTS non-empty subquery is true", func(t *testing.T) {
		// All items are returned because the subquery always has rows.
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE EXISTS (SELECT 1 FROM tags)`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (all items), got %d", len(rs.Rows))
		}
	})

	t.Run("EXISTS empty subquery is false", func(t *testing.T) {
		// No items returned because the subquery is empty.
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE EXISTS (SELECT 1 FROM tags WHERE tag = 'nonexistent')`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("NOT EXISTS non-empty subquery is false", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE NOT EXISTS (SELECT 1 FROM tags)`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("NOT EXISTS empty subquery is true", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE NOT EXISTS (SELECT 1 FROM tags WHERE tag = 'none')`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})
}

// TestLEVENSHTEIN verifies the LEVENSHTEIN / EDIT_DISTANCE functions.
func TestLEVENSHTEIN(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execQuery(t, ctx, db, `CREATE TABLE t (s TEXT)`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('kitten')`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('sitting')`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('abc')`)

	t.Run("kitten→sitting", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT LEVENSHTEIN(s, 'sitting') AS d FROM t WHERE s = 'kitten'`)
		if rs.Rows[0]["d"] != 3 {
			t.Fatalf("expected Levenshtein(kitten,sitting)=3, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("identical strings", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT LEVENSHTEIN('hello', 'hello') AS d FROM t LIMIT 1`)
		if rs.Rows[0]["d"] != 0 {
			t.Fatalf("expected 0 for identical strings, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("EDIT_DISTANCE alias", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT EDIT_DISTANCE('abc', 'xyz') AS d FROM t LIMIT 1`)
		if rs.Rows[0]["d"] != 3 {
			t.Fatalf("expected 3, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("WHERE LEVENSHTEIN filter", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT s FROM t WHERE LEVENSHTEIN(s, 'abc') <= 1`)
		if len(rs.Rows) != 1 || rs.Rows[0]["s"] != "abc" {
			t.Fatalf("expected only 'abc', got %v", rs.Rows)
		}
	})
}

// TestSTRING_PREDICATES verifies CONTAINS, STARTS_WITH, ENDS_WITH.
func TestSTRING_PREDICATES(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("CONTAINS", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE CONTAINS(word, 'err')`)
		// blueberry, cherry, Cherry, elderberry
		if len(rs.Rows) != 4 {
			t.Fatalf("expected 4 rows, got %d: %v", len(rs.Rows), rs.Rows)
		}
	})

	t.Run("STARTS_WITH", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE STARTS_WITH(word, 'b')`)
		// banana, blueberry
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("ENDS_WITH", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE ENDS_WITH(word, 'erry')`)
		// cherry, Cherry (no berry ending without b)
		// actually: cherry ends in 'erry', Cherry ends in 'erry', blueberry ends in 'erry', elderberry ends in 'erry'
		if len(rs.Rows) != 4 {
			t.Fatalf("expected 4 rows (*erry), got %d", len(rs.Rows))
		}
	})

	t.Run("CONTAINS false", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE CONTAINS(word, 'xyz')`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})
}

// --- System views ---

func TestQuotedQualifiedSystemViews(t *testing.T) {
	ctx := context.Background()
	db := NewDB()

	create, err := ParseSQL("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("parse create: %v", err)
	}
	if _, err := Execute(ctx, db, "default", create); err != nil {
		t.Fatalf("execute create: %v", err)
	}

	queries := []string{
		`SELECT * FROM "sys"."constraints" LIMIT 10`,
		`SELECT * FROM "catalog"."columns" LIMIT 10`,
	}
	for _, query := range queries {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatalf("parse %s: %v", query, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %s: %v", query, err)
		}
	}
}

// --- SQL formatting ---

func TestBeautifySQL(t *testing.T) {
	got := BeautifySQL("select id,name from users where note = 'from  x' and id=1")
	want := "SELECT id, name\nFROM users\nWHERE note = 'from  x'\nAND id = 1"
	if got != want {
		t.Fatalf("BeautifySQL() = %q, want %q", got, want)
	}
}

func TestMinifySQLPreservesProtectedTokens(t *testing.T) {
	got := MinifySQL("SELECT  'a  b'  -- keep this\n FROM  [user name]  WHERE id = 1")
	want := "SELECT 'a  b' -- keep this\nFROM [user name] WHERE id=1"
	if got != want {
		t.Fatalf("MinifySQL() = %q, want %q", got, want)
	}
}

// A line comment runs to end of line, so BeautifySQL has to terminate it.
// newline() is a no-op while atLineStart is set, and writing the comment did
// not clear that flag, so the closing newline never fired: "SELECT 1 -- note"
// followed by "FROM t" came out as "-- noteFROM t", putting the FROM clause
// inside the comment.
func TestBeautifySQLTerminatesLineComments(t *testing.T) {
	got := BeautifySQL("SELECT 1 -- note\nFROM t")
	if strings.Contains(got, "-- noteFROM") {
		t.Fatalf("FROM was absorbed into the comment: %q", got)
	}
	if !strings.Contains(got, "-- note\n") {
		t.Errorf("comment is not terminated by a newline: %q", got)
	}
	if _, err := ParseSQL(got); err != nil {
		t.Errorf("beautified output does not parse: %v\n%s", err, got)
	}
}

// The same for a block comment followed by more of the statement.
func TestBeautifySQLSeparatesBlockComments(t *testing.T) {
	got := BeautifySQL("SELECT a, /* mid */ b FROM t")
	if strings.Contains(got, "*/b") {
		t.Errorf("token ran straight onto the block comment: %q", got)
	}
	if _, err := ParseSQL(got); err != nil {
		t.Errorf("beautified output does not parse: %v\n%s", err, got)
	}
}

func BenchmarkBeautifySQL(b *testing.B) {
	sql := "SELECT id, name, email FROM users WHERE active = true AND age > 25 ORDER BY name"
	for i := 0; i < b.N; i++ {
		_ = BeautifySQL(sql)
	}
}

func BenchmarkMinifySQL(b *testing.B) {
	sql := "SELECT  id  ,  name  FROM  users  WHERE  active  =  true"
	for i := 0; i < b.N; i++ {
		_ = MinifySQL(sql)
	}
}

func BenchmarkBeautifySQLComplex(b *testing.B) {
	sql := `SELECT u.id, u.name, d.dept_name, p.project_name
		FROM users u
		JOIN departments d ON u.dept_id = d.id
		LEFT JOIN projects p ON u.id = p.user_id
		WHERE u.active = true AND d.status = 'active'
		GROUP BY u.id, u.name, d.dept_name, p.project_name
		HAVING COUNT(*) > 1
		ORDER BY u.name
		LIMIT 100`
	for i := 0; i < b.N; i++ {
		_ = BeautifySQL(sql)
	}
}

// --- Agent context ---

func TestBuildAgentContext(t *testing.T) {
	ctx := context.Background()
	db := NewDB()

	for _, sql := range []string{
		"CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)",
		"CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT FOREIGN KEY REFERENCES departments(id))",
		"INSERT INTO departments VALUES (1, 'Engineering')",
		"INSERT INTO employees VALUES (1, 'Ada', 1)",
	} {
		stmt, err := ParseSQL(sql)
		if err != nil {
			t.Fatalf("ParseSQL(%q): %v", sql, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("Execute(%q): %v", sql, err)
		}
	}

	profile, err := BuildAgentContext(ctx, db, "default", AgentContextConfig{
		MaxTables:          2,
		MaxColumnsPerTable: 2,
		MaxRelations:       2,
		MaxFunctions:       3,
		MaxViews:           2,
		MaxTriggers:        2,
		MaxJobs:            2,
		MaxConnections:     2,
		MaxChars:           5000,
	})
	if err != nil {
		t.Fatalf("BuildAgentContext: %v", err)
	}

	for _, want := range []string{
		"tinySQL agent profile",
		"version:",
		"tables(",
		"departments",
		"employees",
		"relations(",
		"employees.dept_id -> departments.id",
		"connections(",
		"features:",
		"gaps:",
	} {
		if !strings.Contains(profile, want) {
			t.Fatalf("expected profile to contain %q, got:\n%s", want, profile)
		}
	}

	if len(profile) > 5000 {
		t.Fatalf("expected bounded profile, got %d chars", len(profile))
	}
}

func TestDefaultAgentContextConfig(t *testing.T) {
	cfg := DefaultAgentContextConfig().normalized()
	if cfg.MaxTables <= 0 || cfg.MaxChars <= 0 {
		t.Fatalf("expected positive defaults, got %+v", cfg)
	}
}

func BenchmarkBuildAgentContext(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		db := NewDB()
		for _, sql := range []string{
			"CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)",
			"CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT)",
			"INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')",
			"INSERT INTO employees VALUES (1, 'Ada', 1), (2, 'Grace', 1), (3, 'Alan', 2)",
		} {
			stmt, _ := ParseSQL(sql)
			Execute(ctx, db, "default", stmt)
		}
		_, _ = BuildAgentContext(ctx, db, "default", DefaultAgentContextConfig())
	}
}

// --- Deployment modes ---

func TestParseDeploymentMode(t *testing.T) {
	tests := []struct {
		input string
		want  DeploymentMode
		err   bool
	}{
		{"", DeploymentPackage, false},
		{"package", DeploymentPackage, false},
		{"sqlite", DeploymentEmbedded, false},
		{"server", DeploymentServer, false},
		{"enterprise", DeploymentEnterprise, false},
		{"unknown", DeploymentPackage, true},
	}

	for _, tc := range tests {
		got, err := ParseDeploymentMode(tc.input)
		if (err != nil) != tc.err {
			t.Fatalf("ParseDeploymentMode(%q) error = %v, wantErr %v", tc.input, err, tc.err)
		}
		if got != tc.want {
			t.Fatalf("ParseDeploymentMode(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestOpenPackageInstance(t *testing.T) {
	inst := OpenPackage("tenant1")
	defer inst.Close()

	if inst.Mode != DeploymentPackage {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentPackage)
	}
	if inst.Tenant != "tenant1" {
		t.Fatalf("tenant = %q", inst.Tenant)
	}

	if _, err := inst.ExecuteSQL(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("ExecuteSQL CREATE TABLE failed: %v", err)
	}
	if _, err := inst.ExecuteSQL(context.Background(), "INSERT INTO users VALUES (1, 'Ada')"); err != nil {
		t.Fatalf("ExecuteSQL INSERT failed: %v", err)
	}
	rs, err := inst.ExecuteSQL(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("ExecuteSQL SELECT failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Ada" {
		t.Fatalf("unexpected rows: %#v", rs.Rows)
	}
}

func TestOpenEmbeddedInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "embedded.gob")
	inst, err := OpenEmbedded(path, "")
	if err != nil {
		t.Fatalf("OpenEmbedded failed: %v", err)
	}
	defer inst.Close()

	if inst.Mode != DeploymentEmbedded {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentEmbedded)
	}
	if inst.DB.StorageMode() != ModeWAL {
		t.Fatalf("storage mode = %v, want %v", inst.DB.StorageMode(), ModeWAL)
	}
}

func TestOpenEnterpriseRequiresDurableStorage(t *testing.T) {
	if _, err := OpenEnterprise(StorageConfig{}, ""); err == nil {
		t.Fatal("expected enterprise mode without path to fail")
	}
	if _, err := OpenEnterprise(StorageConfig{Mode: ModeMemory, Path: filepath.Join(t.TempDir(), "db.gob")}, ""); err == nil {
		t.Fatal("expected enterprise memory mode to fail")
	}
}

func TestOpenEnterpriseStartsScheduler(t *testing.T) {
	dir := t.TempDir()
	inst, err := OpenEnterprise(StorageConfig{Mode: ModeDisk, Path: dir}, "")
	if err != nil {
		t.Fatalf("OpenEnterprise failed: %v", err)
	}
	defer inst.Close()

	if inst.Mode != DeploymentEnterprise {
		t.Fatalf("mode = %v, want %v", inst.Mode, DeploymentEnterprise)
	}
	if inst.DB.JobScheduler() == nil {
		t.Fatal("expected enterprise profile to start job scheduler")
	}
}

func TestInstanceStopStartRestartHealth(t *testing.T) {
	dir := t.TempDir()
	inst, err := OpenEnterprise(StorageConfig{Mode: ModeDisk, Path: dir}, "main")
	if err != nil {
		t.Fatalf("OpenEnterprise failed: %v", err)
	}
	defer inst.Close()

	if !inst.Health().OK {
		t.Fatalf("initial health = %#v", inst.Health())
	}
	if _, err := inst.ExecuteSQL(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if _, err := inst.ExecuteSQL(context.Background(), "INSERT INTO users VALUES (1, 'Ada')"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if err := inst.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	stopped := inst.Health()
	if stopped.OK || !stopped.Closed || stopped.LastCloseAt.IsZero() {
		t.Fatalf("stopped health mismatch: %#v", stopped)
	}

	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !inst.Health().OK || inst.DB.JobScheduler() == nil {
		t.Fatalf("started health mismatch: %#v", inst.Health())
	}
	rs, err := inst.ExecuteSQL(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("SELECT after start failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Ada" {
		t.Fatalf("unexpected rows after start: %#v", rs.Rows)
	}

	if err := inst.Restart(); err != nil {
		t.Fatalf("Restart failed: %v", err)
	}
	if !inst.Health().OK || inst.DB.JobScheduler() == nil {
		t.Fatalf("restarted health mismatch: %#v", inst.Health())
	}
}

// --- Specialized tables ---

func TestCreateSpecializedTable(t *testing.T) {
	db := NewDB()
	defer db.Close()
	for _, kind := range []TableKind{KeyValueTable, DocumentTable, TimeSeriesTable} {
		if err := CreateSpecializedTable(t.Context(), db, "default", string(kind), kind); err != nil {
			t.Fatal(err)
		}
	}
	if err := CreateSpecializedTable(t.Context(), db, "other", "keyvalue", KeyValueTable, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecSQL(t.Context(), db, "other", `INSERT INTO keyvalue VALUES ('a', X'0102')`); err != nil {
		t.Fatal(err)
	}
	rs, err := ExecSQL(t.Context(), db, "default", `SELECT * FROM keyvalue`)
	if err != nil || len(rs.Rows) != 0 {
		t.Fatalf("tenant isolation: %v %v", rs, err)
	}
	if err := CreateSpecializedTable(t.Context(), nil, "default", "x", KeyValueTable); err == nil {
		t.Fatal("nil DB accepted")
	}
	if err := CreateSpecializedTable(t.Context(), db, "default", "x", TableKind("bad")); err == nil {
		t.Fatal("unknown kind accepted")
	}
	if err := CreateSpecializedTable(t.Context(), db, "default", "x", KeyValueTable, "one"); err == nil {
		t.Fatal("wrong columns accepted")
	}
}

// --- Delayed inserts scheduler ---

func TestDelayedInsertsScheduledDelivery(t *testing.T) {
	db := NewDB()
	run := func(sql string) *ResultSet {
		t.Helper()
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		return rs
	}
	run(`CREATE TABLE scheduled_docs (id INT)`)
	run(`CALL DELAY_INSERT('0s', 'INSERT INTO scheduled_docs VALUES (1)')`)
	run(`CREATE JOB delayed_ingest SCHEDULE INTERVAL 1000 AS CALL FLUSH_DELAYED_INSERTS(10)`)
	if err := StartJobScheduler(db, "default"); err != nil {
		t.Fatal(err)
	}
	defer StopJobScheduler(db)
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("scheduler did not deliver queued values")
		case <-ticker.C:
			if len(run(`SELECT * FROM scheduled_docs`).Rows) == 1 {
				return
			}
		}
	}
}
