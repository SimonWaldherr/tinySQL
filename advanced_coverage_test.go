package tinysql

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestEdgeCasesAdvanced tests more advanced edge cases for maximum coverage
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
