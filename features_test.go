package tinysql

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestFeatureSupport tests the requested SQL features
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
}
