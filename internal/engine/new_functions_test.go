package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestNewSQLFunctions(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create test table
	_, err := Execute(ctx, db, "default", mustParse("CREATE TABLE test (id INT, name TEXT, value FLOAT)"))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = Execute(ctx, db, "default", mustParse("INSERT INTO test VALUES (1, 'hello world', 3.14159)"))
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// String functions
		{"REPLACE", "SELECT REPLACE(name, 'world', 'SQL') FROM test", "hello SQL"},
		{"INSTR", "SELECT INSTR(name, 'world') FROM test", 7},
		{"REVERSE", "SELECT REVERSE('hello') FROM test", "olleh"},
		{"REPEAT", "SELECT REPEAT('ab', 3) FROM test", "ababab"},
		{"LPAD", "SELECT LPAD('hi', 5, '*') FROM test", "***hi"},
		{"RPAD", "SELECT RPAD('hi', 5, '*') FROM test", "hi***"},

		// Math functions
		{"ABS positive", "SELECT ABS(5) FROM test", float64(5)},
		{"ABS negative", "SELECT ABS(-5) FROM test", float64(5)},
		{"ROUND", "SELECT ROUND(value, 2) FROM test", 3.14},
		{"FLOOR", "SELECT FLOOR(value) FROM test", float64(3)},
		{"CEIL", "SELECT CEIL(value) FROM test", float64(4)},

		// Comparison functions
		{"GREATEST", "SELECT GREATEST(1, 5, 3) FROM test", 5},
		{"LEAST", "SELECT LEAST(1, 5, 3) FROM test", 1},

		// Control flow
		{"IF true", "SELECT IF(1 > 0, 'yes', 'no') FROM test", "yes"},
		{"IF false", "SELECT IF(1 < 0, 'yes', 'no') FROM test", "no"},

		// Date functions
		{"YEAR", "SELECT YEAR('2025-11-24') FROM test", 2025},
		{"MONTH", "SELECT MONTH('2025-11-24') FROM test", 11},
		{"DAY", "SELECT DAY('2025-11-24') FROM test", 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := Execute(ctx, db, "default", mustParse(tt.query))
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(rs.Rows) == 0 {
				t.Fatal("no rows returned")
			}
			// Get first column value
			var got any
			for _, v := range rs.Rows[0] {
				got = v
				break
			}
			// Compare
			switch exp := tt.expected.(type) {
			case int:
				if g, ok := got.(int); ok && g == exp {
					return
				}
				if g, ok := got.(float64); ok && int(g) == exp {
					return
				}
			case float64:
				if g, ok := got.(float64); ok && (g == exp || (g > exp-0.01 && g < exp+0.01)) {
					return
				}
			case string:
				if g, ok := got.(string); ok && g == exp {
					return
				}
			}
			t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, got, got)
		})
	}
}

func TestPrintfFunction(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	_, err := Execute(ctx, db, "default", mustParse("CREATE TABLE dual (x INT)"))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = Execute(ctx, db, "default", mustParse("INSERT INTO dual VALUES (1)"))
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rs, err := Execute(ctx, db, "default", mustParse("SELECT PRINTF('Hello %s, you have %d items', 'User', 5) FROM dual"))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(rs.Rows) == 0 {
		t.Fatal("no rows returned")
	}

	var got string
	for _, v := range rs.Rows[0] {
		got = v.(string)
		break
	}
	expected := "Hello User, you have 5 items"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestRandomFunction(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	_, err := Execute(ctx, db, "default", mustParse("CREATE TABLE dual (x INT)"))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = Execute(ctx, db, "default", mustParse("INSERT INTO dual VALUES (1)"))
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rs, err := Execute(ctx, db, "default", mustParse("SELECT RANDOM() FROM dual"))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(rs.Rows) == 0 {
		t.Fatal("no rows returned")
	}

	var got float64
	for _, v := range rs.Rows[0] {
		got = v.(float64)
		break
	}
	if got < 0 || got >= 1 {
		t.Errorf("RANDOM() should return value in [0,1), got %v", got)
	}
}
