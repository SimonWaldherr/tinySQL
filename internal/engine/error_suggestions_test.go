package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestUnknownTableAndColumnSuggestions covers the "did you mean ...?" hints
// added to unknown-table and unknown-column errors. These are plain
// edit-distance suggestions (no AI/NLP involved), meant to turn a typo like
// "SELECT * FROM usres" or "SELECT nme FROM users" into an actionable error
// instead of a bare "no such table"/"unknown column".
func TestUnknownTableAndColumnSuggestions(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	exec := func(t *testing.T, sql string) error {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		_, err = Execute(ctx, db, "default", stmt)
		return err
	}

	for _, ddl := range []string{
		"CREATE TABLE users (id INT, name TEXT, email TEXT)",
		"INSERT INTO users VALUES (1, 'Alice', 'a@x.com')",
		"CREATE TABLE orders (id INT, user_id INT, total REAL)",
		"INSERT INTO orders VALUES (1, 1, 9.5)",
	} {
		if err := exec(t, ddl); err != nil {
			t.Fatalf("setup %q: %v", ddl, err)
		}
	}

	t.Run("bare table typo suggests existing table", func(t *testing.T) {
		err := exec(t, "usres")
		if err == nil || !strings.Contains(err.Error(), `did you mean "users"?`) {
			t.Fatalf("expected table suggestion, got: %v", err)
		}
	})

	t.Run("SELECT FROM typo suggests existing table", func(t *testing.T) {
		err := exec(t, "SELECT * FROM usres")
		if err == nil || !strings.Contains(err.Error(), `did you mean "users"?`) {
			t.Fatalf("expected table suggestion, got: %v", err)
		}
	})

	t.Run("unrelated bare name gets no false suggestion", func(t *testing.T) {
		err := exec(t, "foobarbaz")
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
		if strings.Contains(err.Error(), "did you mean") {
			t.Fatalf("expected no suggestion for an unrelated name, got: %v", err)
		}
	})

	t.Run("unknown column in projection", func(t *testing.T) {
		err := exec(t, "SELECT nme FROM users")
		if err == nil || !strings.Contains(err.Error(), `unknown column "nme"`) || !strings.Contains(err.Error(), `did you mean "name"?`) {
			t.Fatalf("expected column suggestion, got: %v", err)
		}
	})

	t.Run("unknown column in WHERE", func(t *testing.T) {
		err := exec(t, "SELECT * FROM users WHERE nme = 'Alice'")
		if err == nil || !strings.Contains(err.Error(), `did you mean "name"?`) {
			t.Fatalf("expected column suggestion, got: %v", err)
		}
	})

	t.Run("unknown qualified column in JOIN", func(t *testing.T) {
		err := exec(t, "SELECT u.nme FROM users u JOIN orders o ON u.id = o.user_id")
		if err == nil || !strings.Contains(err.Error(), "unknown column reference") || !strings.Contains(err.Error(), `did you mean "u.name"?`) {
			t.Fatalf("expected qualified column suggestion, got: %v", err)
		}
	})

	t.Run("unknown column in GROUP BY", func(t *testing.T) {
		err := exec(t, "SELECT name, COUNT(*) FROM users GROUP BY nme")
		if err == nil || !strings.Contains(err.Error(), `did you mean "name"?`) {
			t.Fatalf("expected column suggestion, got: %v", err)
		}
	})

	t.Run("unknown column in aggregate", func(t *testing.T) {
		err := exec(t, "SELECT SUM(totl) FROM orders GROUP BY user_id")
		if err == nil || !strings.Contains(err.Error(), `did you mean "total"?`) {
			t.Fatalf("expected column suggestion, got: %v", err)
		}
	})
}
