package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExecManyFunctionsSmoke(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	// create a table to use in some queries
	stmts := []string{
		`CREATE TABLE ftest (id INT, name TEXT, val FLOAT)`,
		`INSERT INTO ftest (id, name, val) VALUES (1, 'a', 1.5)`,
		`INSERT INTO ftest (id, name, val) VALUES (2, 'b', 2.5)`,
	}
	for _, s := range stmts {
		p := NewParser(s)
		stmt, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("parse failed for %q: %v", s, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute failed for %q: %v", s, err)
		}
	}

	queries := []string{
		"SELECT UPPER(name) FROM ftest",
		"SELECT LOWER(name) FROM ftest",
		"SELECT CONCAT(name, 'x') FROM ftest",
		"SELECT LENGTH(name) FROM ftest",
		"SELECT SUBSTRING(name,1,1) FROM ftest",
		"SELECT ABS(val), ROUND(val), FLOOR(val), CEIL(val) FROM ftest",
		"SELECT SQRT(val), POWER(val, 2) FROM ftest",
		"SELECT MOD(id,2) FROM ftest",
		"SELECT COALESCE(NULL, name) FROM ftest",
		"SELECT COUNT(*) FROM ftest",
		"SELECT id FROM ftest WHERE id > 1",
		"SELECT name FROM ftest ORDER BY id DESC LIMIT 1",
		"SELECT id, name FROM ftest WHERE name LIKE 'a%'",
		"SELECT JSON_GET('{\"a\":1}', 'a')",
	}

	for _, q := range queries {
		p := NewParser(q)
		stmt, err := p.ParseStatement()
		if err != nil {
			// skip parse errors but log
			t.Logf("parse skipped %q: %v", q, err)
			continue
		}
		_, _ = Execute(ctx, db, "default", stmt)
	}
}
