package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExecuteBasicStatements(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	stmts := []string{
		`CREATE TABLE users (id INT, name TEXT)`,
		`INSERT INTO users (id, name) VALUES (1, 'Alice')`,
		`INSERT INTO users (id, name) VALUES (2, 'Bob')`,
		`SELECT id, name FROM users WHERE id = 1`,
		`UPDATE users SET name = 'AliceUpdated' WHERE id = 1`,
		`SELECT id, name FROM users WHERE id = 1`,
		`DELETE FROM users WHERE id = 2`,
		`SELECT id, name FROM users`,
	}

	for _, s := range stmts {
		p := NewParser(s)
		stmt, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("parse failed for %q: %v", s, err)
		}
		rs, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			t.Fatalf("execute failed for %q: %v", s, err)
		}
		// basic sanity checks: SELECT should return ResultSet, others may return nil
		if rs != nil && rs.Cols != nil {
			// ensure rows slice is present
			_ = rs.Rows
		}
	}
}

func TestSelectAggregatesAndJoins(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	// Create and populate table t
	stmts := []string{
		`CREATE TABLE t (id INT, g TEXT)`,
		`INSERT INTO t (id, g) VALUES (1, 'a')`,
		`INSERT INTO t (id, g) VALUES (2, 'a')`,
		`INSERT INTO t (id, g) VALUES (3, 'b')`,
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

	// Group by aggregate
	p := NewParser("SELECT g, COUNT(*) as c FROM t GROUP BY g")
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if rs == nil || len(rs.Rows) != 2 {
		t.Fatalf("expected 2 group rows, got %v", rs)
	}

	// Order by + limit
	p = NewParser("SELECT id FROM t ORDER BY id DESC LIMIT 1")
	stmt, _ = p.ParseStatement()
	rs2, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("execute order/limit failed: %v", err)
	}
	if rs2 == nil || len(rs2.Rows) != 1 {
		t.Fatalf("expected 1 row from ORDER/LIMIT, got %v", rs2)
	}

	// Join: create table u and join with t
	p = NewParser("CREATE TABLE u (id INT, name TEXT)")
	stmt, _ = p.ParseStatement()
	if _, err := Execute(ctx, db, "default", stmt); err != nil {
		t.Fatalf("create u failed: %v", err)
	}
	p = NewParser("INSERT INTO u (id, name) VALUES (1, 'Alice')")
	stmt, _ = p.ParseStatement()
	if _, err := Execute(ctx, db, "default", stmt); err != nil {
		t.Fatalf("insert u failed: %v", err)
	}

	p = NewParser("SELECT t.id, u.name FROM t JOIN u ON t.id = u.id")
	stmt, _ = p.ParseStatement()
	rs3, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	// join should return at least one matching row
	if rs3 == nil || len(rs3.Rows) == 0 {
		t.Fatalf("expected join rows, got %v", rs3)
	}
}
