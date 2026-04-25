package tinysql

import (
	"context"
	"testing"
)

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
