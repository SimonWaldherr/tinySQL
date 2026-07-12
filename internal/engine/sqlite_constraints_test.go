package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSQLiteNotNullAndLiteralDefaults(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execAffinitySQL(t, ctx, db, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name VARCHAR(80) NOT NULL,
		state TEXT NOT NULL DEFAULT 'active',
		attempts INTEGER DEFAULT -1,
		payload BLOB DEFAULT X'CAFE'
	)`)
	execAffinitySQL(t, ctx, db, `INSERT INTO users (id, name) VALUES (1, 'Ada')`)

	table, err := db.Get("default", "users")
	if err != nil {
		t.Fatalf("get users: %v", err)
	}
	got := table.Rows[0]
	if got[0] != 1 || got[1] != "Ada" || got[2] != "active" || got[3] != -1 {
		t.Fatalf("row with defaults = %#v", got)
	}
	if payload, ok := got[4].([]byte); !ok || string(payload) != "\xca\xfe" {
		t.Fatalf("BLOB default = %#v, want []byte{0xca, 0xfe}", got[4])
	}

	stmt, err := NewParser(`INSERT INTO users (id) VALUES (2)`).ParseStatement()
	if err != nil {
		t.Fatalf("parse missing NOT NULL insert: %v", err)
	}
	if _, err := Execute(ctx, db, "default", stmt); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("missing NOT NULL value error = %v", err)
	}

	stmt, err = NewParser(`UPDATE users SET name = NULL WHERE id = 1`).ParseStatement()
	if err != nil {
		t.Fatalf("parse NULL update: %v", err)
	}
	if _, err := Execute(ctx, db, "default", stmt); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("NULL update error = %v", err)
	}
}

func TestSQLiteConstraintIntrospectionRetainsDeclaredSchema(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execAffinitySQL(t, ctx, db, `CREATE TABLE settings (
		label VARCHAR(32) NOT NULL DEFAULT 'general',
		retries INTEGER DEFAULT 3
	)`)

	stmt, err := NewParser(`PRAGMA table_info(settings)`).ParseStatement()
	if err != nil {
		t.Fatalf("parse pragma: %v", err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if rs.Rows[0]["type"] != "VARCHAR(32)" || rs.Rows[0]["notnull"] != 1 || rs.Rows[0]["dflt_value"] != "'general'" {
		t.Fatalf("table_info label = %#v", rs.Rows[0])
	}

	stmt, err = NewParser(`SELECT sql FROM sqlite_schema WHERE name = 'settings'`).ParseStatement()
	if err != nil {
		t.Fatalf("parse schema query: %v", err)
	}
	rs, err = Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	sql, _ := rs.Rows[0]["sql"].(string)
	if !strings.Contains(sql, "VARCHAR(32) NOT NULL DEFAULT 'general'") {
		t.Fatalf("schema SQL does not retain declaration: %q", sql)
	}
}
