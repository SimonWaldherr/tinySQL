package main

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"SELECT 1", 1},
		{"SELECT 1; SELECT 2", 2},
		{"SELECT 'a;b'; SELECT 2", 2},
		{"  ;  ;  ", 0},
		{"CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t", 3},
		// Doubled single-quote escape: the semicolon is inside the string.
		{`SELECT 'it''s a;test'`, 1},
		// Backslash-escaped quote: semicolon after the string ends.
		{`SELECT 'it\'s'; SELECT 2`, 2},
	}
	for _, tc := range cases {
		got := splitStatements(tc.input)
		if len(got) != tc.want {
			t.Errorf("splitStatements(%q): got %d stmts, want %d (stmts=%v)", tc.input, len(got), tc.want, got)
		}
	}
}

func TestStmtType(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT 1", "SELECT"},
		{"CREATE TABLE t (id INT)", "CREATE TABLE"},
		{"DROP TABLE t", "DROP TABLE"},
		{"CREATE VIEW v AS SELECT 1", "CREATE VIEW"},
		{"DROP VIEW v", "DROP VIEW"},
	}
	for _, tc := range cases {
		stmt, err := tinysql.ParseSQL(tc.sql)
		if err != nil {
			t.Errorf("parse %q: %v", tc.sql, err)
			continue
		}
		if got := stmtType(stmt); got != tc.want {
			t.Errorf("stmtType(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

func TestStmtTypeInsertUpdateDelete(t *testing.T) {
	db := tinysql.NewDB()
	ctx := context.Background()
	stmt, _ := tinysql.ParseSQL(`CREATE TABLE iud (id INT, v TEXT)`)
	_, _ = tinysql.Execute(ctx, db, "default", stmt)

	for sql, want := range map[string]string{
		`INSERT INTO iud VALUES (1, 'x')`:     "INSERT",
		`UPDATE iud SET v = 'y' WHERE id = 1`: "UPDATE",
		`DELETE FROM iud WHERE id = 1`:        "DELETE",
	} {
		s, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Errorf("parse %q: %v", sql, err)
			continue
		}
		if got := stmtType(s); got != want {
			t.Errorf("stmtType(%q) = %q, want %q", sql, got, want)
		}
	}
}

func TestStmtTypeUnknown(t *testing.T) {
	// A raw *engine.DropJob is returned for DROP JOB; ensure stmtType doesn't panic.
	dj := &engine.DropJob{}
	got := stmtType(dj)
	if got == "" {
		t.Fatal("stmtType returned empty string for unknown type")
	}
}

func TestDebugSQLFlow(t *testing.T) {
	db := tinysql.NewDB()

	// Create table
	p := tinysql.NewParser(`CREATE TABLE test_bool (id INT, flag BOOL)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("execute create error: %v", err)
	}

	// Insert rows
	p = tinysql.NewParser(`INSERT INTO test_bool VALUES (1, true)`)
	st, _ = p.ParseStatement()
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("insert1 error: %v", err)
	}

	p = tinysql.NewParser(`INSERT INTO test_bool VALUES (2, false)`)
	st, _ = p.ParseStatement()
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("insert2 error: %v", err)
	}

	// Select and verify
	p = tinysql.NewParser(`SELECT * FROM test_bool ORDER BY id`)
	st, _ = p.ParseStatement()
	rs, err := tinysql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("select err: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
}
