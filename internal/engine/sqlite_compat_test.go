package engine

import (
	"context"
	"testing"
)

func TestSQLiteSchemaCompatibilityTablesAndViews(t *testing.T) {
	db := setupTestDB()
	if err := db.Catalog().RegisterView("main", "user_names", "SELECT name FROM users"); err != nil {
		t.Fatalf("RegisterView failed: %v", err)
	}
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT sqlite_schema failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected users schema row, got %#v", rs.Rows)
	}
	if rs.Rows[0]["type"] != "table" || rs.Rows[0]["tbl_name"] != "users" {
		t.Fatalf("unexpected users schema row: %#v", rs.Rows[0])
	}
	if sql, _ := rs.Rows[0]["sql"].(string); sql == "" {
		t.Fatalf("expected CREATE TABLE SQL, got %#v", rs.Rows[0])
	}

	rs, err = Execute(ctx, db, "main", mustParseSys("SELECT type, name FROM sqlite_master WHERE name = 'user_names'"))
	if err != nil {
		t.Fatalf("SELECT sqlite_master failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["type"] != "view" {
		t.Fatalf("unexpected sqlite_master view row: %#v", rs.Rows)
	}
}

func TestSQLitePragmaTableInfoAndTableList(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("PRAGMA table_info(users)"))
	if err != nil {
		t.Fatalf("PRAGMA table_info failed: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 columns, got %#v", rs.Rows)
	}
	if rs.Rows[0]["cid"] != 0 || rs.Rows[0]["name"] != "id" || rs.Rows[0]["type"] != "INT" {
		t.Fatalf("unexpected first column: %#v", rs.Rows[0])
	}
	if rs.Rows[0]["notnull"] != 1 || rs.Rows[0]["pk"] != 1 {
		t.Fatalf("expected primary key flags, got %#v", rs.Rows[0])
	}

	rs, err = Execute(ctx, db, "main", mustParseSys("PRAGMA table_xinfo('orders')"))
	if err != nil {
		t.Fatalf("PRAGMA table_xinfo failed: %v", err)
	}
	if len(rs.Rows) != 3 || rs.Cols[len(rs.Cols)-1] != "hidden" {
		t.Fatalf("unexpected table_xinfo result: cols=%v rows=%#v", rs.Cols, rs.Rows)
	}

	rs, err = Execute(ctx, db, "main", mustParseSys("PRAGMA table_list"))
	if err != nil {
		t.Fatalf("PRAGMA table_list failed: %v", err)
	}
	foundUsers := false
	for _, row := range rs.Rows {
		if row["name"] == "users" && row["type"] == "table" && row["ncol"] == 3 {
			foundUsers = true
			break
		}
	}
	if !foundUsers {
		t.Fatalf("users missing from table_list: %#v", rs.Rows)
	}
}

func TestSQLitePragmaOperationalCompatibility(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	tests := []struct {
		sql    string
		column string
		want   any
	}{
		{"PRAGMA database_list", "name", "main"},
		{"PRAGMA foreign_keys", "foreign_keys", 1},
		{"PRAGMA foreign_keys = ON", "foreign_keys", 1},
		{"PRAGMA journal_mode", "journal_mode", "memory"},
		{"PRAGMA integrity_check", "integrity_check", "ok"},
		{"PRAGMA quick_check", "quick_check", "ok"},
		{"PRAGMA schema_version", "schema_version", 0},
		{"PRAGMA user_version", "user_version", 0},
		{"PRAGMA application_id", "application_id", 0},
	}
	for _, tc := range tests {
		rs, err := Execute(ctx, db, "main", mustParseSys(tc.sql))
		if err != nil {
			t.Fatalf("%s failed: %v", tc.sql, err)
		}
		if len(rs.Rows) != 1 || rs.Rows[0][tc.column] != tc.want {
			t.Fatalf("%s result = %#v, want %s=%v", tc.sql, rs.Rows, tc.column, tc.want)
		}
	}

	rs, err := Execute(ctx, db, "main", mustParseSys("PRAGMA compile_options"))
	if err != nil {
		t.Fatalf("PRAGMA compile_options failed: %v", err)
	}
	foundVector := false
	for _, row := range rs.Rows {
		if row["compile_options"] == "ENABLE_VECTOR" {
			foundVector = true
			break
		}
	}
	if !foundVector {
		t.Fatalf("compile_options missing ENABLE_VECTOR: %#v", rs.Rows)
	}
}
