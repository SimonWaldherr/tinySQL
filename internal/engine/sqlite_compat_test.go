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
		// `PRAGMA foreign_keys = ON` used to be asserted here as returning 1.
		// That pinned the bug: the assignment form was parsed, its value
		// discarded, and the current value echoed back -- so `= OFF` also
		// "succeeded" while foreign keys stayed enforced. The assignment form
		// is now a hard error and is covered by
		// TestSQLitePragmaAssignmentIsRejected; only the read form belongs in
		// this table.
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

func TestSQLitePragmaIndexIntrospection(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE INDEX idx_users_name_email ON users(name, email)`,
		`CREATE UNIQUE INDEX idx_users_email ON users(email)`,
	} {
		if _, err := Execute(ctx, db, "main", mustParseSys(sql)); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rs, err := Execute(ctx, db, "main", mustParseSys(`PRAGMA index_list(users)`))
	if err != nil {
		t.Fatalf("PRAGMA index_list: %v", err)
	}
	if got, want := rs.Cols, []string{"seq", "name", "unique", "origin", "partial"}; len(got) != len(want) {
		t.Fatalf("index_list cols = %v, want %v", got, want)
	}
	indexes := map[string]Row{}
	for _, row := range rs.Rows {
		indexes[row["name"].(string)] = row
	}
	if row := indexes["idx_users_name_email"]; row == nil || row["unique"] != 0 || row["origin"] != "c" || row["partial"] != 0 {
		t.Fatalf("ordinary index row = %#v", row)
	}
	if row := indexes["idx_users_email"]; row == nil || row["unique"] != 1 {
		t.Fatalf("unique index row = %#v", row)
	}

	rs, err = Execute(ctx, db, "main", mustParseSys(`PRAGMA index_info(idx_users_name_email)`))
	if err != nil {
		t.Fatalf("PRAGMA index_info: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("index_info rows = %#v, want two columns", rs.Rows)
	}
	if first, second := rs.Rows[0], rs.Rows[1]; first["seqno"] != 0 || first["cid"] != 1 || first["name"] != "name" || second["seqno"] != 1 || second["cid"] != 2 || second["name"] != "email" {
		t.Fatalf("index_info rows = %#v", rs.Rows)
	}

	rs, err = Execute(ctx, db, "main", mustParseSys(`PRAGMA index_info(missing_index)`))
	if err != nil {
		t.Fatalf("unknown PRAGMA index_info: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("unknown index_info rows = %#v, want empty", rs.Rows)
	}
}

func TestSQLitePragmaForeignKeyList(t *testing.T) {
	db := setupTestDB()
	rs, err := Execute(context.Background(), db, "main", mustParseSys(`PRAGMA foreign_key_list(orders)`))
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_list: %v", err)
	}
	if got, want := rs.Cols, []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}; len(got) != len(want) {
		t.Fatalf("foreign_key_list cols = %v, want %v", got, want)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("foreign_key_list rows = %#v, want one FK", rs.Rows)
	}
	row := rs.Rows[0]
	if row["id"] != 0 || row["seq"] != 0 || row["table"] != "users" || row["from"] != "user_id" || row["to"] != "id" || row["on_update"] != "NO ACTION" || row["on_delete"] != "NO ACTION" || row["match"] != "NONE" {
		t.Fatalf("foreign_key_list row = %#v", row)
	}

	rs, err = Execute(context.Background(), db, "main", mustParseSys(`PRAGMA foreign_key_list(missing_table)`))
	if err != nil {
		t.Fatalf("unknown PRAGMA foreign_key_list: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("unknown foreign_key_list rows = %#v, want empty", rs.Rows)
	}
}
