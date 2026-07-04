package tinysql_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// TestJSONModeReadmeExample exercises the README's ModeJSON example: open,
// write data, close, and confirm the table lands on disk as readable JSON.
func TestJSONModeReadmeExample(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tinysql")

	db, err := tsql.OpenDB(tsql.StorageConfig{
		Mode: tsql.ModeJSON,
		Path: dir,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE notes (id INT, body TEXT)`,
		`INSERT INTO notes VALUES (1, 'hello json mode')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "default", "notes.json"))
	if err != nil {
		t.Fatalf("expected notes.json on disk: %v", err)
	}
	if !strings.Contains(string(data), "hello json mode") {
		t.Fatalf("expected readable row content in JSON file, got: %s", data)
	}

	reopened, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeJSON, Path: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	selectStmt, _ := tsql.ParseSQL(`SELECT body FROM notes WHERE id = 1`)
	rs, err := tsql.Execute(ctx, reopened, "default", selectStmt)
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["body"] != "hello json mode" {
		t.Fatalf("unexpected result after reopen: %+v", rs.Rows)
	}
}
