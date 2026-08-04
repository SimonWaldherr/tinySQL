//go:build sqliteimport && !js && !wasm && !baremetal

package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestDriverModeSQLitePersistsAndReopens exercises mode=sqlite end-to-end
// through database/sql: data written through one connection must survive a
// full close/reopen cycle as a real SQLite database file — see
// TestDriverModeJSONPersistsAndReopens above for the equivalent ModeJSON
// test this mirrors.
func TestDriverModeSQLitePersistsAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.sqlite")
	dsn := "file:" + path + "?tenant=default&mode=sqlite"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE notes (id INT, body TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO notes VALUES (1, 'hello sqlite mode')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	row := reopened.QueryRow(`SELECT body FROM notes WHERE id = 1`)
	var body string
	if err := row.Scan(&body); err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	if body != "hello sqlite mode" {
		t.Fatalf("got %q, want %q", body, "hello sqlite mode")
	}

	// The file must be genuinely readable by an independent SQLite
	// connection using the plain "sqlite" driver, not just by tinySQL's own
	// backend — that portability is the entire point of ModeSQLite.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite file: %v", err)
	}
	defer raw.Close()
	var rawBody string
	if err := raw.QueryRow(`SELECT "body" FROM "notes" WHERE "id" = 1`).Scan(&rawBody); err != nil {
		t.Fatalf("query raw sqlite file: %v", err)
	}
	if rawBody != "hello sqlite mode" {
		t.Fatalf("raw query got %q, want %q", rawBody, "hello sqlite mode")
	}
}
