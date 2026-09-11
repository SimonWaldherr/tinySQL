package main

import (
	"context"
	"errors"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestExternalExportRollbackAndCancellation(t *testing.T) {
	dst := migrationSource(t)
	if _, err := dst.Exec(`CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	rs := &tinysql.ResultSet{Cols: []string{"ID", "Name"}, Rows: []tinysql.Row{{"id": 1, "name": "a"}, {"id": 1, "name": "duplicate"}}}
	n, err := exportToExternalContext(t.Context(), dst, "sqlite", rs, "target", false)
	if err == nil || n != 0 {
		t.Fatalf("rolled back count=%d err=%v", n, err)
	}
	var count int
	if err := dst.QueryRow(`SELECT COUNT(*) FROM target`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial transaction: %d %v", count, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := exportToExternalContext(ctx, dst, "sqlite", rs, "new_table", true); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %d %v", n, err)
	}
	if err := dst.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='new_table'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("created table after cancellation")
	}
	rs.Rows[1]["id"] = 2
	if n, err := exportToExternalContext(t.Context(), dst, "sqlite", rs, "target", false); err != nil || n != 2 {
		t.Fatalf("export: %d %v", n, err)
	}
	var name string
	if err := dst.QueryRow(`SELECT name FROM target WHERE id=1`).Scan(&name); err != nil || name != "a" {
		t.Fatalf("reused argument buffer: %q %v", name, err)
	}
}
