package main

import (
	"os"
	"path/filepath"
	"testing"

	"fsql/internal/scope"
	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestCmdQueryAcceptsFlagsAfterSubcommand(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "entry.log"), []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mgr, err := scope.NewManager()
	if err != nil {
		t.Fatal(err)
	}

	err = cmdQuery(mgr, []string{
		"--mount", dir,
		"--output", "table",
		"SELECT count(*) AS count FROM files('/', false)",
	}, "", "", "table")
	if err != nil {
		t.Fatalf("query flags after the subcommand should work: %v", err)
	}
}

func TestPrintResultSetRejectsUnknownOutput(t *testing.T) {
	rs := &tinysql.ResultSet{Cols: []string{"value"}}
	if err := printResultSet(rs, "yaml"); err == nil {
		t.Fatal("expected an error for an unknown output format")
	}
}
