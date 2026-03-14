package main

import (
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestExecuteImportCSV(t *testing.T) {
	a := NewApp()
	a.nativeDB = tsql.NewDB()

	csv := "id,name\n1,Alice\n2,Bob\n"
	resp := a.ExecuteImport("users.csv", csv, "users")
	if !resp.Success {
		t.Fatalf("import failed: %s", resp.Error)
	}
	if resp.RowsImported != 2 {
		t.Fatalf("expected 2 rows imported, got %d", resp.RowsImported)
	}
	if len(resp.Columns) == 0 {
		t.Fatalf("expected columns to be detected")
	}
}
