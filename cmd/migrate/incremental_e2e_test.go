package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// ============================================================================
// -mode flag validation (no I/O -- these must fail before opening any
// connection, per the stage 6 contract).
// ============================================================================

func TestRunImportDBModeValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "invalid mode",
			args:    []string{"-mode", "bogus", "-dsn", "x", "-source-table", "t"},
			wantErr: `invalid -mode "bogus"`,
		},
		{
			name:    "incremental requires source-table not query",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-query", "SELECT * FROM t", "-key-col", "id"},
			wantErr: "-query is not supported for incremental sync",
		},
		{
			name:    "incremental requires source-table",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-key-col", "id"},
			wantErr: "-mode=incremental requires -source-table",
		},
		{
			name:    "incremental requires key-col or allow-hash-identity",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-source-table", "t"},
			wantErr: "-mode=incremental requires -key-col or -allow-hash-identity",
		},
		{
			name:    "incremental requires dsn",
			args:    []string{"-mode", "incremental", "-source-table", "t", "-key-col", "id"},
			wantErr: "database DSN is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runImportDB(tt.args)
			if err == nil {
				t.Fatalf("runImportDB(%v) succeeded, want error containing %q", tt.args, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("runImportDB(%v) error = %q, want it to contain %q", tt.args, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestRunExportDBModeValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "invalid mode",
			args:    []string{"-mode", "bogus", "-dsn", "x", "-table", "t"},
			wantErr: `invalid -mode "bogus"`,
		},
		{
			name:    "incremental requires table not query",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-query", "SELECT * FROM t", "-key-col", "id"},
			wantErr: "-query is not supported for incremental sync",
		},
		{
			name:    "incremental requires table",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-key-col", "id"},
			wantErr: "-mode=incremental requires -table",
		},
		{
			name:    "incremental requires key-col or allow-hash-identity",
			args:    []string{"-mode", "incremental", "-dsn", "x", "-table", "t"},
			wantErr: "-mode=incremental requires -key-col or -allow-hash-identity",
		},
		{
			name:    "incremental requires dsn",
			args:    []string{"-mode", "incremental", "-table", "t", "-key-col", "id"},
			wantErr: "target DSN is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runExportDB(tt.args)
			if err == nil {
				t.Fatalf("runExportDB(%v) succeeded, want error containing %q", tt.args, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("runExportDB(%v) error = %q, want it to contain %q", tt.args, err.Error(), tt.wantErr)
			}
		})
	}
}

// ============================================================================
// -mode=full regression: README documents that full mode always appends,
// never dedupes/upserts. Pin that down so it can't silently change.
// ============================================================================

func TestExportDBFullModeAlwaysAppends(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "one_row.csv")
	if err := os.WriteFile(csvFile, []byte("id,name\n1,Alice\n"), 0600); err != nil {
		t.Fatal(err)
	}

	targetDB := filepath.Join(tmpDir, "target.db")

	args := []string{
		"-dsn", targetDB,
		"-files", csvFile,
		"-table", "one_row",
		"-target", "appended",
	}

	// Run 1: target starts empty, ends with exactly the source's one row.
	if err := runExportDB(args); err != nil {
		t.Fatalf("run 1 runExportDB failed: %v", err)
	}

	extDB, err := sql.Open("sqlite", targetDB)
	if err != nil {
		t.Fatalf("open target db: %v", err)
	}
	defer extDB.Close()

	var countAfter1 int
	if err := extDB.QueryRow(`SELECT COUNT(*) FROM "appended"`).Scan(&countAfter1); err != nil {
		t.Fatalf("count query after run 1: %v", err)
	}
	if countAfter1 != 1 {
		t.Fatalf("after run 1: expected 1 row, got %d", countAfter1)
	}

	// Run 2: full mode must append again, never upsert/dedupe -- the target
	// must end up with TWO copies of the row.
	if err := runExportDB(args); err != nil {
		t.Fatalf("run 2 runExportDB failed: %v", err)
	}

	var countAfter2 int
	if err := extDB.QueryRow(`SELECT COUNT(*) FROM "appended"`).Scan(&countAfter2); err != nil {
		t.Fatalf("count query after run 2: %v", err)
	}
	if countAfter2 != 2 {
		t.Fatalf("after run 2 (-mode=full, run twice): expected 2 rows (always appends), got %d", countAfter2)
	}
}

// ============================================================================
// -mode=incremental end-to-end acceptance test (export-db direction):
// three runs against the same state file and the same SQLite target,
// exercising insert-only, update+insert, and delete in turn. This is the
// plan's primary acceptance test for the whole incremental feature.
// ============================================================================

func writeSrcCSV(t *testing.T, dir, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "src.csv")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestExportDBIncrementalThreeRuns(t *testing.T) {
	tmpDir := t.TempDir()
	targetDB := filepath.Join(tmpDir, "target.db")
	stateFile := filepath.Join(tmpDir, "state.json")

	baseArgs := func(csvPath string) []string {
		return []string{
			"-dsn", targetDB,
			"-files", csvPath,
			"-table", "src",
			"-target", "synced_items",
			"-mode", "incremental",
			"-key-col", "id",
			"-state-file", stateFile,
		}
	}

	// Run 1: seed 3 rows (all inserts).
	dir1 := t.TempDir()
	csv1 := writeSrcCSV(t, dir1, "id,name,qty\n1,Alice,10\n2,Bob,20\n3,Carol,30\n")
	if err := runExportDB(baseArgs(csv1)); err != nil {
		t.Fatalf("run 1 runExportDB failed: %v", err)
	}

	// Run 2: change row 2's content (qty 20 -> 25) and add row 4.
	dir2 := t.TempDir()
	csv2 := writeSrcCSV(t, dir2, "id,name,qty\n1,Alice,10\n2,Bob,25\n3,Carol,30\n4,Dave,40\n")
	if err := runExportDB(baseArgs(csv2)); err != nil {
		t.Fatalf("run 2 runExportDB failed: %v", err)
	}

	// Run 3: delete row 3 from the tinySQL source (simply absent from this
	// run's -files input, simulating the source table having lost that row).
	dir3 := t.TempDir()
	csv3 := writeSrcCSV(t, dir3, "id,name,qty\n1,Alice,10\n2,Bob,25\n4,Dave,40\n")
	if err := runExportDB(baseArgs(csv3)); err != nil {
		t.Fatalf("run 3 runExportDB failed: %v", err)
	}

	extDB, err := sql.Open("sqlite", targetDB)
	if err != nil {
		t.Fatalf("open target db: %v", err)
	}
	defer extDB.Close()

	rows, err := extDB.Query(`SELECT "id", "name", "qty" FROM "synced_items" ORDER BY "id"`)
	if err != nil {
		t.Fatalf("query final rows: %v", err)
	}
	defer rows.Close()

	type got struct {
		id, name, qty string
	}
	var final []got
	for rows.Next() {
		var g got
		if err := rows.Scan(&g.id, &g.name, &g.qty); err != nil {
			t.Fatalf("scan: %v", err)
		}
		final = append(final, g)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []got{
		{"1", "Alice", "10"},
		{"2", "Bob", "25"},
		{"4", "Dave", "40"},
	}
	if len(final) != len(want) {
		t.Fatalf("final rows = %+v (len %d), want %+v (len %d)", final, len(final), want, len(want))
	}
	for i := range want {
		if final[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, final[i], want[i])
		}
	}
}

// ============================================================================
// -mode=incremental basic coverage for the import-db direction (external DB
// -> tinySQL), mirroring the export-db acceptance test but in reverse: an
// insert-only run, then a run that updates one row, inserts a new one, and
// drops another, verified against the persisted tinySQL db file.
// ============================================================================

func TestImportDBIncrementalUpdateInsertDelete(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	dbFile := filepath.Join(tmpDir, "persist.gob")
	stateFile := filepath.Join(tmpDir, "state.json")

	srcDB, err := sql.Open("sqlite", srcPath)
	if err != nil {
		t.Fatalf("open source sqlite db: %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec("CREATE TABLE src (id INTEGER, name TEXT, qty INTEGER)"); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := srcDB.Exec("INSERT INTO src (id, name, qty) VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30)"); err != nil {
		t.Fatalf("seed source rows: %v", err)
	}

	args := []string{
		"-dsn", srcPath,
		"-source-table", "src",
		"-table", "synced",
		"-mode", "incremental",
		"-key-col", "id",
		"-db-file", dbFile,
		"-state-file", stateFile,
	}

	if err := runImportDB(args); err != nil {
		t.Fatalf("run 1 runImportDB failed: %v", err)
	}

	// Mutate the source directly: update row 2, insert row 4, delete row 3.
	if _, err := srcDB.Exec("UPDATE src SET qty = 25 WHERE id = 2"); err != nil {
		t.Fatalf("update source row 2: %v", err)
	}
	if _, err := srcDB.Exec("INSERT INTO src (id, name, qty) VALUES (4, 'Dave', 40)"); err != nil {
		t.Fatalf("insert source row 4: %v", err)
	}
	if _, err := srcDB.Exec("DELETE FROM src WHERE id = 3"); err != nil {
		t.Fatalf("delete source row 3: %v", err)
	}

	if err := runImportDB(args); err != nil {
		t.Fatalf("run 2 runImportDB failed: %v", err)
	}

	loaded, err := tinysql.LoadFromFile(dbFile)
	if err != nil {
		t.Fatalf("LoadFromFile(%s) failed: %v", dbFile, err)
	}
	defer func() { _ = loaded.Close() }()

	ctx := context.Background()
	result := execTestSQL(t, ctx, loaded, "default", "SELECT id, name, qty FROM synced ORDER BY id")

	type got struct {
		id   int64
		name string
		qty  int64
	}
	var final []got
	for _, row := range result.Rows {
		final = append(final, got{
			id:   asInt64(t, row["id"]),
			name: row["name"].(string),
			qty:  asInt64(t, row["qty"]),
		})
	}

	want := []got{
		{1, "Alice", 10},
		{2, "Bob", 25},
		{4, "Dave", 40},
	}
	if len(final) != len(want) {
		t.Fatalf("final rows = %+v (len %d), want %+v (len %d)", final, len(final), want, len(want))
	}
	for i := range want {
		if final[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, final[i], want[i])
		}
	}
}
