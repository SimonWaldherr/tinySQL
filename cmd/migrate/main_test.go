package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildMigrate(t *testing.T) {
	// Generous on purpose. This shells out to the real toolchain, the link step
	// is not cached, and "go test ./..." runs several of these concurrently, so a
	// tight budget fails on a busy machine while the build itself is fine. The
	// bound is here to catch a hung toolchain, not to police build speed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_migrate_bin")
	defer os.Remove(out)

	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}

	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat binary: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("binary is empty")
	}
	t.Logf("migrate binary size: %d bytes", info.Size())
}

func TestParseDSN(t *testing.T) {
	tests := []struct {
		dsn        string
		wantDriver string
		wantConn   string
	}{
		{"postgres://user:pass@localhost/db?sslmode=disable", "postgres", "postgres://user:pass@localhost/db?sslmode=disable"},
		{"postgresql://user:pass@localhost/db", "postgres", "postgresql://user:pass@localhost/db"},
		{"mysql://user:pass@tcp(localhost:3306)/db", "mysql", "user:pass@tcp(localhost:3306)/db"},
		{"sqlite://test.db", "sqlite", "test.db"},
		{"mssql://user:pass@localhost:1433?database=db", "sqlserver", "sqlserver://user:pass@localhost:1433?database=db"},
		{"sqlserver://user:pass@localhost:1433?database=db", "sqlserver", "sqlserver://user:pass@localhost:1433?database=db"},
		{"test.db", "sqlite", "test.db"},
		{"test.sqlite", "sqlite", "test.sqlite"},
		{"user:pass@tcp(localhost:3306)/db", "mysql", "user:pass@tcp(localhost:3306)/db"},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			driver, conn := parseDSN(tt.dsn)
			if driver != tt.wantDriver {
				t.Errorf("parseDSN(%q) driver = %q, want %q", tt.dsn, driver, tt.wantDriver)
			}
			if conn != tt.wantConn {
				t.Errorf("parseDSN(%q) conn = %q, want %q", tt.dsn, conn, tt.wantConn)
			}
		})
	}
}

func TestTableNameFromFile(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		{"users.csv", "users"},
		{"data/sales.json", "sales"},
		{"my-data.csv", "my_data"},
		{"file with spaces.csv", "file_with_spaces"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := tableNameFromFile(tt.filename)
			if got != tt.want {
				t.Errorf("tableNameFromFile(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"users", "users"},
		{"my-table", "my_table"},
		{"table.name", "table_name"},
		{"table name", "table_name"},
		{"123abc", "123abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeTableName(tt.name)
			if got != tt.want {
				t.Errorf("sanitizeTableName(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"nil", nil, "NULL"},
		{"int", int64(42), "42"},
		{"float", 3.14, "3.14"},
		{"string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"bool true", true, "TRUE"},
		{"bool false", false, "FALSE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatValue(tt.val)
			if got != tt.want {
				t.Errorf("formatValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		driver string
		name   string
		want   string
	}{
		{"mysql", "users", "`users`"},
		{"postgres", "users", `"users"`},
		{"sqlserver", "users", "[users]"},
		{"sqlite", "users", `"users"`},
	}

	for _, tt := range tests {
		t.Run(tt.driver+"/"+tt.name, func(t *testing.T) {
			got := quoteIdentifier(tt.driver, tt.name)
			if got != tt.want {
				t.Errorf("quoteIdentifier(%q, %q) = %q, want %q", tt.driver, tt.name, got, tt.want)
			}
		})
	}
}

// TestExportToExternalSQLiteCharacterization pins exportToExternal's
// existing end-to-end behavior against a real database/sql target (SQLite,
// the only external driver this repo can exercise without a live server)
// before placeholderFor is extracted from its inline per-dialect
// placeholder switch below. SQLite requires "?" placeholders, so this also
// exercises (and locks in) the "default" branch of that switch -- if the
// extraction ever changed what gets generated for a non-postgres driver,
// every statement here would fail to execute rather than silently drifting.
func TestExportToExternalSQLiteCharacterization(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "export_target.db")

	extDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer extDB.Close()

	result := &tinysql.ResultSet{
		Cols: []string{"id", "name", "qty"},
		Rows: []tinysql.Row{
			{"id": int64(1), "name": "Alice", "qty": int64(10)},
			{"id": int64(2), "name": "Bob", "qty": int64(20)},
		},
	}

	count, err := exportToExternal(extDB, "sqlite", result, "exported_items", true)
	if err != nil {
		t.Fatalf("exportToExternal failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	rows, err := extDB.QueryContext(ctx, `SELECT "id", "name", "qty" FROM "exported_items" ORDER BY "id"`)
	if err != nil {
		t.Fatalf("query exported table: %v", err)
	}
	defer rows.Close()

	type got struct {
		id   int64
		name string
		qty  int64
	}
	var gotRows []got
	for rows.Next() {
		var g got
		if err := rows.Scan(&g.id, &g.name, &g.qty); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		gotRows = append(gotRows, g)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []got{{1, "Alice", 10}, {2, "Bob", 20}}
	if len(gotRows) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(gotRows), len(want), gotRows)
	}
	for i := range want {
		if gotRows[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, gotRows[i], want[i])
		}
	}

	// A second export into the same (already existing) table, with
	// createTable still true, must not error -- buildExternalCreateTable
	// uses CREATE TABLE IF NOT EXISTS, and the plain INSERT path must keep
	// working against a pre-existing table.
	count2, err := exportToExternal(extDB, "sqlite", result, "exported_items", true)
	if err != nil {
		t.Fatalf("second exportToExternal failed: %v", err)
	}
	if count2 != 2 {
		t.Fatalf("second count = %d, want 2", count2)
	}
}

func TestPlaceholderFor(t *testing.T) {
	tests := []struct {
		driver string
		idx    int
		want   string
	}{
		{"mysql", 0, "?"},
		{"mysql", 3, "?"},
		{"postgres", 0, "$1"},
		{"postgres", 3, "$4"},
		{"sqlserver", 0, "?"},
		{"sqlserver", 3, "?"},
		{"sqlite", 0, "?"},
		{"sqlite", 3, "?"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", tt.driver, tt.idx), func(t *testing.T) {
			got := placeholderFor(tt.driver, tt.idx)
			if got != tt.want {
				t.Errorf("placeholderFor(%q, %d) = %q, want %q", tt.driver, tt.idx, got, tt.want)
			}
		})
	}
}

func TestMaskDSN(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"postgres://user:secret@localhost/db", "postgres://user:***@localhost/db"},
		{"test.db", "test.db"},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			got := maskDSN(tt.dsn)
			if got != tt.want {
				t.Errorf("maskDSN(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}

func TestSplitQuotedFields(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{`import pg users`, []string{"import", "pg", "users"}},
		{`import pg "SELECT * FROM users" AS local`, []string{"import", "pg", "SELECT * FROM users", "AS", "local"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitQuotedFields(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("splitQuotedFields(%q) = %v (len %d), want %v (len %d)",
					tt.input, got, len(got), tt.want, len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitQuotedFields(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestImportFileAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test_users.csv")
	err := os.WriteFile(csvFile, []byte("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"

	err = importFileToTinySQL(db, ctx, tenant, csvFile, "users", true, false)
	if err != nil {
		t.Fatalf("importFileToTinySQL failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}

	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestImportJSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "data.json")
	err := os.WriteFile(jsonFile, []byte(`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()

	err = importFileToTinySQL(db, ctx, "default", jsonFile, "data", true, false)
	if err != nil {
		t.Fatalf("importFileToTinySQL (JSON) failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM data")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}

	result, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestImportYAMLFile(t *testing.T) {
	tmpDir := t.TempDir()
	yamlFile := filepath.Join(tmpDir, "data.yaml")
	err := os.WriteFile(yamlFile, []byte("- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()

	if err := importFileToTinySQL(db, ctx, "default", yamlFile, "yaml_data", true, false); err != nil {
		t.Fatalf("importFileToTinySQL (YAML) failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM yaml_data")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}
	result, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestImportXMLFile(t *testing.T) {
	tmpDir := t.TempDir()
	xmlFile := filepath.Join(tmpDir, "data.xml")
	err := os.WriteFile(xmlFile, []byte(`<root>
  <record id="1" name="Alice" />
  <record id="2" name="Bob" />
</root>`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()

	if err := importFileToTinySQL(db, ctx, "default", xmlFile, "xml_data", true, false); err != nil {
		t.Fatalf("importFileToTinySQL (XML) failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM xml_data")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}
	result, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// makeSourceSQLiteDB creates a small sqlite file at path with a table "src"
// (id, name) containing 2 rows, for use as an import-db source.
func makeSourceSQLiteDB(t *testing.T, path string) {
	t.Helper()
	srcDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open source sqlite db: %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec("CREATE TABLE src (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := srcDB.Exec("INSERT INTO src (id, name) VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
		t.Fatalf("insert source rows: %v", err)
	}
}

// csvCountValue runs runImportDB with a -tinyquery that counts rows in the
// imported table, writing csv output to a temp file, and returns the parsed
// count.
func csvCountValue(t *testing.T, args []string) int {
	t.Helper()
	if err := runImportDB(args); err != nil {
		t.Fatalf("runImportDB(%v) failed: %v", args, err)
	}
	outFile := ""
	for i, a := range args {
		if a == "-output" && i+1 < len(args) {
			outFile = args[i+1]
		}
	}
	if outFile == "" {
		t.Fatalf("test bug: no -output arg found in %v", args)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	recs, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatalf("parse csv output %q: %v", string(data), err)
	}
	if len(recs) != 2 || len(recs[1]) != 1 {
		t.Fatalf("unexpected csv shape %v (raw %q)", recs, string(data))
	}
	n, err := strconv.Atoi(strings.TrimSpace(recs[1][0]))
	if err != nil {
		t.Fatalf("parse count %q: %v", recs[1][0], err)
	}
	return n
}

// TestImportDBOmittedDBFileBehavesLikeBefore verifies the critical
// non-breaking constraint: when -db-file is omitted, import-db must use a
// fresh empty DB on every invocation and never persist anything. Running the
// same import twice must produce the same row count each time (no
// accumulation across runs, no error, no ambient file created).
func TestImportDBOmittedDBFileBehavesLikeBefore(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	makeSourceSQLiteDB(t, srcPath)

	out1 := filepath.Join(tmpDir, "out1.csv")
	out2 := filepath.Join(tmpDir, "out2.csv")

	baseArgs := func(out string) []string {
		return []string{
			"-dsn", srcPath,
			"-source-table", "src",
			"-table", "imported",
			"-tinyquery", "SELECT COUNT(*) AS c FROM imported",
			"-format", "csv",
			"-output", out,
		}
	}

	count1 := csvCountValue(t, baseArgs(out1))
	count2 := csvCountValue(t, baseArgs(out2))

	if count1 != 2 {
		t.Errorf("run 1: expected count 2, got %d", count1)
	}
	if count2 != 2 {
		t.Errorf("run 2: expected count 2, got %d (should not accumulate without -db-file)", count2)
	}
}

// TestImportDBWithDBFileAccumulatesAcrossRuns verifies that passing -db-file
// loads the existing tinySQL DB at start and saves it back at the end, so
// that a second invocation against the same file sees the first invocation's
// data without re-importing it.
func TestImportDBWithDBFileAccumulatesAcrossRuns(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	makeSourceSQLiteDB(t, srcPath)

	dbFile := filepath.Join(tmpDir, "persist.gob")

	if _, err := os.Stat(dbFile); err == nil {
		t.Fatalf("db file %s should not exist before run 1", dbFile)
	}

	// Run 1: import into "run1_tbl", persisting to dbFile (which doesn't
	// exist yet, so this is effectively the "create on first run" path).
	run1Args := []string{
		"-dsn", srcPath,
		"-source-table", "src",
		"-table", "run1_tbl",
		"-db-file", dbFile,
	}
	if err := runImportDB(run1Args); err != nil {
		t.Fatalf("run 1 runImportDB failed: %v", err)
	}

	if _, err := os.Stat(dbFile); err != nil {
		t.Fatalf("expected db file %s to be created by run 1: %v", dbFile, err)
	}

	// Run 2: import into "run2_tbl" against the SAME dbFile. If -db-file is
	// working correctly, this run loads run 1's persisted DB (which already
	// contains run1_tbl) before adding run2_tbl, and saves both back.
	run2Args := []string{
		"-dsn", srcPath,
		"-source-table", "src",
		"-table", "run2_tbl",
		"-db-file", dbFile,
	}
	if err := runImportDB(run2Args); err != nil {
		t.Fatalf("run 2 runImportDB failed: %v", err)
	}

	// Load the persisted file directly (independent of any in-process state)
	// and confirm run1_tbl's rows are visible without having been re-imported
	// in run 2, i.e. the DB loaded on run 2 already contained run 1's table.
	loaded, err := tinysql.LoadFromFile(dbFile)
	if err != nil {
		t.Fatalf("LoadFromFile(%s) failed: %v", dbFile, err)
	}
	defer func() { _ = loaded.Close() }()

	ctx := context.Background()
	for _, tbl := range []string{"run1_tbl", "run2_tbl"} {
		stmt, err := tinysql.ParseSQL("SELECT * FROM " + tbl)
		if err != nil {
			t.Fatalf("ParseSQL for %s failed: %v", tbl, err)
		}
		result, err := tinysql.Execute(ctx, loaded, "default", stmt)
		if err != nil {
			t.Fatalf("Execute for %s failed: %v", tbl, err)
		}
		if len(result.Rows) != 2 {
			t.Errorf("table %s: expected 2 rows in persisted db, got %d", tbl, len(result.Rows))
		}
	}
}
