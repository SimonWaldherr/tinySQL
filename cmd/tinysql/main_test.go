package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildTinysql(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_tinysql_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

func setupTestDB(t *testing.T) *tsql.DB {
	t.Helper()
	db := tsql.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, email TEXT)",
		"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')",
		"INSERT INTO users (id, name, email) VALUES (2, 'Bob', NULL)",
		"CREATE TABLE orders (id INT, user_id INT, amount FLOAT)",
		"INSERT INTO orders (id, user_id, amount) VALUES (101, 1, 99.5)",
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	return db
}

func TestCountTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := countTables(&buf, db, "default", nil); err != nil {
		t.Fatalf("countTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "orders") {
		t.Errorf("expected 'orders' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "TOTAL") {
		t.Errorf("expected 'TOTAL' in output, got:\n%s", out)
	}
}

func TestCountTables_Filtered(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := countTables(&buf, db, "default", []string{"users"}); err != nil {
		t.Fatalf("countTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in output, got:\n%s", out)
	}
	if strings.Contains(out, "orders") {
		t.Errorf("did not expect 'orders' in filtered output, got:\n%s", out)
	}
}

func TestShowStats(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := showStats(&buf, db, "default"); err != nil {
		t.Fatalf("showStats: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Tables:") {
		t.Errorf("expected 'Tables:' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "Total rows:") {
		t.Errorf("expected 'Total rows:' in output, got:\n%s", out)
	}
}

func TestDumpTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := dumpTables(&buf, db, "default", nil); err != nil {
		t.Fatalf("dumpTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE") {
		t.Errorf("expected 'CREATE TABLE' in dump, got:\n%s", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected 'INSERT INTO' in dump, got:\n%s", out)
	}
}

func TestDumpTables_Filtered(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := dumpTables(&buf, db, "default", []string{"orders"}); err != nil {
		t.Fatalf("dumpTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "orders") {
		t.Errorf("expected 'orders' in dump, got:\n%s", out)
	}
	// Users should NOT be dumped
	if strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("did not expect users in filtered dump, got:\n%s", out)
	}
}

func TestImportFileCmd_CSV(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "test.csv")
	os.WriteFile(csvFile, []byte("name,age\nAlice,30\nBob,25\n"), 0644)

	var buf bytes.Buffer
	if err := importFileCmd(db, "default", []string{csvFile, "people"}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Imported") {
		t.Errorf("expected import confirmation, got:\n%s", out)
	}

	// Verify data was imported
	ctx := context.Background()
	stmt, _ := tsql.ParseSQL("SELECT COUNT(*) AS cnt FROM people")
	rs, err := tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		t.Fatal("expected count result")
	}
}

func TestImportFileCmd_JSON(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	jsonFile := filepath.Join(dir, "items.json")
	os.WriteFile(jsonFile, []byte(`[{"x":"hello"},{"x":"world"}]`), 0644)

	var buf bytes.Buffer
	if err := importFileCmd(db, "default", []string{jsonFile, "items"}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Imported") {
		t.Errorf("expected import confirmation, got:\n%s", out)
	}
}

func TestImportFileCmd_AutoTableName(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "products.csv")
	os.WriteFile(csvFile, []byte("name,price\nWidget,9.99\n"), 0644)

	var buf bytes.Buffer
	// Don't pass table name — should default to "products" from filename
	if err := importFileCmd(db, "default", []string{csvFile}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "products") {
		t.Errorf("expected auto-detected table name 'products', got:\n%s", out)
	}
}

func TestExecute_Select(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg, "SELECT name FROM users ORDER BY name", &buf)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected 'Alice' in output, got:\n%s", out)
	}
}

func TestExecute_Timer(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true, Timer: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg, "SELECT 1 AS x", &buf)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Run Time:") {
		t.Errorf("expected 'Run Time:' in timer output, got:\n%s", out)
	}
}

func TestExecute_MultiStatement(t *testing.T) {
	db := tsql.NewDB()
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg,
		"CREATE TABLE ms (x INT); INSERT INTO ms (x) VALUES (42)", &buf)
	if err != nil {
		t.Fatalf("execute multi: %v", err)
	}
	// Verify the table exists
	stmt, _ := tsql.ParseSQL("SELECT x FROM ms")
	rs, err := tsql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		t.Fatal("expected a row")
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"SELECT 1; SELECT 2", 2},
		{"SELECT 'a;b' FROM t", 1},
		{"CREATE TABLE t (x INT)", 1},
		{"", 0},
	}
	for _, tc := range tests {
		got := splitStatements(tc.input)
		if len(got) != tc.want {
			t.Errorf("splitStatements(%q): got %d stmts, want %d", tc.input, len(got), tc.want)
		}
	}
}

func TestFmtScalar(t *testing.T) {
	tests := []struct {
		v       any
		nullVal string
		want    string
	}{
		{nil, "", ""},
		{nil, "NULL", "NULL"},
		{"hello", "", "hello"},
		{float64(42), "", "42"},
		{float64(3.14), "", "3.14"},
	}
	for _, tc := range tests {
		got := fmtScalar(tc.v, tc.nullVal)
		if got != tc.want {
			t.Errorf("fmtScalar(%v, %q) = %q, want %q", tc.v, tc.nullVal, got, tc.want)
		}
	}
}

func TestOpenDatabase_Memory(t *testing.T) {
	db, savePath, err := openDatabase(":memory:")
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	if savePath != "" {
		t.Errorf("expected empty savePath for :memory:, got %q", savePath)
	}
}

func TestOpenDatabase_NewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, savePath, err := openDatabase(path)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	if savePath != path {
		t.Errorf("expected savePath=%q, got %q", path, savePath)
	}
}

func TestPrintTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	printTables(&buf, db, "default")
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in tables list, got:\n%s", out)
	}
}

func TestPrintSchema(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := printSchema(&buf, db, "default", "users"); err != nil {
		t.Fatalf("printSchema: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("expected 'CREATE TABLE users', got:\n%s", out)
	}
}

// Ensure the Repl handleMeta dispatches new commands without error.
func TestReplHandleMeta_NewCommands(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	commands := []string{".help", ".tables", ".schema", ".count", ".stats"}
	for _, cmd := range commands {
		buf.Reset()
		if err := r.handleMeta(cmd); err != nil {
			t.Errorf("handleMeta(%q) error: %v", cmd, err)
		}
		if buf.Len() == 0 {
			t.Errorf("handleMeta(%q) produced no output", cmd)
		}
	}
}

func TestReplHandleMeta_Mode(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	// Show current mode
	if err := r.handleMeta(".mode"); err != nil {
		t.Fatalf("handleMeta(.mode): %v", err)
	}
	if !strings.Contains(buf.String(), "column") {
		t.Errorf("expected current mode in output, got:\n%s", buf.String())
	}

	// Change mode
	if err := r.handleMeta(".mode json"); err != nil {
		t.Fatalf("handleMeta(.mode json): %v", err)
	}
	if cfg.Mode != ModeJSON {
		t.Errorf("expected mode=json, got %s", cfg.Mode)
	}
}

func TestReplHandleMeta_Timer(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default"}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	if err := r.handleMeta(".timer on"); err != nil {
		t.Fatalf("handleMeta(.timer on): %v", err)
	}
	if !cfg.Timer {
		t.Error("expected timer to be on")
	}

	buf.Reset()
	if err := r.handleMeta(".timer"); err != nil {
		t.Fatalf("handleMeta(.timer): %v", err)
	}
	if !strings.Contains(buf.String(), "on") {
		t.Errorf("expected timer status, got:\n%s", buf.String())
	}
}

func TestReplHandleMeta_Dump(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	if err := r.handleMeta(".dump users"); err != nil {
		t.Fatalf("handleMeta(.dump users): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("expected CREATE TABLE in dump, got:\n%s", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in dump, got:\n%s", out)
	}
}

func TestReplHandleMeta_Unknown(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default"}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	err := r.handleMeta(".nonexistent")
	if err == nil {
		t.Error("expected error for unknown meta command")
	}
	if !strings.Contains(fmt.Sprint(err), "unknown") {
		t.Errorf("expected 'unknown' in error, got: %v", err)
	}
}
