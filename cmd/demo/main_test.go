package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

func TestBuildDemo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_demo_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

// openMemDB opens a fresh in-memory tinySQL database for testing.
func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSeedSampleData(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "table")
	seedSampleData(exec)

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("count users: %v", err)
	}
	defer rows.Close()
	var cnt int
	rows.Next()
	if err := rows.Scan(&cnt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("expected 3 users, got %d", cnt)
	}
}

func TestFeatureTour(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "table")
	seedSampleData(exec)
	// Running the tour must not panic or return errors.
	runFeatureTour(exec)
}

func TestRunScript(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "table")

	script := `CREATE TABLE t (x INT);
INSERT INTO t VALUES (42);
SELECT x FROM t;`

	if err := runScript(exec, strings.NewReader(script), "<test>"); err != nil {
		t.Fatalf("runScript: %v", err)
	}

	rows, err := db.Query("SELECT x FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var x int
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	if err := rows.Scan(&x); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if x != 42 {
		t.Fatalf("expected x=42, got %d", x)
	}
}

func TestRunScriptFile(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "table")

	f, err := os.CreateTemp(t.TempDir(), "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.WriteString("CREATE TABLE temp_test (id INT);\nINSERT INTO temp_test VALUES (7);\n")
	f.Close()

	if err := runScriptFile(exec, f.Name()); err != nil {
		t.Fatalf("runScriptFile: %v", err)
	}

	rows, err := db.Query("SELECT id FROM temp_test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
}

func TestPrintRows(t *testing.T) {
	db := openMemDB(t)
	if _, err := db.Exec("CREATE TABLE pr_test (a TEXT, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO pr_test VALUES ('hello', 1)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT a, b FROM pr_test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var buf bytes.Buffer
	printRows(&buf, rows, cols)

	out := buf.String()
	if !strings.Contains(out, "hello") {
		t.Errorf("expected 'hello' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "1 row(s)") {
		t.Errorf("expected row count in output, got:\n%s", out)
	}
}

func TestExecutorTimer(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, true, false, "table")
	if err := exec.run("CREATE TABLE timer_test (x INT)"); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Timer flag must not cause a crash; just verify no error.
}

func TestPadRight(t *testing.T) {
	tests := []struct {
		s    string
		w    int
		want string
	}{
		{"hi", 5, "hi   "},
		{"hello", 3, "hello"},
		{"", 2, "  "},
	}
	for _, tc := range tests {
		if got := padRight(tc.s, tc.w); got != tc.want {
			t.Errorf("padRight(%q,%d)=%q want %q", tc.s, tc.w, got, tc.want)
		}
	}
}

// ---- Tests for new features ----

func TestOutputCSV(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "csv")
	if _, err := db.Exec("CREATE TABLE csv_test (a TEXT, b INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO csv_test VALUES ('hello', 42)"); err != nil {
		t.Fatal(err)
	}
	// CSV output should not panic
	if err := exec.run("SELECT a, b FROM csv_test"); err != nil {
		t.Fatalf("run csv: %v", err)
	}
}

func TestOutputJSON(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true, "json")
	if _, err := db.Exec("CREATE TABLE json_test (name TEXT, val INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO json_test VALUES ('Alice', 10)"); err != nil {
		t.Fatal(err)
	}
	// JSON output should not panic
	if err := exec.run("SELECT name, val FROM json_test"); err != nil {
		t.Fatalf("run json: %v", err)
	}
}

func TestPrintRowsJSON(t *testing.T) {
	db := openMemDB(t)
	if _, err := db.Exec("CREATE TABLE jtest (x TEXT, y INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO jtest VALUES ('foo', 99)"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT x, y FROM jtest")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var buf bytes.Buffer
	printRowsJSON(&buf, rows, cols)
	var result []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\nOutput: %s", err, buf.String())
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
}

func TestImportCSV(t *testing.T) {
	db := openMemDB(t)
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "test.csv")
	os.WriteFile(csvFile, []byte("name,age\nAlice,30\nBob,25\n"), 0644)

	if err := importFile(db, csvFile, "people"); err != nil {
		t.Fatalf("importFile CSV: %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM people")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	defer rows.Close()
	var cnt int
	rows.Next()
	rows.Scan(&cnt)
	if cnt != 2 {
		t.Fatalf("expected 2 rows, got %d", cnt)
	}
}

func TestImportJSON(t *testing.T) {
	db := openMemDB(t)
	dir := t.TempDir()
	jsonFile := filepath.Join(dir, "test.json")
	os.WriteFile(jsonFile, []byte(`[{"x":"hello","y":1},{"x":"world","y":2}]`), 0644)

	if err := importFile(db, jsonFile, "jdata"); err != nil {
		t.Fatalf("importFile JSON: %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM jdata")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	defer rows.Close()
	var cnt int
	rows.Next()
	rows.Scan(&cnt)
	if cnt != 2 {
		t.Fatalf("expected 2 rows, got %d", cnt)
	}
}

func TestDumpTable(t *testing.T) {
	db := openMemDB(t)
	if _, err := db.Exec("CREATE TABLE dump_test (id INT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO dump_test VALUES (1, 'Alice')"); err != nil {
		t.Fatal(err)
	}
	// dumpTable should not panic
	dumpTable(db, "dump_test")
}

func TestSanitizeIdent(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Name", "name"},
		{"First Name", "first_name"},
		{"123col", "_123col"},
		{"a-b.c", "a_b_c"},
		{"", "col"},
	}
	for _, tc := range tests {
		got := sanitizeIdent(tc.in)
		if got != tc.want {
			t.Errorf("sanitizeIdent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPrintTableCount(t *testing.T) {
	db := openMemDB(t)
	if _, err := db.Exec("CREATE TABLE cnt_test (x INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO cnt_test VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO cnt_test VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	// Must not panic
	printTableCount(db, "cnt_test")
}
