package main

import (
	"bytes"
	"context"
	"database/sql"
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
	exec := newExecutor(db, false, true)
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
	exec := newExecutor(db, false, true)
	seedSampleData(exec)
	// Running the tour must not panic or return errors.
	runFeatureTour(exec)
}

func TestRunScript(t *testing.T) {
	db := openMemDB(t)
	exec := newExecutor(db, false, true)

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
	exec := newExecutor(db, false, true)

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
	exec := newExecutor(db, true, false)
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
