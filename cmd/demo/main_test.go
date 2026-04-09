package main

import (
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

func TestShowcase(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=test_showcase")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	// runShowcase must not panic and must complete without errors printed to
	// stdout (we can't capture stdout easily here, but at least the function
	// must not hang or crash).
	runShowcase(db, false)
}

func TestBenchmark(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=test_bench")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	// Run a very short benchmark (3 iterations) to verify it doesn't crash.
	runBenchmark(db, 3)
}

func TestPrintRows(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=test_print")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE pr (id INT, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pr VALUES (1, 'hello')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT * FROM pr`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	// printRows must not panic
	printRows(rows, cols)
}

func TestPadRight(t *testing.T) {
	if got := padRight("hi", 5); got != "hi   " {
		t.Errorf("padRight(hi,5) = %q, want %q", got, "hi   ")
	}
	if got := padRight("hello", 3); !strings.HasPrefix(got, "hello") {
		t.Errorf("padRight should not truncate: got %q", got)
	}
}
