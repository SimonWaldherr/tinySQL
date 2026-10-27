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

	_ "github.com/SimonWaldherr/tinySQL/driver"
)

func TestREPLExecutesScriptsWithCommentsAndMultilineStrings(t *testing.T) {
	db := openTestDB(t)
	input := "CREATE TABLE repl_script (id INT, value TEXT); INSERT INTO repl_script VALUES (1, 'first');\n" +
		"INSERT INTO repl_script VALUES (2, -- keep this newline\n'second'); -- trailing comment\n" +
		"/* block comment */ INSERT INTO repl_script VALUES (3, 'third');\n" +
		"INSERT INTO repl_script VALUES (4, 'line one\n-- not a comment\nline three');\n" +
		"INSERT INTO repl_script VALUES (9876, 'returning') RETURNING id;\n"
	stdin, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if _, err = stdin.WriteString(input); err != nil {
		t.Fatal(err)
	}
	if _, err = stdin.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdin, stdout
	defer func() { os.Stdin, os.Stdout = oldIn, oldOut }()
	runREPL(db, false, "table", false, false, false)
	if _, err = stdout.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(stdout.Name())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output), "ERR") {
		t.Fatalf("REPL errors: %s", output)
	}
	if !strings.Contains(string(output), "9876") {
		t.Fatalf("RETURNING result missing from REPL output: %s", output)
	}
	for id, want := range map[int]string{1: "first", 2: "second", 3: "third", 4: "line one\n-- not a comment\nline three", 9876: "returning"} {
		var got string
		if err := db.QueryRow("SELECT value FROM repl_script WHERE id = ?", id).Scan(&got); err != nil || got != want {
			t.Errorf("row %d: got %q, err %v; want %q", id, got, err, want)
		}
	}
}

func TestBuildRepl(t *testing.T) {
	// Generous on purpose. This shells out to the real toolchain, the link step
	// is not cached, and "go test ./..." runs several of these concurrently, so a
	// tight budget fails on a busy machine while the build itself is fine. The
	// bound is here to catch a hung toolchain, not to police build speed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_repl_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestHandleMetaHelp(t *testing.T) {
	db := openTestDB(t)
	if !handleMeta(db, ".help") {
		t.Error("expected .help to be handled")
	}
}

func TestHandleMetaTables(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE test_tbl (x INT)"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".tables") {
		t.Error("expected .tables to be handled")
	}
}

func TestHandleMetaSchema(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE schema_tbl (id INT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".schema schema_tbl") {
		t.Error("expected .schema to be handled")
	}
}

func TestHandleMetaSchemaAll(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE sa1 (a INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE sa2 (b TEXT)"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".schema") {
		t.Error("expected .schema to be handled")
	}
}

func TestHandleMetaCount(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE cnt (x INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO cnt VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".count cnt") {
		t.Error("expected .count to be handled")
	}
}

func TestHandleMetaCountAll(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE ca1 (x INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO ca1 VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".count") {
		t.Error("expected .count to be handled")
	}
}

func TestHandleMetaDump(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE dmp (id INT, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO dmp VALUES (1, 'hello')"); err != nil {
		t.Fatal(err)
	}
	if !handleMeta(db, ".dump dmp") {
		t.Error("expected .dump to be handled")
	}
}

func TestHandleMetaRead(t *testing.T) {
	db := openTestDB(t)
	dir := t.TempDir()
	f := filepath.Join(dir, "test.sql")
	os.WriteFile(f, []byte("CREATE TABLE rd (x INT);\nINSERT INTO rd VALUES (42);"), 0644)
	if !handleMeta(db, ".read "+f) {
		t.Error("expected .read to be handled")
	}
	// Verify the table was created
	rows, err := db.Query("SELECT x FROM rd")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row from read file")
	}
}

func TestHandleMetaClear(t *testing.T) {
	db := openTestDB(t)
	if !handleMeta(db, ".clear") {
		t.Error("expected .clear to be handled")
	}
}

func TestHandleMetaUnknown(t *testing.T) {
	db := openTestDB(t)
	if handleMeta(db, ".nonexistent_command") {
		t.Error("expected unknown command to not be handled")
	}
}

func TestReplListTableNames(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE lt1 (a INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE lt2 (b INT)"); err != nil {
		t.Fatal(err)
	}
	names := replListTableNames(db)
	if len(names) < 2 {
		t.Fatalf("expected at least 2 tables, got %d", len(names))
	}
}

func TestReplShowSchema(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE ss_test (id INT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	// Should not panic
	replShowSchema(db, "ss_test")
}

func TestReplDumpTable(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("CREATE TABLE ddt (id INT, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO ddt VALUES (1, 'test')"); err != nil {
		t.Fatal(err)
	}
	// Should not panic
	replDumpTable(db, "ddt")
}
