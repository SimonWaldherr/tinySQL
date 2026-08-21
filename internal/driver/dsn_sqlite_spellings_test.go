// DSN spellings a SQLite migrant actually types: `:memory:`, a bare path, and
// a mixed-case scheme. The important negative case is that an explicit but
// UNKNOWN scheme must stay an error rather than becoming a filename.
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseDSNMemorySpellings(t *testing.T) {
	// Every one of these must mean "in memory": no file path, no storage mode.
	for _, dsn := range []string{
		"mem://",
		"MEM://",
		"Mem://?tenant=t1",
		":memory:",
		":MEMORY:",
		":memory:?tenant=t1",
		"file::memory:",
		"FILE::memory:",
		"file::memory:?tenant=t1",
	} {
		c, err := parseDSN(dsn)
		if err != nil {
			t.Errorf("parseDSN(%q) returned error: %v", dsn, err)
			continue
		}
		if c.filePath != "" {
			t.Errorf("parseDSN(%q) filePath = %q, want empty (in-memory)", dsn, c.filePath)
		}
		if c.modeSet {
			t.Errorf("parseDSN(%q) set a storage mode, want unset", dsn)
		}
		if c.defaultDSN {
			t.Errorf("parseDSN(%q) marked defaultDSN; only the empty DSN may inherit SetDefaultDB", dsn)
		}
	}
}

// TestParseDSNFileSpellings covers the file-backed forms, including a bare
// path with no scheme at all — what every Go SQLite driver accepts — and a
// Windows drive letter, which must NOT be mistaken for a one-letter scheme.
func TestParseDSNFileSpellings(t *testing.T) {
	cases := []struct{ dsn, wantPath string }{
		{"file:./test.db", filepath.Clean("./test.db")},
		{"FILE:./test.db", filepath.Clean("./test.db")},
		{"File:/var/lib/app.db", filepath.Clean("/var/lib/app.db")},
		{"file:///var/lib/app.db", filepath.Clean("/var/lib/app.db")},
		{"./app.db", filepath.Clean("./app.db")},
		{"app.db", "app.db"},
		{"/var/lib/app.db", filepath.Clean("/var/lib/app.db")},
		{"data/sub/app.db", filepath.Clean("data/sub/app.db")},
		{"C:/tmp/app.db", filepath.Clean("C:/tmp/app.db")},
		{`C:\tmp\app.db`, filepath.Clean(`C:\tmp\app.db`)},
		{"./app.db?tenant=t2", filepath.Clean("./app.db")},
	}
	for _, tc := range cases {
		c, err := parseDSN(tc.dsn)
		if err != nil {
			t.Errorf("parseDSN(%q) returned error: %v", tc.dsn, err)
			continue
		}
		if c.filePath != tc.wantPath {
			t.Errorf("parseDSN(%q) filePath = %q, want %q", tc.dsn, c.filePath, tc.wantPath)
		}
	}

	// The query really is applied on the bare-path form, not just accepted.
	c, err := parseDSN("./app.db?tenant=t2&mode=json")
	if err != nil {
		t.Fatalf("parseDSN with options returned error: %v", err)
	}
	if c.tenant != "t2" || !c.modeSet {
		t.Fatalf("bare-path options not applied: tenant=%q modeSet=%v", c.tenant, c.modeSet)
	}
}

// TestParseDSNUnknownSchemeIsNotAPath is the load-bearing negative test. A
// typo'd scheme (`men://` for `mem://`) must be rejected; if it were treated as
// a relative path the driver would silently create a database in a directory
// literally named "men:" and the caller would never find their data.
func TestParseDSNUnknownSchemeIsNotAPath(t *testing.T) {
	for _, dsn := range []string{
		"men://x",
		"mmem://x",
		"custom://path",
		"http://example.com/db",
		"sqlite3:/tmp/app.db",
		"postgres://localhost/db",
	} {
		c, err := parseDSN(dsn)
		if err == nil {
			t.Errorf("parseDSN(%q) succeeded with filePath=%q, want an error", dsn, c.filePath)
			continue
		}
		if !strings.Contains(err.Error(), "scheme") {
			t.Errorf("parseDSN(%q) error = %v, want it to name the unsupported scheme", dsn, err)
		}
	}

	// An explicit file: scheme with nothing after it is still an error; the
	// bare-path rule must not turn it into the current directory.
	if _, err := parseDSN("file:"); err == nil {
		t.Fatal("expected error for missing file path")
	}
}

func TestSplitDSNScheme(t *testing.T) {
	cases := []struct{ in, scheme, rest string }{
		{"mem://", "mem", ""},
		{"MEM://?x=1", "mem", "?x=1"},
		{"file:./a.db", "file", "./a.db"},
		{"file:///tmp/a.db", "file", "/tmp/a.db"},
		{"file::memory:", "file", ":memory:"},
		// Single-letter "schemes" are Windows drive letters, never schemes.
		{"C:/tmp/a.db", "", "C:/tmp/a.db"},
		{`c:\tmp\a.db`, "", `c:\tmp\a.db`},
		// No colon at all, or a colon that is not right after the scheme word.
		{"./a.db", "", "./a.db"},
		{":memory:", "", ":memory:"},
		{"data/a:b.db", "", "data/a:b.db"},
		{"custom://x", "custom", "x"},
	}
	for _, tc := range cases {
		scheme, rest := splitDSNScheme(tc.in)
		if scheme != tc.scheme || rest != tc.rest {
			t.Errorf("splitDSNScheme(%q) = (%q, %q), want (%q, %q)", tc.in, scheme, rest, tc.scheme, tc.rest)
		}
	}
}

// TestFileMemoryDSNCreatesNoFile pins the file::memory: decision end to end:
// it used to create a real file named ":memory:" in the working directory. Now
// it is an in-memory database and the filesystem is left untouched.
func TestFileMemoryDSNCreatesNoFile(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	db, err := sql.Open("tinysql", "file::memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE mem_only (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO mem_only VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM mem_only").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("file::memory: created %q; it must not touch the filesystem", e.Name())
	}
}

// TestBarePathDSNPersistsAndReopens proves the bare-path form is a real file
// DSN and not merely parsed: data written through it survives a reopen.
func TestBarePathDSNPersistsAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bare.gob")

	db, err := sql.Open("tinysql", path+"?autosave=1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE bare (id INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO bare VALUES (42)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := sql.Open("tinysql", path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	var id int
	if err := db2.QueryRow("SELECT id FROM bare").Scan(&id); err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if id != 42 {
		t.Fatalf("id = %d, want 42", id)
	}
}
