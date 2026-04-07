package adapter_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fsql/internal/adapter"
	tinysql "github.com/SimonWaldherr/tinySQL"
)

// noopResolver returns the path unchanged (for unit tests that provide absolute paths).
func noopResolver(s string) (string, error) { return filepath.Abs(s) }

func setup(t *testing.T) {
	t.Helper()
	adapter.RegisterAll(noopResolver)
}

// ─────────────────────────────────────────────────────────────────────────────
// files() tests
// ─────────────────────────────────────────────────────────────────────────────

func TestFilesFunc_Basic(t *testing.T) {
	setup(t)

	// Create a temporary directory with a couple of files
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.txt"), "hello")
	writeFile(t, filepath.Join(dir, "b.log"), "world")

	sql := fmt.Sprintf(`SELECT name, ext FROM files('%s')`, dir)
	rs := execQuery(t, sql)

	// The directory itself shows up as the first row; filter to files only
	var fileNames []string
	for _, r := range rs.Rows {
		if isDir, _ := r["is_dir"].(bool); !isDir {
			fileNames = append(fileNames, fmt.Sprintf("%v", r["name"]))
		}
	}

	assertContains(t, fileNames, "a.txt")
	assertContains(t, fileNames, "b.log")
}

func TestFilesFunc_Recursive(t *testing.T) {
	setup(t)

	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "root.txt"), "r")
	writeFile(t, filepath.Join(sub, "nested.txt"), "n")

	// Non-recursive: should not include nested.txt
	sql := fmt.Sprintf(`SELECT name FROM files('%s', false)`, dir)
	rs := execQuery(t, sql)
	nonRecNames := colStrValues(rs, "name")
	assertNotContains(t, nonRecNames, "nested.txt")

	// Recursive: should include nested.txt
	sql = fmt.Sprintf(`SELECT name FROM files('%s', true)`, dir)
	rs = execQuery(t, sql)
	recNames := colStrValues(rs, "name")
	assertContains(t, recNames, "nested.txt")
	assertContains(t, recNames, "root.txt")
}

func TestFilesFunc_Columns(t *testing.T) {
	setup(t)

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "test.csv"), "x,y\n1,2\n")

	sql := fmt.Sprintf(`SELECT path, name, size, ext, mod_time, is_dir FROM files('%s')`, dir)
	rs := execQuery(t, sql)

	expectedCols := []string{"path", "name", "size", "ext", "mod_time", "is_dir"}
	for _, expected := range expectedCols {
		found := false
		for _, got := range rs.Cols {
			if strings.ToLower(got) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected column %q in result, got: %v", expected, rs.Cols)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// lines() tests
// ─────────────────────────────────────────────────────────────────────────────

func TestLinesFunc_Basic(t *testing.T) {
	setup(t)

	f := writeTempFile(t, "line1\nline2\nline3\n")

	sql := fmt.Sprintf(`SELECT line_number, line FROM lines('%s')`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	if got := rs.Rows[0]["line"]; got != "line1" {
		t.Errorf("row 0 line: got %v, want line1", got)
	}
	if got := rs.Rows[2]["line_number"]; fmt.Sprintf("%v", got) != "3" {
		t.Errorf("row 2 line_number: got %v, want 3", got)
	}
}

func TestLinesFunc_Filter(t *testing.T) {
	setup(t)

	f := writeTempFile(t, "ERROR: disk full\nINFO: started\nERROR: timeout\n")

	sql := fmt.Sprintf(`SELECT line FROM lines('%s') WHERE line LIKE 'ERROR%%'`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 ERROR rows, got %d", len(rs.Rows))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// csv_rows() tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCSVRowsFunc_Basic(t *testing.T) {
	setup(t)

	f := writeTempFile(t, "id,name,score\n1,Alice,95\n2,Bob,87\n")

	sql := fmt.Sprintf(`SELECT id, name, score FROM csv_rows('%s')`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	if got := rs.Rows[0]["name"]; got != "Alice" {
		t.Errorf("row 0 name: got %v, want Alice", got)
	}
}

func TestCSVRowsFunc_NoHeader(t *testing.T) {
	setup(t)

	f := writeTempFile(t, "1,Alice\n2,Bob\n")

	sql := fmt.Sprintf(`SELECT col0, col1 FROM csv_rows('%s', false)`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	if got := rs.Rows[0]["col1"]; got != "Alice" {
		t.Errorf("row 0 col1: got %v, want Alice", got)
	}
}

func TestCSVRowsFunc_Filter(t *testing.T) {
	setup(t)

	f := writeTempFile(t, "name,score\nAlice,95\nBob,60\nCarol,88\n")

	// CSV values are strings; cast score to INT for numeric comparison
	sql := fmt.Sprintf(`SELECT name FROM csv_rows('%s') WHERE CAST(score AS INT) > 80`, f)
	rs := execQuery(t, sql)

	names := colStrValues(rs, "name")
	assertContains(t, names, "Alice")
	assertContains(t, names, "Carol")
	assertNotContains(t, names, "Bob")
}

// ─────────────────────────────────────────────────────────────────────────────
// json_rows() tests
// ─────────────────────────────────────────────────────────────────────────────

func TestJSONRowsFunc_ArrayOfObjects(t *testing.T) {
	setup(t)

	data := `[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`
	f := writeTempFile(t, data)

	sql := fmt.Sprintf(`SELECT id, name FROM json_rows('%s')`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	names := colStrValues(rs, "name")
	assertContains(t, names, "Alice")
	assertContains(t, names, "Bob")
}

func TestJSONRowsFunc_WithPath(t *testing.T) {
	setup(t)

	data := `{"data":{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]}}`
	f := writeTempFile(t, data)

	sql := fmt.Sprintf(`SELECT name FROM json_rows('%s', 'data.users')`, f)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: files JOIN csv_rows
// ─────────────────────────────────────────────────────────────────────────────

func TestFilesAndCSVJoin(t *testing.T) {
	setup(t)

	dir := t.TempDir()
	// Write two CSV files
	for _, name := range []string{"a.csv", "b.csv"} {
		w, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"val"})
		_ = cw.Write([]string{name})
		cw.Flush()
		w.Close()
	}

	// Query: list all CSV files in the directory
	sql := fmt.Sprintf(`SELECT name FROM files('%s') WHERE ext = 'csv'`, dir)
	rs := execQuery(t, sql)
	names := colStrValues(rs, "name")
	assertContains(t, names, "a.csv")
	assertContains(t, names, "b.csv")
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation tests
// ─────────────────────────────────────────────────────────────────────────────

func TestFilesCount(t *testing.T) {
	setup(t)

	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		writeFile(t, filepath.Join(dir, fmt.Sprintf("f%d.txt", i)), "x")
	}

	// Use an alias so the result column has a predictable name
	sql := fmt.Sprintf(`SELECT count(*) AS cnt FROM files('%s')`, dir)
	rs := execQuery(t, sql)

	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row from count, got %d", len(rs.Rows))
	}
	countVal := rs.Rows[0]["cnt"]
	if countVal == nil {
		t.Fatalf("cnt column not found in result: %v", rs.Rows[0])
	}
	// count(*) includes the directory itself, so expect >= 5
	n := int64(0)
	switch v := countVal.(type) {
	case int:
		n = int64(v)
	case int64:
		n = v
	case float64:
		n = int64(v)
	default:
		t.Fatalf("unexpected count type %T: %v", countVal, countVal)
	}
	if n < 5 {
		t.Errorf("expected count >= 5, got %d", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func execQuery(t *testing.T, sql string) *tinysql.ResultSet {
	t.Helper()
	db := tinysql.NewDB()
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse SQL %q: %v", sql, err)
	}
	rs, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("execute SQL %q: %v", sql, err)
	}
	return rs
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func colStrValues(rs *tinysql.ResultSet, col string) []string {
	out := make([]string, len(rs.Rows))
	for i, r := range rs.Rows {
		out[i] = fmt.Sprintf("%v", r[col])
	}
	return out
}

func assertContains(t *testing.T, haystack []string, needle string) {
	t.Helper()
	for _, s := range haystack {
		if s == needle {
			return
		}
	}
	t.Errorf("expected %q in %v", needle, haystack)
}

func assertNotContains(t *testing.T, haystack []string, needle string) {
	t.Helper()
	for _, s := range haystack {
		if s == needle {
			t.Errorf("did not expect %q in %v", needle, haystack)
			return
		}
	}
}

// Verify JSON marshaling works (used in printJSON)
func TestJSONOutputHelper(t *testing.T) {
	rs := &tinysql.ResultSet{
		Cols: []string{"name", "value"},
		Rows: []tinysql.Row{
			{"name": "Alice", "value": 42},
		},
	}
	out := make([]map[string]any, len(rs.Rows))
	for i, row := range rs.Rows {
		m := make(map[string]any, len(rs.Cols))
		for _, col := range rs.Cols {
			m[col] = row[col]
		}
		out[i] = m
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "Alice") {
		t.Errorf("expected Alice in JSON output: %s", data)
	}
}
