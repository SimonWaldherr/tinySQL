package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	_ "modernc.org/sqlite"
)

// newAwkwardValueSource holds values a source database yields happily but that
// rendering into SQL text cannot represent: IEEE infinities (PostgreSQL's
// double precision and SQLite's REAL both store them), plus an apostrophe and
// an injection-shaped string.
func newAwkwardValueSource(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	src, err := sql.Open("sqlite", filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	for _, q := range []string{
		`CREATE TABLE readings (id INTEGER, sensor TEXT, value REAL)`,
		`INSERT INTO readings VALUES (1, 'ok', 1.5)`,
		`INSERT INTO readings VALUES (2, 'pos-inf', 9e999)`,
		`INSERT INTO readings VALUES (3, 'neg-inf', -9e999)`,
		`INSERT INTO readings VALUES (4, 'it''s', 4.25)`,
		`INSERT INTO readings VALUES (5, '''); DROP TABLE readings; --', 5.75)`,
	} {
		if _, err := src.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return src
}

// Rows used to be dropped silently: each failure was logged only under
// -verbose and then skipped, and the function returned the successful count
// with a nil error, so a partial import was indistinguishable from a complete
// one at every call site.
//
// The rows that failed were not actually bad. formatValue rendered a
// non-finite float as the bare token +Inf, which the parser then read as a
// column reference. Building the INSERT as an AST removes the SQL-text round
// trip, so every row lands -- and source values stop being interpolated into
// SQL at all.
func TestImportFromExternalKeepsValuesSQLTextCannotRepresent(t *testing.T) {
	src := newAwkwardValueSource(t)
	db := tinysql.NewDB()

	stats, err := importFromExternal(db, context.Background(), "default", src,
		"SELECT * FROM readings", "readings")
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if stats.Imported != 5 || stats.Skipped != 0 {
		t.Fatalf("imported=%d skipped=%d, want 5 and 0: %v", stats.Imported, stats.Skipped, stats.Errors)
	}

	stmt, err := tinysql.ParseSQL("SELECT id, value FROM readings ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	rs, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 5 {
		t.Fatalf("landed %d rows, want 5", len(rs.Rows))
	}

	// The infinities must survive as infinities, not as NULL or 0.
	got := map[int]float64{}
	for _, r := range rs.Rows {
		id, _ := tinysql.GetVal(r, "id")
		v, _ := tinysql.GetVal(r, "value")
		idf, _ := importTestFloat(id)
		vf, _ := importTestFloat(v)
		got[int(idf)] = vf
	}
	if !math.IsInf(got[2], 1) {
		t.Errorf("row 2 value = %v, want +Inf", got[2])
	}
	if !math.IsInf(got[3], -1) {
		t.Errorf("row 3 value = %v, want -Inf", got[3])
	}
}

func importTestFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// A skipped row is data loss, so the report must never be gated on -verbose,
// and it must account for every row rather than only the sampled ones.
func TestReportImportSkipsIsUnconditionalAndComplete(t *testing.T) {
	stats := importStats{Imported: 3}
	for i := 1; i <= 9; i++ {
		stats.note(i, "insert", fmt.Errorf("boom %d", i))
	}
	if stats.Skipped != 9 {
		t.Fatalf("Skipped = %d, want 9", stats.Skipped)
	}
	if len(stats.Errors) != importErrorSample {
		t.Fatalf("kept %d sample messages, want %d", len(stats.Errors), importErrorSample)
	}

	var sb strings.Builder
	reportImportSkips(&sb, stats, "readings")
	out := sb.String()
	for _, want := range []string{"9 of 12", "readings", "source row 1", "and 4 more"} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}

	// A clean import stays quiet.
	sb.Reset()
	reportImportSkips(&sb, importStats{Imported: 5}, "readings")
	if sb.String() != "" {
		t.Errorf("a clean import printed %q", sb.String())
	}
}
