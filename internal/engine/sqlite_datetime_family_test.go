// Tests for the SQLite-compatible builtins fixed together: HEX() over BLOBs,
// and the DATE/TIME/DATETIME/JULIANDAY/UNIXEPOCH/STRFTIME family's handling of
// the 'now' literal and of modifier arguments.
package engine

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// evalScalarSQL runs a single-column, single-row SELECT and returns the value.
func evalScalarSQL(t *testing.T, sql string) any {
	t.Helper()
	db := storage.NewDB()
	p := NewParser(sql)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	if len(rs.Rows) != 1 || len(rs.Cols) != 1 {
		t.Fatalf("%q: want 1 row / 1 column, got %d/%d", sql, len(rs.Rows), len(rs.Cols))
	}
	return rs.Rows[0][rs.Cols[0]]
}

// expectSQLError runs a SELECT that must fail and returns the error message.
func expectSQLError(t *testing.T, sql string) string {
	t.Helper()
	db := storage.NewDB()
	p := NewParser(sql)
	st, err := p.ParseStatement()
	if err != nil {
		// A parse error is still a refusal, which is what these cases assert.
		return err.Error()
	}
	if _, err := Execute(context.Background(), db, "default", st); err != nil {
		return err.Error()
	}
	t.Fatalf("%q: expected an error, got none", sql)
	return ""
}

// TestHexOfBlobHexesTheBytes pins the fix for HEX() over BLOBs. It used to
// render the value with fmt's %v first — whose []byte form is the decimal byte
// list "[1 2]" — and hex *that*, so hex(X'0102') returned 5B3120325D (the hex
// of "[1 2]", brackets and space included) instead of 0102.
func TestHexOfBlobHexesTheBytes(t *testing.T) {
	cases := []struct {
		sql  string
		want any
	}{
		{"SELECT HEX(X'0102') AS h", "0102"},
		{"SELECT HEX(X'deadbeef') AS h", "DEADBEEF"},
		{"SELECT HEX(X'') AS h", ""},
		// Non-blob input keeps SQLite's definition: the hex of the UTF-8
		// bytes of the value's text rendering. So a *string* of hex digits is
		// hexed as those characters (unlike BLOB_HEX, which decodes hex text),
		// and a number is hexed as its digits, not as its binary value.
		{"SELECT HEX('AB') AS h", "4142"},
		{"SELECT HEX('0102') AS h", "30313032"},
		{"SELECT HEX(123) AS h", "313233"},
		// NULL propagates, like MD5/SHA*/BASE64 in the same file. This is a
		// deliberate deviation from SQLite, whose hex(NULL) is '' because it
		// casts NULL to an empty blob; the old behaviour here was the hex of
		// the Go string "<nil>" (3C6E696C3E), which matched neither.
		{"SELECT HEX(NULL) AS h", nil},
	}
	for _, c := range cases {
		if got := evalScalarSQL(t, c.sql); got != c.want {
			t.Errorf("%s = %#v, want %#v", c.sql, got, c.want)
		}
	}
}

// TestHexOfBlobColumn covers the same fix for a value arriving from a BLOB
// column rather than from an X'...' literal — the two share evalHex, but only
// the column form goes through storage round-tripping.
func TestHexOfBlobColumn(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE files (id INT, data BLOB)",
		"INSERT INTO files VALUES (1, X'00ff10')",
	} {
		p := NewParser(sql)
		st, err := p.ParseStatement()
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := Execute(ctx, db, "default", st); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	p := NewParser("SELECT HEX(data) AS h FROM files WHERE id = 1")
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rs.Rows))
	}
	if got := rs.Rows[0]["h"]; got != "00FF10" {
		t.Errorf("HEX(blob column) = %#v, want \"00FF10\"", got)
	}
}

// TestDateTimeFunctionsRejectModifiers pins the fix for the silent-swallow bug:
// every function in the SQLite date/time family used to read its time value and
// return, discarding modifier arguments, so date('2024-01-01', '+1 day')
// answered "2024-01-01" — a plausible-looking wrong date with nothing in the
// result to hint that a day was meant to be added. The modifier language is not
// implemented; the functions must now refuse rather than mislead.
func TestDateTimeFunctionsRejectModifiers(t *testing.T) {
	for _, sql := range []string{
		"SELECT DATE('2024-01-01', '+1 day') AS d",
		"SELECT TIME('2024-01-01 10:00:00', '+1 hour') AS d",
		"SELECT DATETIME('2024-01-01', 'start of month') AS d",
		"SELECT JULIANDAY('2024-01-01', '+1 day') AS d",
		"SELECT UNIXEPOCH('2024-01-01', 'utc') AS d",
		"SELECT STRFTIME('%Y', '2024-01-01', '+1 day') AS d",
		// The clock functions take no arguments at all, and used to ignore
		// anything passed to them for the same reason.
		"SELECT NOW('+1 day') AS d",
		"SELECT CURRENT_TIMESTAMP('+1 day') AS d",
		"SELECT CURRENT_DATE('+1 day') AS d",
		"SELECT CURRENT_TIME('+1 hour') AS d",
		"SELECT TODAY('+1 day') AS d",
	} {
		msg := expectSQLError(t, sql)
		if !strings.Contains(msg, "not supported") {
			t.Errorf("%s: error %q should say modifiers are not supported", sql, msg)
		}
		if !strings.Contains(msg, "DATE_ADD") {
			t.Errorf("%s: error %q should point at DATE_ADD/DATE_SUB", sql, msg)
		}
	}
}

// TestDateTimeFamilyFormats pins the SQLite return formats for the newly
// registered functions and their siblings. DATETIME returns fixed-width text
// rather than a time.Time on purpose: NOW()/DATE_ADD() return time.Time and
// render with Go's nanosecond-and-zone form, which is not what a caller writing
// datetime(...) asked for.
func TestDateTimeFamilyFormats(t *testing.T) {
	// Bare date and datetime literals parse as UTC (time.Parse with no zone),
	// so these expectations are machine-timezone independent.
	cases := []struct {
		sql  string
		want any
	}{
		{"SELECT DATE('2024-03-05 06:07:08') AS v", "2024-03-05"},
		{"SELECT TIME('2024-03-05 06:07:08') AS v", "06:07:08"},
		{"SELECT DATETIME('2024-03-05 06:07:08') AS v", "2024-03-05 06:07:08"},
		{"SELECT DATETIME('2024-03-05') AS v", "2024-03-05 00:00:00"},
		{"SELECT UNIXEPOCH('2024-01-01') AS v", int64(1704067200)},
	}
	for _, c := range cases {
		if got := evalScalarSQL(t, c.sql); got != c.want {
			t.Errorf("%s = %#v (%T), want %#v", c.sql, got, got, c.want)
		}
	}

	// JULIANDAY is a float: 1970-01-01T00:00:00Z is JD 2440587.5, and
	// 2024-01-01T00:00:00Z is 19723 days later.
	jd, ok := evalScalarSQL(t, "SELECT JULIANDAY('2024-01-01') AS v").(float64)
	if !ok {
		t.Fatalf("JULIANDAY should return float64")
	}
	if math.Abs(jd-2460310.5) > 1e-6 {
		t.Errorf("JULIANDAY('2024-01-01') = %v, want 2460310.5", jd)
	}
}

// TestNowLiteralIsAccepted pins support for SQLite's 'now' time string, which
// used to fail with "cannot parse 'now' as datetime" in every function of the
// family. 'now' here is the local wall clock, matching NOW()/CURRENT_TIMESTAMP
// in this engine rather than SQLite's UTC default — see the family comment in
// builtin_datetime.go for why the engine's own clock wins over sqlite3 parity.
func TestNowLiteralIsAccepted(t *testing.T) {
	before := time.Now()
	if got := evalScalarSQL(t, "SELECT DATE('now') AS v"); got != before.Format("2006-01-02") {
		// Only fails across a midnight boundary, which the second read below
		// would show as a same-day value; accept the day after too.
		if got != time.Now().Format("2006-01-02") {
			t.Errorf("DATE('now') = %#v, want today's date", got)
		}
	}
	// Case-insensitive and whitespace-tolerant, like SQLite's own time strings.
	for _, sql := range []string{
		"SELECT DATETIME('now') AS v",
		"SELECT DATETIME('NOW') AS v",
		"SELECT DATETIME(' now ') AS v",
	} {
		got, ok := evalScalarSQL(t, sql).(string)
		if !ok {
			t.Fatalf("%s should return a string", sql)
		}
		parsed, err := time.ParseInLocation("2006-01-02 15:04:05", got, time.Local)
		if err != nil {
			t.Fatalf("%s = %q, not a datetime: %v", sql, got, err)
		}
		if d := time.Since(parsed); d < -time.Minute || d > time.Minute {
			t.Errorf("%s = %q, which is %v away from now", sql, got, d)
		}
	}
	// The zero-argument form defaults to 'now' as well.
	unix, ok := evalScalarSQL(t, "SELECT UNIXEPOCH() AS v").(int64)
	if !ok {
		t.Fatalf("UNIXEPOCH() should return int64")
	}
	if delta := time.Now().Unix() - unix; delta < -60 || delta > 60 {
		t.Errorf("UNIXEPOCH() = %d, %d seconds away from now", unix, delta)
	}
	// STRFTIME's time value is its second argument, so 'now' has to be
	// recognized there too.
	if got := evalScalarSQL(t, "SELECT STRFTIME('%Y', 'now') AS v"); got != before.Format("2006") {
		t.Errorf("STRFTIME('%%Y', 'now') = %#v, want %q", got, before.Format("2006"))
	}
}

// TestCurrentTimeIsTimeOfDay pins CURRENT_TIME as time-of-day text. It was
// aliased to NOW() and returned a full timestamp, so it carried the very date
// component its name says it drops and was indistinguishable from
// CURRENT_TIMESTAMP. It is the zero-argument shorthand for TIME(), and now
// matches it.
func TestCurrentTimeIsTimeOfDay(t *testing.T) {
	got, ok := evalScalarSQL(t, "SELECT CURRENT_TIME() AS v").(string)
	if !ok {
		t.Fatalf("CURRENT_TIME() should return a string, got %T",
			evalScalarSQL(t, "SELECT CURRENT_TIME() AS v"))
	}
	if _, err := time.Parse("15:04:05", got); err != nil {
		t.Errorf("CURRENT_TIME() = %q, want HH:MM:SS: %v", got, err)
	}
	// CURRENT_TIMESTAMP deliberately still returns a time.Time — this engine's
	// temporal values are time.Time (see NOW(), DATE_ADD()), and only the
	// SQLite-named renderers (DATE/TIME/DATETIME/CURRENT_TIME) return text.
	if _, ok := evalScalarSQL(t, "SELECT CURRENT_TIMESTAMP() AS v").(time.Time); !ok {
		t.Errorf("CURRENT_TIMESTAMP() should still return a time.Time")
	}
}
