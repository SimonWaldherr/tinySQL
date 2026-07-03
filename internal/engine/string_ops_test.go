// Tests for the improved LIKE, BETWEEN, LTRIM/RTRIM/TRIM, and REGEXP
// behaviour: UTF-8 awareness, NULL semantics, escape backtracking,
// single-evaluation BETWEEN, and the shared regex cache.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func queryBool(t *testing.T, db *storage.DB, expr string) bool {
	t.Helper()
	rs := execSQL(t, db, `SELECT `+expr+` AS r`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row for %s", expr)
	}
	v := rs.Rows[0]["r"]
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("%s: expected bool, got %T (%v)", expr, v, v)
	}
	return b
}

func queryScalar(t *testing.T, db *storage.DB, expr string) any {
	t.Helper()
	rs := execSQL(t, db, `SELECT `+expr+` AS r`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row for %s", expr)
	}
	return rs.Rows[0]["r"]
}

func TestLikeUnicode(t *testing.T) {
	db := storage.NewDB()
	cases := []struct {
		expr string
		want bool
	}{
		// _ must match one character, not one byte.
		{`'héllo' LIKE 'h_llo'`, true},
		{`'日本語' LIKE '__語'`, true},
		{`'日本語' LIKE '_語'`, false},
		{`'héllo' LIKE 'h%o'`, true},
		// % backtracking across multi-byte characters.
		{`'ααβγγ' LIKE '%β%'`, true},
		{`'ααβγγ' LIKE '%δ%'`, false},
		// GLOB ? is also rune-aware.
		{`'über' GLOB '?ber'`, true},
	}
	for _, c := range cases {
		if got := queryBool(t, db, c.expr); got != c.want {
			t.Errorf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestLikeEscapeBacktracking(t *testing.T) {
	db := storage.NewDB()
	cases := []struct {
		expr string
		want bool
	}{
		// The escaped _ is a literal underscore; the match starts after
		// position 0, so the matcher must backtrack instead of failing.
		{`'a_b' LIKE '%\_%'`, true},
		{`'axb' LIKE '%\_%'`, false},
		{`'50% off' LIKE '%\%%'`, true},
		{`'50 off' LIKE '%\%%'`, false},
	}
	for _, c := range cases {
		if got := queryBool(t, db, c.expr); got != c.want {
			t.Errorf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestLikeNullSemantics(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT, s TEXT)`)
	execSQL(t, db, `INSERT INTO t (id, s) VALUES (1, 'abc')`)
	execSQL(t, db, `INSERT INTO t (id) VALUES (2)`) // s is NULL

	// NULL must not match LIKE '%' (previously nil stringified to "<nil>").
	rs := execSQL(t, db, `SELECT id FROM t WHERE s LIKE '%'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 matching row, got %d", len(rs.Rows))
	}
	if v := rs.Rows[0]["id"]; v != 1 && v != float64(1) {
		t.Fatalf("expected id=1, got %v", v)
	}
}

func TestBetweenSingleEvaluation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE n (v INT)`)
	for i := 1; i <= 5; i++ {
		execSQL(t, db, `INSERT INTO n (v) VALUES (`+string(rune('0'+i))+`)`)
	}

	// Function-call comparand goes through the single-eval BetweenExpr.
	rs := execSQL(t, db, `SELECT v FROM n WHERE ABS(v) BETWEEN 2 AND 4 ORDER BY v`)
	if len(rs.Rows) != 3 {
		t.Fatalf("BETWEEN on ABS(v): expected 3 rows, got %d", len(rs.Rows))
	}
	rs = execSQL(t, db, `SELECT v FROM n WHERE ABS(v) NOT BETWEEN 2 AND 4 ORDER BY v`)
	if len(rs.Rows) != 2 {
		t.Fatalf("NOT BETWEEN on ABS(v): expected 2 rows, got %d", len(rs.Rows))
	}

	// Column comparand keeps the desugared fast path.
	rs = execSQL(t, db, `SELECT v FROM n WHERE v BETWEEN 2 AND 4`)
	if len(rs.Rows) != 3 {
		t.Fatalf("BETWEEN on v: expected 3 rows, got %d", len(rs.Rows))
	}

	// RANDOM() BETWEEN 0 AND 1 must hold on every row: with the old
	// double-evaluation desugaring two different random values were compared
	// and this failed intermittently.
	for i := 0; i < 20; i++ {
		if !queryBool(t, db, `(RANDOM() BETWEEN 0.0 AND 1.0)`) {
			t.Fatal("RANDOM() BETWEEN 0 AND 1 was false — comparand evaluated twice?")
		}
	}
}

func TestTrimImprovements(t *testing.T) {
	db := storage.NewDB()

	// Unicode whitespace (U+00A0 non-breaking space) trimmed by default.
	if got := queryScalar(t, db, "LTRIM('  x')"); got != "x" {
		t.Errorf("LTRIM nbsp: got %q", got)
	}
	if got := queryScalar(t, db, "RTRIM('x ')"); got != "x" {
		t.Errorf("RTRIM nbsp: got %q", got)
	}
	if got := queryScalar(t, db, "TRIM(' x ')"); got != "x" {
		t.Errorf("TRIM nbsp: got %q", got)
	}

	// Cutset forms still work.
	if got := queryScalar(t, db, `LTRIM('00042', '0')`); got != "42" {
		t.Errorf("LTRIM cutset: got %q", got)
	}
	if got := queryScalar(t, db, `RTRIM('42000', '0')`); got != "42" {
		t.Errorf("RTRIM cutset: got %q", got)
	}
	if got := queryScalar(t, db, `TRIM('##x##', '#')`); got != "x" {
		t.Errorf("TRIM cutset: got %q", got)
	}

	// Non-string inputs are coerced instead of erroring.
	if got := queryScalar(t, db, `TRIM(42)`); got != "42" {
		t.Errorf("TRIM(42): got %v (%T)", got, got)
	}

	// NULL propagates.
	if got := queryScalar(t, db, `TRIM(NULL)`); got != nil {
		t.Errorf("TRIM(NULL): got %v", got)
	}
}

func TestRegexpCachedAndCorrect(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE logs (id INT, msg TEXT)`)
	for i := 0; i < 50; i++ {
		suffix := "ok"
		if i%10 == 0 {
			suffix = "error 500"
		}
		execSQL(t, db, `INSERT INTO logs (id, msg) VALUES (1, 'request `+suffix+`')`)
	}

	rs := execSQL(t, db, `SELECT msg FROM logs WHERE msg REGEXP 'error [0-9]+'`)
	if len(rs.Rows) != 5 {
		t.Fatalf("REGEXP: expected 5 rows, got %d", len(rs.Rows))
	}

	// The cache now holds the pattern; the same query must still work.
	rs = execSQL(t, db, `SELECT msg FROM logs WHERE msg REGEXP 'error [0-9]+'`)
	if len(rs.Rows) != 5 {
		t.Fatalf("REGEXP (cached): expected 5 rows, got %d", len(rs.Rows))
	}

	// Invalid pattern still errors cleanly.
	if _, err := Execute(context.Background(), db, "default", mustParse(`SELECT msg FROM logs WHERE msg REGEXP '['`)); err == nil {
		t.Fatal("expected error for invalid regexp")
	}

	// REGEXP_* functions.
	if got := queryScalar(t, db, `REGEXP_EXTRACT('order-1234', '[0-9]+')`); got != "1234" {
		t.Errorf("REGEXP_EXTRACT: got %v", got)
	}
	if got := queryScalar(t, db, `REGEXP_REPLACE('a1b2', '[0-9]', '#')`); got != "a#b#" {
		t.Errorf("REGEXP_REPLACE: got %v", got)
	}
	if !queryBool(t, db, `REGEXP_MATCH('hello42', '[a-z]+[0-9]+')`) {
		t.Error("REGEXP_MATCH: expected true")
	}
}
