// Tests for the || string concatenation operator: lexing, precedence, and its
// SQLite NULL semantics (which differ deliberately from the CONCAT function).
package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestConcatOperatorBasics covers the operator that did not exist at all
// before: '|' had no case in tokenizeSymbol, so "SELECT 'a' || 'b'" failed
// with `near "|": unexpected token after statement` and blocked essentially
// every migrated statement that builds a string.
func TestConcatOperatorBasics(t *testing.T) {
	db := storage.NewDB()
	cases := []struct {
		expr string
		want any
	}{
		{`'a' || 'b'`, "ab"},
		// Left-associative chaining, and whitespace-insensitive lexing.
		{`'a' || 'b' || 'c'`, "abc"},
		{`'a'||'b'`, "ab"},
		// Non-text operands coerce to text, as in SQLite.
		{`1 || 2`, "12"},
		{`'x=' || 3.5`, "x=3.5"},
		{`'b=' || true`, "b=true"},
		// Same precedence tier as + and -, and left-associative, so this
		// groups as ('n=' || 1) - the grouping SQLite produces too (SQLite
		// binds || tighter than + and -, which cannot change a left-to-right
		// chain of them).
		{`'n=' || 1`, "n=1"},
	}
	for _, tc := range cases {
		if got := queryScalar(t, db, tc.expr); got != tc.want {
			t.Errorf("%s = %#v, want %#v", tc.expr, got, tc.want)
		}
	}
}

// TestConcatOperatorPropagatesNull pins the one semantic choice in ||: SQLite's
// operator yields NULL when either side is NULL, while SQLite's CONCAT()
// function (3.44+, which tinySQL's CONCAT matches) skips NULL arguments. The
// operator therefore does NOT route through the CONCAT builtin — reusing it
// would turn "first || middle || last" from "NULL until every part is present"
// into a silently gap-filled string.
func TestConcatOperatorPropagatesNull(t *testing.T) {
	db := storage.NewDB()
	for _, expr := range []string{`'a' || NULL`, `NULL || 'a'`, `NULL || NULL`, `'a' || NULL || 'b'`} {
		if got := queryScalar(t, db, expr); got != nil {
			t.Errorf("%s = %#v, want NULL", expr, got)
		}
	}
	// The divergence is deliberate, so the CONCAT function's NULL-skipping
	// behaviour is asserted alongside it: if these two ever agree again, one
	// of them has silently changed.
	if got := queryScalar(t, db, `CONCAT('a', NULL, 'b')`); got != "ab" {
		t.Errorf(`CONCAT('a', NULL, 'b') = %#v, want "ab" (function skips NULLs)`, got)
	}
}

// TestConcatOperatorOverColumns exercises || against real column values. It
// matters that this path works even though the raw fast paths only evaluate
// arithmetic and comparison operators: isSimpleRawExpr rejects an unknown
// binary operator, so a projection or predicate containing || falls back to
// the general evaluator instead of erroring inside a fast path.
func TestConcatOperatorOverColumns(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE people (first TEXT, last TEXT, age INT)`)
	execSQL(t, db, `INSERT INTO people (first, last, age) VALUES ('Ada', 'Lovelace', 36)`)
	execSQL(t, db, `INSERT INTO people (first, last, age) VALUES ('Grace', NULL, 45)`)

	rs := execSQL(t, db, `SELECT first || ' ' || last AS full FROM people ORDER BY first`)
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %#v", rs.Rows)
	}
	if rs.Rows[0]["full"] != "Ada Lovelace" {
		t.Errorf("full = %#v, want \"Ada Lovelace\"", rs.Rows[0]["full"])
	}
	if rs.Rows[1]["full"] != nil {
		t.Errorf("full with NULL last = %#v, want NULL", rs.Rows[1]["full"])
	}

	// In a WHERE clause (also a fast-path fallback) and with a non-text column.
	rs = execSQL(t, db, `SELECT first FROM people WHERE first || '/' || age = 'Ada/36'`)
	if len(rs.Rows) != 1 || rs.Rows[0]["first"] != "Ada" {
		t.Fatalf("|| in WHERE returned %#v", rs.Rows)
	}
}

// TestConcatOperatorRendersBlobAsText pins the text coercion used for a BLOB:
// SQLite reads the bytes as text for ||, which is what stringifySQLValue does.
// The general-purpose valueText helper would render []byte through fmt's %v as
// the decimal byte list "[65 66]" instead — that is why || does not use it.
func TestConcatOperatorRendersBlobAsText(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE b (payload BLOB)`)
	execSQL(t, db, `INSERT INTO b (payload) VALUES (X'4142')`)
	if got := execSQL(t, db, `SELECT payload || '!' AS r FROM b`).Rows[0]["r"]; got != "AB!" {
		t.Fatalf("blob || text = %#v, want \"AB!\"", got)
	}
}

// TestSinglePipeStillFails guards the other half of the lexer change: '||' is
// an operator, but a lone '|' is not one tinySQL has (there is no bitwise OR),
// so it must keep producing a parse error rather than being consumed as if it
// were a concatenation.
func TestSinglePipeStillFails(t *testing.T) {
	stmt, err := NewParser(`SELECT 'a' | 'b'`).ParseStatement()
	if err == nil {
		t.Fatalf("single '|' parsed as %#v, want a parse error", stmt)
	}
	if !strings.Contains(err.Error(), "|") {
		t.Fatalf("single '|' error = %v, want it to name the offending token", err)
	}
}
