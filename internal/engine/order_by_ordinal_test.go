package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ordinalTestDB(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE o (a INT, b TEXT)`)
	execSQL(t, db, `INSERT INTO o VALUES (3,'c'),(1,'a'),(2,'b')`)
	return db
}

// A positional ORDER BY must sort by the same column the equivalent named form
// does. Ordinals address the SELECT list, so the pairs below are the same query
// written two ways.
func TestOrderByOrdinalMatchesNamedColumn(t *testing.T) {
	db := ordinalTestDB(t)
	execSQL(t, db, `CREATE TABLE g (k TEXT)`)
	execSQL(t, db, `INSERT INTO g VALUES ('a'),('b'),('b'),('c'),('c'),('c')`)

	cases := []struct{ ordinal, named string }{
		{`SELECT a, b FROM o ORDER BY 1`, `SELECT a, b FROM o ORDER BY a`},
		{`SELECT a, b FROM o ORDER BY 2 DESC`, `SELECT a, b FROM o ORDER BY b DESC`},
		// The ordinal resolves to the alias, not the underlying column name.
		{`SELECT a AS x, b FROM o ORDER BY 1 DESC`, `SELECT a AS x, b FROM o ORDER BY x DESC`},
		// An aggregate is addressable positionally without needing an alias in
		// the ORDER BY text.
		{`SELECT k, COUNT(*) c FROM g GROUP BY k ORDER BY 2 DESC`,
			`SELECT k, COUNT(*) c FROM g GROUP BY k ORDER BY c DESC`},
	}

	for _, c := range cases {
		got := rowsAsKeys(execSQL(t, db, c.ordinal))
		want := rowsAsKeys(execSQL(t, db, c.named))
		if strings.Join(got, ";") != strings.Join(want, ";") {
			t.Errorf("%s\n  ordinal form gave %v\n  named form gave   %v", c.ordinal, got, want)
		}
	}
}

// An unresolvable ordinal is a parse error rather than a silently wrong sort.
func TestOrderByOrdinalRejectsUnresolvable(t *testing.T) {
	cases := []struct{ query, wantSubstring string }{
		{`SELECT a, b FROM o ORDER BY 5`, "out of range"},
		{`SELECT a, b FROM o ORDER BY 0`, "out of range"},
		{`SELECT a, b FROM o ORDER BY 1.5`, "whole number"},
		// The parser does not know how wide * is, so it cannot say which output
		// column position 1 names.
		{`SELECT * FROM o ORDER BY 1`, "cannot be resolved through *"},
	}

	for _, c := range cases {
		_, err := NewParser(c.query).ParseStatement()
		if err == nil {
			t.Errorf("%s: expected a parse error, got none", c.query)
			continue
		}
		if !strings.Contains(err.Error(), c.wantSubstring) {
			t.Errorf("%s: error %q does not mention %q", c.query, err, c.wantSubstring)
		}
	}
}
