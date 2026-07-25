// Regression tests for ORDER BY on a column the SELECT list does not project.
//
// Sorting runs on projected rows. An ORDER BY term naming a column that was not
// projected used to find nothing there, compare every row equal, and return them
// in physical order — with no error. "SELECT name FROM users ORDER BY created_at"
// silently came back unsorted.
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

func orderByFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, s := range []string{
		`CREATE TABLE t (id INT, name TEXT, score INT, ratio FLOAT)`,
		`INSERT INTO t VALUES (1, 'c', 30, 3.5)`,
		`INSERT INTO t VALUES (2, 'a', 10, 1.5)`,
		`INSERT INTO t VALUES (3, 'b', 20, 2.5)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return db
}

// firstColumn runs a query and returns the first column of every row as text.
func firstColumn(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %s: %v", query, err)
		}
		switch v := cells[0].(type) {
		case string:
			out = append(out, v)
		case []byte:
			out = append(out, string(v))
		default:
			t.Fatalf("%s: first column is %T, want text", query, cells[0])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

func TestOrderByColumnNotInSelectList(t *testing.T) {
	db := orderByFixture(t)
	cases := []struct {
		query string
		want  []string
	}{
		{`SELECT name FROM t ORDER BY score`, []string{"a", "b", "c"}},
		{`SELECT name FROM t ORDER BY score ASC`, []string{"a", "b", "c"}},
		{`SELECT name FROM t ORDER BY score DESC`, []string{"c", "b", "a"}},
		{`SELECT name FROM t ORDER BY id DESC`, []string{"b", "a", "c"}},
		{`SELECT name FROM t ORDER BY ratio DESC`, []string{"c", "b", "a"}},
		// Projected ordering columns must keep working.
		{`SELECT name, score FROM t ORDER BY score`, []string{"a", "b", "c"}},
		{`SELECT name FROM t ORDER BY name DESC`, []string{"c", "b", "a"}},
		// With LIMIT, which takes the top-N path rather than a full sort.
		{`SELECT name FROM t ORDER BY score DESC LIMIT 2`, []string{"c", "b"}},
		{`SELECT name FROM t ORDER BY score LIMIT 2 OFFSET 1`, []string{"b", "c"}},
	}
	for _, tc := range cases {
		got := firstColumn(t, db, tc.query)
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s\n  got  %v\n  want %v", tc.query, got, tc.want)
		}
	}
}

// TestOrderByHiddenColumnDoesNotLeakIntoResult checks that carrying the sort key
// across the projection does not add a column to the result.
func TestOrderByHiddenColumnDoesNotLeakIntoResult(t *testing.T) {
	db := orderByFixture(t)
	rows, err := db.Query(`SELECT name FROM t ORDER BY score`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || !strings.EqualFold(cols[0], "name") {
		t.Errorf("columns = %v, want exactly [name]; the ordering column must not appear in the result", cols)
	}
}

// TestOrderByAliasShadowsSourceColumn pins the resolution order SQL requires: an
// output alias wins over a source column of the same name.
func TestOrderByAliasShadowsSourceColumn(t *testing.T) {
	db := orderByFixture(t)
	// The alias "score" is the negated source score, so ordering by it must
	// reverse the source ordering.
	got := firstColumn(t, db, `SELECT name, 0 - score AS score FROM t ORDER BY score`)
	want := []string{"c", "b", "a"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("ORDER BY resolved to the source column instead of the alias\n  got  %v\n  want %v", got, want)
	}
}

// TestOrderByUnknownColumnIsAnError checks that a name that is neither an output
// column nor a source column is reported rather than ignored.
func TestOrderByUnknownColumnIsAnError(t *testing.T) {
	db := orderByFixture(t)
	rows, err := db.Query(`SELECT name FROM t ORDER BY nonexistent_column`)
	if err == nil {
		_ = rows.Close()
		t.Error("ORDER BY on an unknown column succeeded; want an error")
	}
}
