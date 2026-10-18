package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInSubquerySetSemantics(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE candidates (v INT)`)
	execSQL(t, db, `INSERT INTO candidates VALUES (NULL), (2), (4), (4)`)
	execSQL(t, db, `CREATE TABLE probes (v INT)`)
	execSQL(t, db, `INSERT INTO probes VALUES (1), (2), (4), (NULL)`)

	for _, tc := range []struct {
		expr string
		want any
	}{
		{`2 IN (SELECT v FROM candidates)`, true},
		{`4 IN (SELECT v FROM candidates)`, true},
		{`2 NOT IN (SELECT v FROM candidates)`, false},
		{`1 IN (SELECT v FROM candidates)`, nil},
		{`1 NOT IN (SELECT v FROM candidates)`, nil},
		{`NULL IN (SELECT v FROM candidates)`, nil},
		{`NULL NOT IN (SELECT v FROM candidates)`, nil},
		{`1 IN (SELECT v FROM candidates WHERE v IS NOT NULL)`, false},
		{`1 NOT IN (SELECT v FROM candidates WHERE v IS NOT NULL)`, true},
		{`1 IN (SELECT v FROM candidates WHERE FALSE)`, false},
		{`1 NOT IN (SELECT v FROM candidates WHERE FALSE)`, true},
		{`NULL IN (SELECT v FROM candidates WHERE FALSE)`, false},
		{`NULL NOT IN (SELECT v FROM candidates WHERE FALSE)`, true},
		{`4 IN (SELECT v AS renamed FROM candidates)`, true},
		{`4 IN (SELECT * FROM candidates)`, true},
		{`4 IN (SELECT v FROM candidates WHERE v IS NOT NULL ORDER BY v LIMIT 1)`, false},
		{`4 IN (WITH selected AS (SELECT v FROM candidates) SELECT v FROM selected)`, true},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			rs := execSQL(t, db, "SELECT "+tc.expr+" AS result")
			if got := rs.Rows[0]["result"]; !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}

	for _, modifier := range []string{"", "DISTINCT "} {
		rs := execSQL(t, db, "SELECT "+modifier+`v FROM probes WHERE v IN (SELECT v FROM candidates) ORDER BY v`)
		if len(rs.Rows) != 2 || rs.Rows[0]["v"] != 2 || rs.Rows[1]["v"] != 4 {
			t.Fatalf("IN WHERE with %q: unexpected rows %#v", modifier, rs.Rows)
		}
		rs = execSQL(t, db, "SELECT "+modifier+`v FROM probes WHERE v NOT IN (SELECT v FROM candidates WHERE FALSE)`)
		if len(rs.Rows) != 4 {
			t.Fatalf("NOT IN empty set with %q: got %d rows, want 4", modifier, len(rs.Rows))
		}
	}

	// Parsed statements may be reused, but their subquery results must be
	// refreshed for every execution when the underlying table changes.
	stmt := mustParse(`SELECT 8 IN (SELECT v FROM candidates) AS result`)
	for _, want := range []any{nil, true} {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil || !reflect.DeepEqual(rs.Rows[0]["result"], want) {
			t.Fatalf("reused statement: got %#v, %v; want %#v", rs, err, want)
		}
		execSQL(t, db, `INSERT INTO candidates VALUES (8)`)
	}
}

func TestInScalarSubqueryListItems(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE candidates (v INT)`)
	execSQL(t, db, `INSERT INTO candidates VALUES (2), (4)`)
	for _, tc := range []struct {
		expr string
		want any
	}{
		{`2 IN ((SELECT v FROM candidates WHERE v = 2))`, true},
		{`4 NOT IN ((SELECT v FROM candidates WHERE v = 2))`, true},
		{`2 IN ((SELECT v FROM candidates WHERE FALSE))`, nil},
		{`2 NOT IN ((SELECT v FROM candidates WHERE FALSE))`, nil},
		{`2 IN (1, (SELECT v FROM candidates WHERE v = 2))`, true},
	} {
		rs := execSQL(t, db, "SELECT "+tc.expr+" AS result")
		if got := rs.Rows[0]["result"]; !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: got %#v, want %#v", tc.expr, got, tc.want)
		}
	}
	for _, expr := range []string{`2 IN ((SELECT v FROM candidates))`, `2 IN (1, (SELECT v FROM candidates))`} {
		_, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expr+" AS result"))
		if err == nil || !strings.Contains(err.Error(), "scalar subquery returned 2 rows") {
			t.Fatalf("%s: got %v, want scalar cardinality error", expr, err)
		}
	}
}

func TestSubqueryRequiresSingleColumn(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE pairs (a INT, b INT)`)
	execSQL(t, db, `INSERT INTO pairs VALUES (1, 2)`)
	for _, body := range []string{`SELECT a, b FROM pairs`, `SELECT a, b FROM pairs WHERE FALSE`, `SELECT * FROM pairs`} {
		for _, expr := range []string{fmt.Sprintf("1 IN (%s)", body), fmt.Sprintf("NULL NOT IN (%s)", body), "(" + body + ")"} {
			_, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expr+" AS result"))
			if err == nil || !strings.Contains(err.Error(), "one column") {
				t.Fatalf("%s: got %v, want a single-column error", expr, err)
			}
		}
	}
	_, err := Execute(context.Background(), db, "default", mustParse(`SELECT (SELECT a FROM pairs UNION ALL SELECT b FROM pairs) AS result`))
	if err == nil || !strings.Contains(err.Error(), "scalar subquery returned 2 rows") {
		t.Fatalf("multirow scalar subquery: got %v, want cardinality error", err)
	}
}

func TestLogicalSubqueriesDeclineRawFilter(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE probes (id INT, active BOOL)`)
	execSQL(t, db, `INSERT INTO probes VALUES (1, TRUE), (2, FALSE), (3, TRUE), (4, FALSE), (5, NULL)`)
	execSQL(t, db, `CREATE TABLE candidates (v INT)`)
	execSQL(t, db, `INSERT INTO candidates VALUES (NULL), (1), (2), (2)`)
	for _, tc := range []struct {
		predicate string
		want      []int
	}{
		{`active = TRUE AND id IN (SELECT DISTINCT v FROM candidates)`, []int{1}},
		{`id IN (SELECT DISTINCT v FROM candidates) AND active = TRUE`, []int{1}},
		{`active = TRUE OR id IN (SELECT v FROM candidates)`, []int{1, 2, 3}},
		{`id IN (SELECT v FROM candidates) OR active = TRUE`, []int{1, 2, 3}},
		{`active = TRUE AND (id IN (SELECT v FROM candidates) OR id = 4)`, []int{1}},
		{`active = TRUE AND id = (SELECT v FROM candidates WHERE v = 1)`, []int{1}},
		{`active = TRUE AND EXISTS (SELECT v FROM candidates)`, []int{1, 3}},
		{`active = FALSE OR EXISTS (SELECT v FROM candidates WHERE FALSE)`, []int{2, 4}},
	} {
		t.Run(tc.predicate, func(t *testing.T) {
			query := `SELECT id FROM probes WHERE ` + tc.predicate + ` ORDER BY id`
			selectStmt := mustParseSelect(t, query)
			if buildRawFilter(map[string]int{"id": 0, "active": 1}, selectStmt.Where) != nil {
				t.Fatal("raw filter accepted a subquery it cannot evaluate")
			}
			rs := execSQL(t, db, query)
			got := make([]int, len(rs.Rows))
			for i, row := range rs.Rows {
				got[i] = row["id"].(int)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
