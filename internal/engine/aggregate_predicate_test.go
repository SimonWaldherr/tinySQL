package engine

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestAggregatePredicatesUseWholeGroup(t *testing.T) {
	db := aggregateFilterFixture(t)
	for _, expression := range []string{
		`SUM(v) BETWEEN 40 AND 50`,
		`SUM(v) IN (47, 99)`,
		`SUM(v) NOT IN (48, 99)`,
		`47 IN (SUM(v), 99)`,
		`SUM(v) IN (SELECT 47 AS total)`,
		`CAST(SUM(v) AS TEXT) LIKE '4%'`,
		`CAST(SUM(v) AS TEXT) GLOB '4*'`,
		`CAST(SUM(v) AS TEXT) REGEXP '^4'`,
	} {
		query := "SELECT " + expression + " AS matches FROM filter_facts"
		rs := execSQL(t, db, query)
		if len(rs.Rows) != 1 || rs.Rows[0]["matches"] != true {
			t.Errorf("%s: got %v, want one TRUE row", query, rs.Rows)
		}
	}
	for _, predicate := range []string{`SUM(v) IN (47, 99)`, `SUM(v) BETWEEN 40 AND 50`} {
		rs := execSQL(t, db, "SELECT COUNT(*) AS n FROM filter_facts HAVING "+predicate)
		if len(rs.Rows) != 1 || rs.Rows[0]["n"] != 6 {
			t.Errorf("HAVING %s: %v", predicate, rs.Rows)
		}
	}
}

func TestWindowUnsupportedContextsAreExplicit(t *testing.T) {
	db := aggregateFilterFixture(t)
	_, err := Execute(t.Context(), db, "default", mustParse(`SELECT id FROM filter_facts HAVING FALSE`))
	if err == nil || !strings.Contains(err.Error(), "HAVING requires") {
		t.Fatalf("ignored HAVING: %v", err)
	}
	for _, query := range []string{
		`SELECT grp, COUNT(*), ROW_NUMBER() OVER (ORDER BY grp) FROM filter_facts GROUP BY grp`,
		`SELECT COUNT(*), SUM(v) OVER () FROM filter_facts`,
	} {
		_, err := Execute(t.Context(), db, "default", mustParse(query))
		if err == nil || !strings.Contains(err.Error(), "aggregate in a subquery") {
			t.Fatalf("%s: %v", query, err)
		}
	}
	for _, query := range []string{
		`SELECT COUNT(SUM(v)) FROM filter_facts`,
		`SELECT SUM(ROW_NUMBER() OVER ()) OVER () FROM filter_facts`,
		`SELECT SUM(v) OVER (PARTITION BY ROW_NUMBER() OVER () IN (1,2)) FROM filter_facts`,
	} {
		_, err := NewParser(query).ParseStatement()
		if err == nil || !strings.Contains(err.Error(), "nested aggregate or window") {
			t.Fatalf("%s: %v", query, err)
		}
	}
	for _, query := range []string{
		`SELECT ROW_NUMBER() OVER (ORDER BY missing) FROM filter_facts`,
		`SELECT SUM(v) OVER (ORDER BY missing RANGE 1 PRECEDING) FROM filter_facts`,
	} {
		_, err := Execute(t.Context(), db, "default", mustParse(query))
		if err == nil || !strings.Contains(err.Error(), "unknown column") {
			t.Fatalf("%s: %v", query, err)
		}
	}
	// A subquery establishes a separate aggregation scope and is supported.
	rs := execSQL(t, db, `SELECT grp, n, ROW_NUMBER() OVER (ORDER BY grp) AS rn FROM
		(SELECT grp,COUNT(*) AS n FROM filter_facts GROUP BY grp) AS grouped ORDER BY grp`)
	if len(rs.Rows) != 2 || rs.Rows[0]["n"] != 4 || rs.Rows[1]["n"] != 2 || rs.Rows[1]["rn"] != 2 {
		t.Fatal(rs.Rows)
	}
	rs = execSQL(t, db, `SELECT COUNT(*) FILTER (WHERE EXISTS (SELECT SUM(v) FROM filter_facts)) AS n FROM filter_facts`)
	if rs.Rows[0]["n"] != 6 {
		t.Fatal(rs.Rows)
	}
}

func TestAggregatePredicateSharedStatement(t *testing.T) {
	db := aggregateFilterFixture(t)
	statement := mustParse(`SELECT grp, SUM(v) IN (20,40) AS matches,
		CAST(SUM(v) AS TEXT) LIKE '%' AS text_match FROM filter_facts GROUP BY grp ORDER BY grp`)
	want, err := Execute(t.Context(), db, "default", statement)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			for range 8 {
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || !reflect.DeepEqual(got, want) {
					errors <- fmt.Errorf("shared statement: %v, %v", got, err)
					return
				}
			}
		})
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestFilterRejectsNestedAggregatePredicates(t *testing.T) {
	for _, predicate := range []string{
		`SUM(v) BETWEEN 0 AND 100`, `1 IN (SUM(v), 2)`,
		`CAST(SUM(v) AS TEXT) LIKE '%'`, `CAST(SUM(v) AS TEXT) REGEXP '.'`,
		`ROW_NUMBER() OVER () BETWEEN 1 AND 2`,
		`1 IN (ROW_NUMBER() OVER (), 2)`,
	} {
		query := "SELECT COUNT(*) FILTER (WHERE " + predicate + ") FROM facts"
		_, err := NewParser(query).ParseStatement()
		if err == nil || !strings.Contains(err.Error(), "FILTER predicate") {
			t.Errorf("%s: got %v, want FILTER predicate error", query, err)
		}
	}
}

func TestWindowPredicatesHaveWindowContext(t *testing.T) {
	db := aggregateFilterFixture(t)
	for _, expression := range []string{
		`ROW_NUMBER() OVER (ORDER BY id) BETWEEN 1 AND 6`,
		`ROW_NUMBER() OVER (ORDER BY id) IN (1,2,3,4,5,6)`,
		`CAST(ROW_NUMBER() OVER (ORDER BY id) AS TEXT) LIKE '_'`,
		`CAST(ROW_NUMBER() OVER (ORDER BY id) AS TEXT) REGEXP '[1-6]'`,
	} {
		rs := execSQL(t, db, "SELECT "+expression+" AS matches FROM filter_facts")
		if len(rs.Rows) != 6 {
			t.Fatal(rs.Rows)
		}
		for _, row := range rs.Rows {
			if row["matches"] != true {
				t.Errorf("%s: %v", expression, row)
			}
		}
	}
}
