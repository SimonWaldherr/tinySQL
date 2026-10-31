package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func aggregateFilterFixture(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE filter_facts (id INT PRIMARY KEY, grp TEXT, v INT, keep BOOL)`)
	execSQL(t, db, `INSERT INTO filter_facts VALUES (1,'a',10,TRUE),(2,'a',20,FALSE),
		(3,'a',10,TRUE),(4,'a',NULL,TRUE),(5,'b',7,NULL),(6,'b',NULL,FALSE)`)
	return db
}

func TestAggregateFilterClausePaths(t *testing.T) {
	db := aggregateFilterFixture(t)
	projections := `grp, COUNT(*) FILTER (WHERE keep) AS rows_kept,
		COUNT(v) FILTER (WHERE keep) AS values_kept,
		COUNT(DISTINCT v) FILTER (WHERE keep) AS distinct_kept,
		SUM(v) FILTER (WHERE keep) AS total, AVG(v) FILTER (WHERE keep) AS mean,
		MIN(v) FILTER (WHERE keep) AS minimum, MAX(v) FILTER (WHERE keep) AS maximum`
	want := []Row{
		{"grp": "a", "rows_kept": 3, "values_kept": 2, "distinct_kept": 1, "total": 20, "mean": float64(10), "minimum": 10, "maximum": 10},
		{"grp": "b", "rows_kept": 0, "values_kept": 0, "distinct_kept": 0, "total": nil, "mean": nil, "minimum": nil, "maximum": nil},
	}
	for _, source := range []string{"filter_facts", "(SELECT * FROM filter_facts) AS facts", "filter_facts JOIN (SELECT 1 AS marker) AS other ON 1=1"} {
		sql := "SELECT " + projections + " FROM " + source + " GROUP BY grp ORDER BY grp"
		rs := execSQL(t, db, sql)
		if !reflect.DeepEqual(rs.Rows, want) {
			t.Fatalf("%s: %v want %v", sql, rs.Rows, want)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", mustParse(sql))
		if err != nil {
			t.Fatal(err)
		}
		var rows []Row
		for stream.Next() {
			rows = append(rows, stream.Row())
		}
		streamErr := stream.Err()
		_ = stream.Close()
		if streamErr != nil || !reflect.DeepEqual(rows, want) {
			t.Fatalf("stream: %v %v", rows, streamErr)
		}
		columns, err := ExecuteColumnar(t.Context(), db, "default", mustParse(sql))
		if err != nil || !reflect.DeepEqual(*columns, columnarFromRows(rs)) {
			t.Fatalf("columnar: %v %v", columns, err)
		}
	}
}

func TestAggregateFilterEmptyLazyAndHaving(t *testing.T) {
	db := aggregateFilterFixture(t)
	for _, sql := range []string{
		`SELECT SUM(1/0) FILTER (WHERE FALSE) AS total, COUNT(*) FILTER (WHERE NULL) AS n FROM filter_facts`,
		`SELECT SUM(v) FILTER (WHERE keep) AS total, COUNT(*) FILTER (WHERE keep) AS n FROM filter_facts WHERE id<0`,
		`SELECT SUM(v) FILTER (WHERE keep) AS total, COUNT(*) FILTER (WHERE keep) AS n FROM filter_facts WHERE grp='b'`,
	} {
		rs := execSQL(t, db, sql)
		if len(rs.Rows) != 1 || rs.Rows[0]["total"] != nil || rs.Rows[0]["n"] != 0 {
			t.Fatalf("%s: %v", sql, rs.Rows)
		}
	}
	// Neither visible-table cardinality nor the cached primary-key maximum
	// can answer a filtered aggregate without applying its predicate.
	for _, sql := range []string{
		`SELECT COUNT(*) FILTER (WHERE FALSE) AS n FROM filter_facts`,
		`SELECT MAX(id) FILTER (WHERE FALSE) AS n FROM filter_facts`,
	} {
		rs := execSQL(t, db, sql)
		if got := rs.Rows[0]["n"]; got != nil && got != 0 {
			t.Fatalf("%s: %v", sql, got)
		}
	}
	rs := execSQL(t, db, `SELECT grp,COUNT(*) FILTER (WHERE keep) AS n FROM filter_facts
		GROUP BY grp HAVING COUNT(*) FILTER (WHERE NOT keep)>1 ORDER BY grp`)
	if len(rs.Rows) != 0 {
		t.Fatalf("different HAVING predicate reused a projection: %v", rs.Rows)
	}
	rs = execSQL(t, db, `SELECT COALESCE(SUM(v) FILTER (WHERE FALSE), 9) AS v FROM filter_facts`)
	if rs.Rows[0]["v"] != 9 {
		t.Fatalf("nested filtered aggregate: %v", rs.Rows)
	}
	rs = execSQL(t, db, `SELECT COALESCE(SUM(v), 9) AS v FROM filter_facts WHERE FALSE`)
	if rs.Rows[0]["v"] != 9 {
		t.Fatalf("empty nested aggregate: %v", rs.Rows)
	}
	for _, expression := range []string{
		`COALESCE(SUM(v) FILTER (WHERE keep), 1/0)`,
		`IF(COUNT(*)>0, SUM(v) FILTER (WHERE keep), 1/0)`,
		`IIF(COUNT(*)>0, SUM(v) FILTER (WHERE keep), 1/0)`,
	} {
		rs := execSQL(t, db, "SELECT "+expression+" AS v FROM filter_facts")
		if rs.Rows[0]["v"] != 20 {
			t.Fatalf("%s: %v", expression, rs.Rows)
		}
	}
	for _, expression := range []string{
		`(COUNT(*)>0) OR (1/0>0)`, `NOT ((COUNT(*)<0) AND (1/0>0))`,
	} {
		rs := execSQL(t, db, "SELECT "+expression+" AS v FROM filter_facts")
		if rs.Rows[0]["v"] != true {
			t.Fatalf("%s: %v", expression, rs.Rows)
		}
	}

}

func TestAggregateFilterLargeTableAndRebinding(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	sql := `SELECT grp,SUM(amount) FILTER (WHERE id>=2048) AS s,
		COUNT(*) FILTER (WHERE id<2048) AS n FROM batch_facts GROUP BY grp ORDER BY grp`
	rs := execSQL(t, db, sql)
	want := execSQL(t, db, `SELECT grp,SUM(CASE WHEN id>=2048 THEN amount END) AS s,
		COUNT(CASE WHEN id<2048 THEN 1 END) AS n FROM batch_facts GROUP BY grp ORDER BY grp`)
	if !reflect.DeepEqual(rs.Rows, want.Rows) {
		t.Fatalf("filtered batches: %v want %v", rs.Rows, want.Rows)
	}
	stmt := mustParse(`SELECT COUNT(*) FILTER (WHERE id>=2048) AS n FROM batch_facts`).(*Select)
	literal := stmt.Projs[0].Expr.(*FuncCall).Filter.(*Binary).Right.(*Literal)
	literal.Parameter = true
	for _, n := range []int{0, 2048, 4099, 9000} {
		literal.Val = n
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || rs.Rows[0]["n"] != max(4099-n, 0) {
			t.Fatalf("bound %d: %v %v", n, rs, err)
		}
	}
}

func TestAggregateFilterSyntaxErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT ABS(v) FILTER (WHERE TRUE) FROM t`,
		`SELECT COUNT(*) FILTER (v>0) FROM t`,
		`SELECT SUM(v) FILTER (WHERE COUNT(*)>0) FROM t`,
		`SELECT SUM(v) FILTER (WHERE ROW_NUMBER() OVER ()>0) FROM t`,
		`SELECT COUNT(*) FILTER (WHERE TRUE) FILTER (WHERE FALSE) FROM t`,
	} {
		if _, err := NewParser(sql).ParseStatement(); err == nil {
			t.Fatalf("accepted %s", sql)
		}
	}
}
