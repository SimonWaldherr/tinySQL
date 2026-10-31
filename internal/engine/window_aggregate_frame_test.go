package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestWindowAggregatesWithFilterAndDistinct(t *testing.T) {
	db := aggregateFilterFixture(t)
	rs := execSQL(t, db, `SELECT id, COUNT(DISTINCT v) FILTER (WHERE keep) OVER (PARTITION BY grp) AS n,
		SUM(DISTINCT v) FILTER (WHERE keep) OVER (PARTITION BY grp ORDER BY id ROWS UNBOUNDED PRECEDING) AS s,
		COUNT(*) OVER (PARTITION BY grp ORDER BY v NULLS FIRST) AS peers
		FROM filter_facts ORDER BY id`)
	wantN := []int{1, 1, 1, 1, 0, 0}
	wantPeers := []int{3, 4, 3, 1, 2, 1}
	for i, r := range rs.Rows {
		var wantSum any
		if i < 4 {
			wantSum = 10
		}
		if r["n"] != wantN[i] || r["s"] != wantSum || r["peers"] != wantPeers[i] {
			t.Fatalf("row %d: %v", i, r)
		}
	}
	rs = execSQL(t, db, `SELECT id, COUNT(*) FILTER (WHERE FALSE) OVER () AS n,
		SUM(1/0) FILTER (WHERE FALSE) OVER () AS s FROM filter_facts ORDER BY id`)
	for _, r := range rs.Rows {
		if r["n"] != 0 || r["s"] != nil {
			t.Fatalf("filtered empty window: %v", r)
		}
	}
}

func TestWindowRangeValuesAndRows(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE range_facts (id INT, k INT, v INT)`)
	execSQL(t, db, `INSERT INTO range_facts VALUES (1,1,10),(2,2,20),(3,2,30),(4,10,40),(5,NULL,50)`)
	for _, tc := range []struct {
		order, frame string
		sums         []any
	}{
		{"k NULLS LAST", "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW", []any{10, 60, 60, 40, 50}},
		{"k DESC NULLS FIRST", "RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING", []any{10, 60, 60, 40, 50}},
		{"k NULLS LAST", "RANGE BETWEEN CURRENT ROW AND CURRENT ROW", []any{10, 50, 50, 40, 50}},
		{"k NULLS LAST", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW", []any{10, 30, 50, 70, 90}},
		{"k NULLS LAST", "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING", []any{50, 70, 90, 50, nil}},
		{"k NULLS LAST", "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING", []any{nil, 10, 30, 50, 70}},
		{"k NULLS LAST", "RANGE BETWEEN 2 FOLLOWING AND 3 FOLLOWING", []any{nil, nil, nil, nil, 50}},
	} {
		sql := `SELECT id,SUM(v) OVER (ORDER BY ` + tc.order + ` ` + tc.frame + `) AS s FROM range_facts ORDER BY id`
		rs := execSQL(t, db, sql)
		for i, r := range rs.Rows {
			if r["s"] != tc.sums[i] {
				t.Fatalf("%s row %d: %v want %v", sql, i, r, tc.sums[i])
			}
		}
	}
	// FIRST/LAST_VALUE use the same bounds as aggregate windows, including
	// empty frames at the partition edges.
	rs := execSQL(t, db, `SELECT id,FIRST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS f,
		LAST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS l FROM range_facts ORDER BY id`)
	wantF := []any{20, 30, 40, 50, nil}
	wantL := []any{nil, 10, 20, 30, 40}
	for i, r := range rs.Rows {
		if r["f"] != wantF[i] || r["l"] != wantL[i] {
			t.Fatalf("frame values row %d: %v", i, r)
		}
	}
}

func TestWindowRangeExactIntegerKeys(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE wide_keys (k BIGINT, v INT)`)
	execSQL(t, db, `INSERT INTO wide_keys VALUES (9007199254740992,1),(9007199254740993,2),(9007199254740994,4)`)
	rs := execSQL(t, db, `SELECT k,SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM wide_keys ORDER BY k`)
	for i, want := range []int{1, 3, 6} {
		if rs.Rows[i]["s"] != want {
			t.Fatalf("wide range row %d: %v want %v", i, rs.Rows[i], want)
		}
	}
}

func TestWindowFrameSyntaxErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT SUM(v) OVER (RANGE 1 PRECEDING) FROM t`,
		`SELECT SUM(v) OVER (ORDER BY a,b RANGE 1 PRECEDING) FROM t`,
		`SELECT SUM(v) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t`,
		`SELECT SUM(v) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t`,
		`SELECT SUM(v) OVER (ORDER BY a ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM t`,
	} {
		if _, err := NewParser(sql).ParseStatement(); err == nil {
			t.Fatalf("accepted %s", sql)
		}
	}
}
