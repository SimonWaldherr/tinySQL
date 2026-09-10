package engine

import (
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCaseRawMatchesGeneral(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0, "other": 1}}
	for _, sql := range []string{
		`CASE v WHEN other THEN 'match' ELSE 'other' END`,
		`CASE WHEN v THEN other END`,
		`CASE WHEN v IS NULL THEN other WHEN v > 0 THEN v ELSE 0 END`,
		`CASE WHEN v THEN CASE other WHEN 1 THEN 2 ELSE 3 END ELSE 4 END`,
		`CASE WHEN v THEN 7 ELSE 1/0 END`,
		`CASE v WHEN 1 THEN 7 WHEN 1/0 THEN 8 ELSE 9 END`,
		`CASE WHEN v THEN 7 WHEN 1/0 THEN 8 ELSE 9 END`,
	} {
		expr := mustParse(`SELECT ` + sql).(*Select).Projs[0].Expr
		if !isSimpleRawExpr(expr) {
			t.Fatalf("CASE not admitted: %s", sql)
		}
		for _, v := range []any{nil, 0, 1, int64(1), 1.0, "", "1", "x", false, true, []byte("1")} {
			for _, other := range []any{nil, 0, 1, "1", true} {
				got, err := evalRawExpr(plan, []any{v, other}, expr)
				want, werr := evalExpr(ExecEnv{}, expr, Row{"v": v, "other": other})
				if !reflect.DeepEqual(got, want) || (err != nil) != (werr != nil) || err != nil && err.Error() != werr.Error() {
					t.Fatalf("%s (%v,%v): %v/%v want %v/%v", sql, v, other, got, err, want, werr)
				}
			}
		}
	}
}

func TestCaseHavingSQL(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE cases (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO cases VALUES (1, 10), (1, NULL), (2, 20), (2, 30)`)
	for _, tc := range []struct {
		sql    string
		values []any
	}{
		{`SELECT CASE WHEN v IS NULL THEN 0 ELSE v END AS x FROM cases ORDER BY x LIMIT 2`, []any{0, 10}},
		{`SELECT CASE v WHEN 10 THEN 1 WHEN 20 THEN 2 ELSE 3 END AS x FROM cases`, []any{1, 3, 2, 3}},
		{`SELECT v AS x FROM cases WHERE CASE WHEN v IS NULL THEN FALSE ELSE v >= 20 END`, []any{20, 30}},
		{`SELECT SUM(CASE WHEN v IS NULL THEN 0 ELSE v END) AS x FROM cases`, []any{float64(60)}},
		{`SELECT g, SUM(CASE WHEN v > 10 THEN v ELSE 0 END) AS x FROM cases GROUP BY g HAVING SUM(CASE WHEN v > 10 THEN v ELSE 0 END) > 10`, []any{float64(50)}},
		{`SELECT g, COUNT(*) AS x FROM cases GROUP BY g HAVING g=2 AND COUNT(*)=2`, []any{2}},
		{`SELECT COUNT(*) AS x FROM cases WHERE g=9 HAVING COUNT(*)=0`, []any{0}},
		{`SELECT SUM(v) AS x FROM cases WHERE g=9 HAVING SUM(v) IS NULL`, []any{nil}},
		{`SELECT COUNT(*) AS x FROM cases HAVING FALSE AND 1/0 > 0`, nil},
		{`SELECT COUNT(*) AS x FROM cases HAVING TRUE OR 1/0 > 0`, []any{4}},
		{`SELECT COUNT(*) AS x FROM cases HAVING NULL OR COUNT(*)=4`, []any{4}},
		{`SELECT COUNT(*) AS x FROM cases HAVING NULL AND COUNT(*)=4`, nil},
	} {
		stmt := mustParse(tc.sql).(*Select)
		if len(stmt.GroupBy) > 0 || anyAggInSelect(stmt.Projs) {
			_, ok, err := buildSimpleAggregatePlan(ExecEnv{db: db, tenant: "default"}, stmt)
			if !ok || err != nil {
				t.Fatalf("not optimized: %s: %v", tc.sql, err)
			}
		}
		rs := execSQL(t, db, tc.sql)
		if len(rs.Rows) != len(tc.values) {
			t.Fatalf("%s: %+v", tc.sql, rs)
		}
		for i, want := range tc.values {
			if !rawEqual(rs.Rows[i]["x"], want) {
				t.Fatalf("%s: %v want %v", tc.sql, rs.Rows[i], want)
			}
		}
		stream, err := ExecuteStream(t.Context(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		var rows []Row
		for stream.Next() {
			rows = append(rows, stream.Row())
		}
		err = stream.Err()
		stream.Close()
		if err != nil || len(rows) != len(rs.Rows) || len(rows) > 0 && !reflect.DeepEqual(rows, rs.Rows) {
			t.Fatalf("stream %s: %v/%v want %v", tc.sql, rows, err, rs.Rows)
		}
	}
	stmt := mustParse(`SELECT CASE WHEN v IS NULL THEN 99 ELSE v END AS x FROM cases`).(*Select)
	param := stmt.Projs[0].Expr.(*CaseExpr).Whens[0].Then.(*Literal)
	param.Parameter = true
	for _, value := range []any{8, nil, "new"} {
		param.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || !reflect.DeepEqual(rs.Rows[1]["x"], value) {
			t.Fatalf("stale CASE: %v %v", rs, err)
		}
	}
	having := mustParse(`SELECT COUNT(*) AS x FROM cases HAVING COUNT(*) > 0`).(*Select)
	threshold := having.Having.(*Binary).Right.(*Literal)
	threshold.Parameter = true
	for _, value := range []int{0, 9, 1} {
		threshold.Val = value
		rs, err := Execute(t.Context(), db, "default", having)
		if err != nil || (len(rs.Rows) == 1) != (value < 4) {
			t.Fatalf("stale HAVING: %v %v", rs, err)
		}
	}
	concurrent := mustParse(`SELECT g, SUM(CASE WHEN v IS NULL THEN 0 ELSE v END) AS x FROM cases GROUP BY g HAVING SUM(CASE WHEN v IS NULL THEN 0 ELSE v END)>0 ORDER BY g`)
	want, err := Execute(t.Context(), db, "default", concurrent)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 20; j++ {
				got, err := Execute(t.Context(), db, "default", concurrent)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("concurrent: %v %v", got, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestCaseTriggerDMLBindings(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	for _, sql := range []string{
		`CREATE TABLE source (id INT)`,
		`CREATE TABLE updated (id INT, v INT)`,
		`CREATE TABLE deleted (id INT)`,
		`INSERT INTO updated VALUES (1,0),(2,0)`,
		`INSERT INTO deleted VALUES (1),(2)`,
		`CREATE TRIGGER modify AFTER INSERT ON source BEGIN UPDATE updated SET v=CASE WHEN id=NEW.id THEN 7 ELSE v END; DELETE FROM deleted WHERE id=CASE WHEN TRUE THEN NEW.id ELSE 0 END; END`,
		`INSERT INTO source VALUES (2)`,
	} {
		execSQL(t, db, sql)
	}
	if rs := execSQL(t, db, `SELECT v FROM updated ORDER BY id`); len(rs.Rows) != 2 || !rawEqual(rs.Rows[0]["v"], 0) || !rawEqual(rs.Rows[1]["v"], 7) {
		t.Fatal(rs)
	}
	if rs := execSQL(t, db, `SELECT id FROM deleted`); len(rs.Rows) != 1 || !rawEqual(rs.Rows[0]["id"], 1) {
		t.Fatal(rs)
	}
}

func TestCasePlanValidationAndParameters(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE boundcase (v INT)`)
	execSQL(t, db, `INSERT INTO boundcase VALUES (1),(2)`)
	stmt := mustParse(`SELECT v FROM boundcase WHERE v=CASE WHEN TRUE THEN 1 ELSE 0 END`).(*Select)
	param := stmt.Where.(*Binary).Right.(*CaseExpr).Whens[0].Then.(*Literal)
	param.Parameter = true
	for _, value := range []int{1, 2, 1} {
		param.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 1 || !rawEqual(rs.Rows[0]["v"], value) {
			t.Fatalf("cached CASE predicate: %v %v", rs, err)
		}
	}
	for _, sql := range []string{
		`SELECT CASE WHEN FALSE THEN missing ELSE v END FROM boundcase LIMIT 0`,
		`SELECT v FROM boundcase WHERE CASE WHEN FALSE THEN missing ELSE TRUE END LIMIT 0`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err == nil {
			t.Fatalf("missing column accepted: %s", sql)
		}
	}
	for _, sql := range []string{
		`SELECT SUM(v) FROM boundcase HAVING SUM(v+1)>0`,
		`SELECT SUM(v) FROM boundcase HAVING COALESCE(SUM(v),0)>0`,
	} {
		_, ok, err := buildSimpleAggregatePlan(ExecEnv{db: db, tenant: "default"}, mustParse(sql).(*Select))
		if ok || err != nil {
			t.Fatalf("unsupported HAVING incorrectly admitted: %s", sql)
		}
		execSQL(t, db, sql)
	}
}
