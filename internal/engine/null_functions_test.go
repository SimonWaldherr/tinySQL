package engine

import (
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestNullFunctionsRawMatchesGeneral(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0, "other": 1}}
	values := []any{nil, 0, 1, int64(0), int64(1), 0.0, 1.0, math.NaN(), "", "0", "FALSE", "1", false, true, []byte(nil), []byte("1")}
	for _, sql := range []string{
		`COALESCE(v, other, 'fallback')`, `IFNULL(v, other)`, `NVL(v, other)`,
		`ISNULL(v)`, `NOT ISNULL(v)`, `NULLIF(v, other)`,
		`COALESCE(NULLIF(v, other), 'fallback')`, `IF(v, other, 'else')`, `IIF(v, other, 'else')`,
	} {
		expr := mustParse(`SELECT ` + sql).(*Select).Projs[0].Expr
		for _, v := range values {
			for _, other := range values {
				got, err := evalRawExpr(plan, []any{v, other}, expr)
				want, wantErr := evalExpr(ExecEnv{}, expr, Row{"v": v, "other": other})
				if (err != nil) != (wantErr != nil) || err != nil && err.Error() != wantErr.Error() {
					t.Fatalf("%s (%#v, %#v): error %v want %v", sql, v, other, err, wantErr)
				}
				if a, ok := got.(float64); ok && math.IsNaN(a) {
					if b, ok := want.(float64); ok && math.IsNaN(b) {
						continue
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s (%#v, %#v): %T(%#v) want %T(%#v)", sql, v, other, got, got, want, want)
				}
			}
		}
	}
}

func TestNullFunctionsShortCircuitAndErrors(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, sql := range []string{
		`COALESCE(v, 1/0)`, `IFNULL(v, 1/0)`, `NVL(v, 1/0)`,
		`COALESCE(NULL, v, missing_column)`, `COALESCE(v, unknown_function())`,
		`IF(v, 1, 1/0)`, `IIF(v, 1, 1/0)`, `IF(NULL, 1/0, v)`,
		`COALESCE()`, `NULLIF(NULL, 1/0)`, `NULLIF(v, 1/0)`, `ISNULL(1/0)`,
		`ISNULL()`, `ISNULL(v, 1/0)`, `NULLIF(v)`, `NULLIF(v, v, 1/0)`,
		`IF(v, 1/0)`, `IIF(v, v, v, 1/0)`,
	} {
		expr := mustParse(`SELECT ` + sql).(*Select).Projs[0].Expr
		for _, value := range []any{nil, 0, 1} {
			got, err := evalRawExpr(plan, []any{value}, expr)
			want, wantErr := evalExpr(ExecEnv{}, expr, Row{"v": value})
			if !reflect.DeepEqual(got, want) || (err != nil) != (wantErr != nil) || err != nil && err.Error() != wantErr.Error() {
				t.Fatalf("%s v=%v: got %v / %v, want %v / %v", sql, value, got, err, want, wantErr)
			}
		}
	}
}

func TestNullFunctionsSQLAndStreaming(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE nullable (id INT, v INT)`)
	execSQL(t, db, `INSERT INTO nullable VALUES (1, 5), (2, NULL), (3, 0)`)
	for _, sql := range []string{
		`SELECT COALESCE(v, 1/0) AS x FROM nullable WHERE id=1`,
		`SELECT IFNULL(v, 1/0) AS x FROM nullable WHERE id=1`,
		`SELECT NVL(v, 1/0) AS x FROM nullable WHERE id=1`,
		`SELECT IF(ISNULL(v), 5, 1/0) AS x FROM nullable WHERE id=2`,
		`SELECT IIF(ISNULL(v), 1/0, v) AS x FROM nullable WHERE id=1`,
		`SELECT SUM(COALESCE(v, 1/0)) AS x FROM nullable WHERE id=1`,
		`SELECT COALESCE(v, 1/0) AS x FROM nullable WHERE id=1 ORDER BY x`,
		`SELECT id FROM nullable WHERE COALESCE(v, 1/0) = 5 AND id=1`,
	} {
		rs := execSQL(t, db, sql)
		if len(rs.Rows) != 1 {
			t.Fatalf("%s: %+v", sql, rs)
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
		stream.Close()
		if streamErr != nil || !reflect.DeepEqual(rows, rs.Rows) {
			t.Fatalf("%s: stream=%+v err=%v want %+v", sql, rows, streamErr, rs.Rows)
		}
	}
	for _, tc := range []struct{ predicate, reference string }{
		{`ISNULL(v)`, `v IS NULL`},
		{`NOT ISNULL(v)`, `v IS NOT NULL`},
		{`ISNULL(COALESCE(v, NULL))`, `v IS NULL`},
		{`ISNULL(v) OR id=3`, `v IS NULL OR id=3`},
	} {
		want := execSQL(t, db, `SELECT id FROM nullable WHERE `+tc.reference)
		got := execSQL(t, db, `SELECT id FROM nullable WHERE `+tc.predicate)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %+v want %+v", tc.predicate, got, want)
		}
	}
	stmt := mustParse(`SELECT COALESCE(v, 9) AS x FROM nullable WHERE id=2`).(*Select)
	param := stmt.Projs[0].Expr.(*FuncCall).Args[1].(*Literal)
	param.Parameter = true
	for _, value := range []any{8, nil, "text", []byte{1, 2}} {
		param.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || !reflect.DeepEqual(rs.Rows[0]["x"], value) {
			t.Fatalf("stale parameter: %+v %v", rs, err)
		}
	}
	concurrent := mustParse(`SELECT COALESCE(NULLIF(v, 0), 7) AS x, ISNULL(v) AS empty FROM nullable`)
	want, err := Execute(t.Context(), db, "default", concurrent)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 30; i++ {
				got, err := Execute(t.Context(), db, "default", concurrent)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("concurrent evaluation: %+v %v", got, err)
					return
				}
			}
		})
	}
	wg.Wait()
}
