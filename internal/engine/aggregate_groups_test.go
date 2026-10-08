package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateGroupsMatchMaterializedMixedKeys(t *testing.T) {
	keys := []any{
		nil, 1, int64(1), float64(1), "1", false, true,
		float64(0), math.Copysign(0, -1), math.NaN(),
		math.Float64frombits(0x7ff8000000000001), math.Inf(1), math.Inf(-1),
		[]byte("x"), []byte(nil), big.NewRat(1, 3), map[string]any{"x": 1},
		[2]int{1, 2}, []int{1, 2}, "N;", "I1;", "x\x1fy", "",
	}
	for _, repeats := range []int{3, 257} {
		t.Run(fmt.Sprint(repeats), func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("group_inputs", []storage.Column{
				{Name: "g", Type: storage.TextType}, {Name: "v", Type: storage.IntType},
				{Name: "seq", Type: storage.IntType}, {Name: "tag", Type: storage.TextType},
			}, false)
			for repeat := 0; repeat < repeats; repeat++ {
				for i, key := range keys {
					var v any = i + repeat
					if repeat%5 == 0 {
						v = nil
					}
					table.Rows = append(table.Rows, []any{key, v, i, fmt.Sprint(repeat % 2)})
				}
			}
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				`SELECT COUNT(*) AS n FROM group_inputs GROUP BY g`,
				`SELECT COUNT(*) AS n,SUM(v) AS s,AVG(v) AS a FROM group_inputs GROUP BY g`,
				`SELECT MIN(seq) AS first,COUNT(*) AS n,SUM(v) AS s,MIN(v) AS lo,MAX(v) AS hi FROM group_inputs GROUP BY g`,
				`SELECT COUNT(DISTINCT v) AS n,SUM(v) AS s FROM group_inputs GROUP BY g`,
				`SELECT COUNT(*) AS n,SUM(v) AS s FROM group_inputs GROUP BY g,tag`,
				`SELECT COUNT(*) AS n,SUM(v) AS s FROM group_inputs GROUP BY g HAVING COUNT(*)>3 ORDER BY s DESC LIMIT 7 OFFSET 2`,
			} {
				got := execSQL(t, db, query)
				general := strings.Replace(query, "FROM group_inputs", "FROM (SELECT * FROM group_inputs) AS group_inputs", 1)
				want := execSQL(t, db, general)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: got %v, want %v", query, got, want)
				}
			}
		})
	}
}

func TestAggregateGroupsBlockOwnershipAndPromotion(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("many_groups", []storage.Column{
		{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.Float64Type},
	}, false)
	for repeat := 0; repeat < 17; repeat++ {
		for group := 0; group < 257; group++ {
			var v any = group + repeat
			if repeat == 10 && group%3 == 0 {
				v = big.NewRat(int64(group+1), 3)
			}
			table.Rows = append(table.Rows, []any{group, v})
		}
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`SELECT g,COUNT(*) AS n,SUM(v) AS s,AVG(v) AS a FROM many_groups GROUP BY g`,
		`SELECT g,COUNT(DISTINCT v) AS n,SUM(v) AS s,AVG(v) AS a,MIN(v) AS lo,MAX(v) AS hi FROM many_groups GROUP BY g`,
	} {
		stmt := mustParse(query)
		want := execSQL(t, db, strings.Replace(query, "FROM many_groups", "FROM (SELECT * FROM many_groups) AS many_groups", 1))
		retained, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || !reflect.DeepEqual(retained, want) {
			t.Fatalf("%s: %v, got %v, want %v", query, err, retained, want)
		}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 3 {
					got, err := Execute(t.Context(), db, "default", stmt)
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Errorf("concurrent aggregation: %v, got %v, want %v", err, got, want)
						return
					}
				}
			})
		}
		wg.Wait()
		if !reflect.DeepEqual(retained, want) {
			t.Fatal("later executions changed retained aggregate values")
		}
	}
}

func TestSimpleCaseNullEvaluationOrder(t *testing.T) {
	for _, sql := range []string{
		`SELECT CASE NULL WHEN NULL THEN 1/0 WHEN 1 THEN 9 ELSE 7 END AS v`,
		`SELECT CASE 1 WHEN NULL THEN 1/0 WHEN 1 THEN 7 ELSE 9 END AS v`,
	} {
		expr := mustParse(sql).(*Select).Projs[0].Expr
		for _, eval := range []func() (any, error){
			func() (any, error) { return evalExpr(ExecEnv{}, expr, Row{}) },
			func() (any, error) { return evalRawExpr(&simpleSelectPlan{}, nil, expr) },
			func() (any, error) { return evalAggregate(ExecEnv{}, expr, []Row{{}}) },
		} {
			if got, err := eval(); err != nil || got != 7 {
				t.Fatalf("%s: got %v, error %v", sql, got, err)
			}
		}
	}
	expr := mustParse(`SELECT CASE NULL WHEN 1/0 THEN 9 ELSE 7 END`).(*Select).Projs[0].Expr
	for _, eval := range []func() (any, error){
		func() (any, error) { return evalExpr(ExecEnv{}, expr, Row{}) },
		func() (any, error) { return evalRawExpr(&simpleSelectPlan{}, nil, expr) },
		func() (any, error) { return evalAggregate(ExecEnv{}, expr, []Row{{}}) },
	} {
		if _, err := eval(); err == nil {
			t.Fatal("NULL target hid an error in a WHEN expression")
		}
	}
}
