package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompiledRawCaseMatchesEvaluator(t *testing.T) {
	columns := map[string]int{"v": 0, "other": 1}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, expression := range []string{
		"CASE v WHEN other THEN v ELSE other END",
		"CASE WHEN v THEN other END",
		"CASE WHEN v IS NULL THEN other WHEN v > 0 THEN v ELSE 0 END",
		"CASE WHEN v IS NOT NULL THEN v ELSE other END",
		"CASE WHEN v THEN CASE other WHEN 1 THEN 2 ELSE 3 END ELSE 4 END",
		"CASE WHEN v THEN 7 ELSE 1/0 END",
		"CASE v WHEN 1 THEN 7 WHEN 1/0 THEN 8 ELSE 9 END",
		"CASE WHEN v THEN 7 WHEN 1/0 THEN 8 ELSE 9 END",
		"CASE WHEN v THEN v + 1 ELSE other END",
		"CASE WHEN v IS NULL THEN CAST(other AS TEXT) ELSE TRY_CONVERT(INT,v) END",
		"v IS NULL", "v IS NOT NULL", "(v + other) IS NULL",
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		compiled := compileRawValue(columns, expr)
		if compiled == nil {
			t.Fatal("expression was not compiled", expression)
		}
		check := func(raw []any) {
			t.Helper()
			want, wantErr := evalRawExpr(plan, raw, expr)
			got, gotErr := compiled(raw)
			if !conversionValuesEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s (%#v): %v / %v, want %v / %v", expression, raw, got, gotErr, want, wantErr)
			}
		}
		for _, value := range []any{nil, 0, 1, int64(1), int64(math.MaxInt64), 1.0, 0.5, math.NaN(), math.Inf(1), "", "1", "x", false, true, []byte("1"), big.NewRat(1, 1)} {
			for _, other := range []any{nil, 0, 1, "one", true, []byte("1")} {
				check([]any{value, other})
			}
		}
		for _, raw := range [][]any{nil, {nil}, {1}} {
			check(raw)
		}
	}
}

func TestCompiledRawCaseFallsBackAsAWhole(t *testing.T) {
	for _, expression := range []string{
		"CASE WHEN v THEN 1 ELSE missing END",
		"CASE WHEN v THEN ABS(other) ELSE 0 END",
		"CASE WHEN v THEN 1 ELSE CAST(NOW() AS TEXT) END",
		"CASE WHEN v AND other THEN 1 ELSE 0 END",
		"CASE WHEN NOT v THEN 1 ELSE 0 END",
		"CASE WHEN v THEN 1 ELSE (other OR v) END",
		"(missing + 1) IS NULL",
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		if compileRawValue(map[string]int{"v": 0, "other": 1}, expr) != nil {
			t.Fatal("unsupported child was bound", expression)
		}
	}
}

func TestCompiledRawCaseQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE bound_cases (id INT, v INT)")
	execSQL(t, db, "INSERT INTO bound_cases VALUES (1,NULL),(2,0),(3,2),(4,3),(5,2)")
	for _, distinct := range []bool{false, true} {
		prefix := "SELECT "
		want := []Row{{"value": 9}, {"value": 0}, {"value": 4}, {"value": 6}, {"value": 4}}
		if distinct {
			prefix += "DISTINCT "
			want = want[:4]
		}
		statement := mustParse(prefix + "CASE WHEN v IS NULL THEN 9 ELSE v*2 END AS value FROM bound_cases")
		got, err := Execute(t.Context(), db, "default", statement)
		if err != nil || !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("materialized CASE: %v / %v, want %v", got, err, want)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", statement)
		if err != nil {
			t.Fatal(err)
		}
		var streamed []Row
		for stream.Next() {
			copy := Row{}
			for key, value := range stream.Row() {
				copy[key] = value
			}
			streamed = append(streamed, copy)
			stream.ReleaseRow(stream.Row())
		}
		err = stream.Err()
		stream.Close()
		if err != nil || !reflect.DeepEqual(streamed, want) {
			t.Fatalf("streamed CASE: %v / %v, want %v", streamed, err, want)
		}
		columnar, err := ExecuteColumnar(t.Context(), db, "default", statement)
		if err != nil || columnar.RowCount != len(want) {
			t.Fatalf("columnar CASE: %v / %v", columnar, err)
		}
		for i, row := range want {
			if !conversionValuesEqual(columnar.Values[0][i], row["value"]) {
				t.Fatalf("columnar CASE row %d: %v, want %v", i, columnar.Values[0][i], row["value"])
			}
		}
	}
	if got := execSQL(t, db, "SELECT id FROM bound_cases WHERE CASE WHEN v IS NULL THEN FALSE ELSE v > 1 END"); !reflect.DeepEqual(got.Rows, []Row{{"id": 3}, {"id": 4}, {"id": 5}}) {
		t.Fatal("CASE filter changed results", got)
	}
	rows := []Row{{"id": 1, "v": nil}, {"id": 2, "v": 0}, {"id": 3, "v": 2}, {"id": 4, "v": 3}, {"id": 5, "v": 2}}
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	for _, function := range []string{"COUNT(%s)", "COUNT(DISTINCT %s)", "SUM(%s)", "AVG(%s)", "MIN(%s)", "MAX(%s)"} {
		statement := mustParse("SELECT " + fmt.Sprintf(function, "CASE WHEN v IS NULL THEN NULL ELSE v*2 END") + " AS value FROM bound_cases").(*Select)
		want, wantErr := evalAggregate(env, statement.Projs[0].Expr.(*FuncCall), rows)
		got, err := Execute(t.Context(), db, "default", statement)
		if fmt.Sprint(err) != fmt.Sprint(wantErr) || err == nil && !conversionValuesEqual(got.Rows[0]["value"], want) {
			t.Fatalf("CASE aggregate: %v / %v, want %v / %v", got, err, want, wantErr)
		}
	}
}

func TestCompiledRawCaseReadsCurrentParameters(t *testing.T) {
	columns := map[string]int{"v": 0}
	plan := &simpleSelectPlan{colIndex: columns}
	expr := mustParse("SELECT CASE v WHEN 1 THEN 10 ELSE 20 END").(*Select).Projs[0].Expr.(*CaseExpr)
	when, then, otherwise := expr.Whens[0].When.(*Literal), expr.Whens[0].Then.(*Literal), expr.Else.(*Literal)
	when.Parameter, then.Parameter, otherwise.Parameter = true, true, true
	compiled := compileRawValue(columns, expr)
	for _, parameters := range [][3]any{{1, 10, 20}, {2, nil, "other"}, {nil, "match", false}} {
		when.Val, then.Val, otherwise.Val = parameters[0], parameters[1], parameters[2]
		for _, value := range []any{nil, 1, 2} {
			got, err := compiled([]any{value})
			want, wantErr := evalRawExpr(plan, []any{value}, expr)
			if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("stale CASE parameters %v (%v): %v / %v, want %v / %v", parameters, value, got, err, want, wantErr)
			}
		}
	}
}

func TestCompiledRawCaseSharedStatementAcrossSchemas(t *testing.T) {
	statement := mustParse("SELECT SUM(CASE WHEN v IS NULL THEN spare ELSE v END) AS total FROM case_shared")
	var group sync.WaitGroup
	for worker := range 8 {
		group.Go(func() {
			db := storage.NewDB()
			defer db.Close()
			columns := []storage.Column{{Name: "v", Type: storage.IntType}, {Name: "spare", Type: storage.IntType}}
			if worker%2 != 0 {
				columns[0], columns[1] = columns[1], columns[0]
			}
			table := storage.NewTable("case_shared", columns, false)
			if err := db.Put("default", table); err != nil {
				t.Error(err)
				return
			}
			for execution := range 12 {
				rows := [][]any{{execution, worker}, {nil, 3}}
				if worker%2 != 0 {
					for _, row := range rows {
						row[0], row[1] = row[1], row[0]
					}
				}
				table.Rows = rows
				table.Version++
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || got.Rows[0]["total"] != execution+3 {
					t.Errorf("worker %d, execution %d: %v / %v", worker, execution, got, err)
					return
				}
			}
		})
	}
	group.Wait()
}
