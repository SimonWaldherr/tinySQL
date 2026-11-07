package engine

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func conversionValuesEqual(a, b any) bool {
	if x, ok := a.(float64); ok && math.IsNaN(x) {
		y, ok := b.(float64)
		return ok && math.IsNaN(y)
	}
	return reflect.DeepEqual(a, b)
}

func TestCompiledRawConversionsMatchEvaluator(t *testing.T) {
	columns := map[string]int{"v": 0}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, expression := range []string{
		"CAST(v AS INT)", "CAST(v AS FLOAT)", "CAST(v AS TEXT)", "CAST(v AS BOOL)",
		"CONVERT(INT,v)", "CONVERT(BIGINT,v)", "CONVERT(FLOAT,v)", "TRY_CONVERT(INT,v)",
		"TRY_CONVERT(FLOAT,v)", "CONVERT(BOOL,v)", "CONVERT(VARCHAR(3),v)", "CONVERT(CHAR(5),v)",
		"CONVERT(DATE,v,104)", "TRY_CONVERT(DATE,v,104)", "CONVERT(TIME,v,108)",
		"CAST(CONVERT(FLOAT,v) AS TEXT)", "CONVERT(TEXT,CAST(v AS INT))",
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		compiled := compileRawConversion(columns, expr)
		if compiled == nil {
			t.Fatal("conversion was not compiled", expression)
		}
		for _, value := range []any{nil, "12", "12.5", "bad", "äöüß", "29.02.2024", "12:34:56", 7, int64(9007199254740993), -2.5, true, math.NaN(), math.Inf(1)} {
			raw := []any{value}
			want, wantErr := evalRawExpr(plan, raw, expr)
			got, gotErr := compiled(raw)
			if !conversionValuesEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s (%#v): %v / %v, want %v / %v", expression, value, got, gotErr, want, wantErr)
			}
		}
		_, gotErr := compiled(nil)
		_, wantErr := evalRawExpr(plan, nil, expr)
		if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatal("short row diagnostics", gotErr, wantErr)
		}
	}
	// Invalid declarations and dynamic styles keep row-time validation. In
	// particular, TRY_CONVERT must not hide declaration or expression errors.
	for _, expression := range []string{
		"CONVERT(unknown,v)", "CONVERT(VARCHAR(8001),v)", "CONVERT(INT,v,130)",
		"CONVERT(INT,v,NULL)", "TRY_CONVERT(INT,1/0)", "CONVERT(DATE,v,v)",
		"CAST(NOW() AS TEXT)",
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		if compileRawConversion(columns, expr) != nil {
			t.Fatal("unexpected eager binding", expression)
		}
	}
}

func TestCompiledConversionFiltersKeepBetweenAndParameters(t *testing.T) {
	columns := map[string]int{"v": 0, "lo": 1, "hi": 2}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, expression := range []string{
		"CAST(v AS FLOAT) BETWEEN lo AND hi", "CAST(v AS FLOAT) NOT BETWEEN lo AND hi",
		"TRY_CONVERT(INT,v) BETWEEN lo AND hi", "TRY_CONVERT(INT,v) NOT BETWEEN lo AND hi",
		"CONVERT(FLOAT,v)>=lo", "lo<CONVERT(FLOAT,v)", "CAST(v AS FLOAT)=lo",
	} {
		expr := mustParse("SELECT v FROM t WHERE " + expression).(*Select).Where
		filter := buildRawConversionFilter(columns, expr)
		if filter == nil {
			t.Fatal("filter was not compiled", expression)
		}
		for _, value := range []any{nil, "-1", "0", "5", "10", "11", "bad", "NaN", math.Inf(1)} {
			for _, bounds := range [][2]any{{0, 10}, {10, 0}, {nil, 10}, {0, nil}, {nil, nil}, {"0", "10"}, {math.NaN(), 10}} {
				raw := []any{value, bounds[0], bounds[1]}
				want, wantErr := evalRawExpr(plan, raw, expr)
				got, gotErr := filter(raw)
				if got != (toTri(want) == tvTrue) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("%s, %v: %v / %v, want %v / %v", expression, raw, got, gotErr, want, wantErr)
				}
			}
		}
	}
	expr := mustParse("SELECT v FROM t WHERE CAST(v AS FLOAT) BETWEEN 0 AND 10").(*Select).Where.(*BetweenExpr)
	lo, hi := expr.Lo.(*Literal), expr.Hi.(*Literal)
	lo.Parameter, hi.Parameter = true, true
	filter := buildRawConversionFilter(columns, expr)
	for _, bounds := range [][2]any{{0, 10}, {6, 10}, {nil, 10}, {0, nil}, {0.0, 10.0}} {
		lo.Val, hi.Val = bounds[0], bounds[1]
		want, wantErr := evalRawExpr(plan, []any{"5"}, expr)
		got, gotErr := filter([]any{"5"})
		if got != (toTri(want) == tvTrue) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatal("compiled filter retained parameter values", bounds, got, want, gotErr, wantErr)
		}
	}
	input := &Literal{Val: "1", Parameter: true}
	call := &FuncCall{Name: "CONVERT", Args: []Expr{&VarRef{Name: "INT"}, input}}
	compiled := compileRawConversion(columns, call)
	for _, text := range []string{"1", "2", "bad"} {
		input.Val = text
		got, gotErr := compiled(nil)
		want, wantErr := evalRawExpr(plan, nil, call)
		if !conversionValuesEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatal("compiled projection retained a parameter", text, got, want)
		}
	}
}

func TestCompiledConversionQueriesKeepProjectionAndShortCircuit(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE conversion_source (id INT,input TEXT)")
	execSQL(t, db, "INSERT INTO conversion_source VALUES (1,'12'),(2,NULL),(3,'bad'),(4,'12')")
	for _, projection := range []string{"TRY_CONVERT(INT,input) AS value", "CONVERT(VARCHAR(2),input) AS value", "CAST(input AS TEXT) AS value"} {
		for _, prefix := range []string{"SELECT ", "SELECT DISTINCT "} {
			query := prefix + projection + " FROM conversion_source"
			want := execSQL(t, db, prefix+projection+" FROM (SELECT * FROM conversion_source) q")
			got := execSQL(t, db, query)
			if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Cols, want.Cols) {
				t.Fatal(query, got, want)
			}
			stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
			if err != nil {
				t.Fatal(err)
			}
			var rows []Row
			for stream.Next() {
				copy := Row{}
				for key, value := range stream.Row() {
					copy[key] = value
				}
				rows = append(rows, copy)
				stream.ReleaseRow(stream.Row())
			}
			if err := stream.Err(); err != nil {
				t.Fatal(err)
			}
			stream.Close()
			if !reflect.DeepEqual(rows, want.Rows) {
				t.Fatal("streamed conversion differs", query, rows, want.Rows)
			}
		}
	}
	// Conversions remain in the expensive expression tier. A cheap sibling
	// predicate must still exclude invalid input before conversion runs.
	for _, predicate := range []string{
		"CONVERT(INT,input)>0 AND id<3", "id<3 AND CONVERT(INT,input)>0",
		"CAST(input AS INT) BETWEEN 0 AND 20 AND id<3",
		"id=3 OR TRY_CONVERT(INT,input) BETWEEN 0 AND 20",
	} {
		stmt := mustParse("SELECT id FROM conversion_source WHERE " + predicate)
		original := mustParse("SELECT id FROM conversion_source WHERE " + predicate)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 3 {
					if _, err := Execute(t.Context(), db, "default", stmt); err != nil {
						t.Error(predicate, err)
					}
				}
			})
		}
		wg.Wait()
		if !reflect.DeepEqual(stmt.(*Select).Where, original.(*Select).Where) {
			t.Fatal("compiled conversion mutated the shared statement")
		}
	}
}
