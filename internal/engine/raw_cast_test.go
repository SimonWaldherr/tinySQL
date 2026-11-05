package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawCastMatchesGeneral(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, target := range []Expr{&Literal{Val: "FLOAT"}, &VarRef{Name: "FLOAT"}, &Literal{Val: "INT"}, &Literal{Val: "TEXT"}, &Literal{Val: "unsupported"}, &Literal{Val: 3}} {
		for _, value := range []any{nil, "12.5", "invalid", 7, 2.5} {
			call := &FuncCall{Name: "CAST", Args: []Expr{&VarRef{Name: "v", Lower: "v"}, target}}
			want, wantErr := evalCast(ExecEnv{}, call.Args, Row{"v": value})
			got, err := evalRawFuncCall(plan, []any{value}, call)
			if !reflect.DeepEqual(got, want) || (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() {
				t.Fatalf("CAST(%v, %v): got %v / %v, want %v / %v", value, target, got, err, want, wantErr)
			}
		}
	}
	// Arity is checked before resolving a missing input column.
	for _, args := range [][]Expr{nil, {&VarRef{Name: "missing"}}} {
		_, got := evalRawFuncCall(plan, nil, &FuncCall{Name: "CAST", Args: args})
		_, want := evalCast(ExecEnv{}, args, nil)
		if got == nil || got.Error() != want.Error() {
			t.Fatalf("arity: %v, want %v", got, want)
		}
	}
}

func TestSnapshotStyleCastAggregateUsesRawPlan(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE cast_facts (region TEXT, amount TEXT)")
	execSQL(t, db, "INSERT INTO cast_facts VALUES ('a','1.5'),('a','2.5'),('b',NULL),('b','4')")
	query := "SELECT region, SUM(CAST(amount AS FLOAT)) AS total, AVG(CAST(amount AS FLOAT)) AS mean FROM cast_facts GROUP BY region ORDER BY region"
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	if _, ok, err := buildSimpleAggregatePlan(env, mustParse(query).(*Select)); !ok || err != nil {
		t.Fatalf("raw plan: %v, %v", ok, err)
	}
	got := execSQL(t, db, query)
	want := execSQL(t, db, "SELECT region, SUM(CAST(amount AS FLOAT)) AS total, AVG(CAST(amount AS FLOAT)) AS mean FROM (SELECT * FROM cast_facts) AS f GROUP BY region ORDER BY region")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func BenchmarkRawCast(b *testing.B) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	raw := []any{12.5}
	call := &FuncCall{Name: "CAST", Args: []Expr{&VarRef{Name: "v", Lower: "v"}, &Literal{Val: "FLOAT"}}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := evalRawFuncCall(plan, raw, call); err != nil {
			b.Fatal(err)
		}
	}
}
