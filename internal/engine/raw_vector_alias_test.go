package engine

import (
	"testing"
)

func TestRawVectorMetricAliases(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"a": 0, "b": 1}}
	for _, values := range [][]any{
		{[]float64{1, 2, 3}, []float64{4, 5, 6}},
		{`[1,2,3]`, `[4,5,6]`},
		{[]float64{1}, []float64{1, 2}},
	} {
		for _, alias := range []string{"VEC_DOT_PRODUCT", "VEC_INNER_PRODUCT", "VEC_EUCLIDEAN_DISTANCE", "VEC_L1_DISTANCE"} {
			call := mustParse("SELECT " + alias + "(a, b)").(*Select).Projs[0].Expr.(*FuncCall)
			want, wantErr := evalExpr(ExecEnv{}, call, Row{"a": values[0], "b": values[1]})
			got, handled, err := evalRawVectorFunc(plan, values, call)
			if !handled || (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() {
				t.Fatalf("%s: raw=%v, handled=%v, error=%v; ordinary=%v, %v", alias, got, handled, err, want, wantErr)
			}
			if got != want {
				t.Errorf("%s: raw=%v; ordinary=%v", alias, got, want)
			}
		}
	}
}
