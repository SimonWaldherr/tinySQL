package engine

import (
	"fmt"
	"math"
	"math/big"
	"testing"
)

func TestJoinPredicateMatchesMergedEvaluation(t *testing.T) {
	values := []any{nil, 1, int64(1), int64(1<<54) + 1, 2.0, math.NaN(), "1", big.NewRat(1, 1), []byte{1}}
	env := ExecEnv{ctx: t.Context()}
	for _, predicate := range []string{
		"l.val <= r.val AND r.val <> 2",
		"l.val < r.val OR r.val >= l.val",
		"shared = l.val",
		"FALSE AND missing = r.val",
		"TRUE OR missing = r.val",
		"NULL AND missing = r.val",
		"NULL OR missing = r.val",
	} {
		stmt := mustParse("SELECT * FROM l JOIN r ON " + predicate).(*Select)
		on := stmt.Joins[0].On
		compiled := compileJoinPredicate(env, on)
		if compiled == nil {
			t.Fatalf("%s did not compile", predicate)
		}
		for _, left := range values {
			for _, right := range values {
				l := Row{"l.val": left, "shared": left}
				r := Row{"r.val": right, "shared": right}
				got, err := compiled(l, r)
				want, wantErr := evalExpr(env, on, mergeRows(l, r))
				if fmt.Sprint(err) != fmt.Sprint(wantErr) || (err == nil && got != toTri(want)) {
					t.Fatalf("%s (%v, %v): got %v, %v, want %v, %v", predicate, left, right, got, err, toTri(want), wantErr)
				}
			}
		}
	}
}
