package engine

import (
	"fmt"
	"testing"
)

var mathDecimalBenchmarkSink any

func BenchmarkMathDecimalPrecision(b *testing.B) {
	for _, function := range []struct {
		name string
		eval func(ExecEnv, []Expr, Row) (any, error)
	}{{"ROUND", evalRound}, {"TRUNCATE", evalTruncate}} {
		for _, precision := range []int{0, 2, -2, 310} {
			b.Run(fmt.Sprintf("%s/%d", function.name, precision), func(b *testing.B) {
				value := 1234.56789
				if precision == 310 {
					value = 1.1e-310
				}
				args := []Expr{&Literal{Val: value}, &Literal{Val: precision}}
				b.ReportAllocs()
				for b.Loop() {
					result, err := function.eval(ExecEnv{}, args, nil)
					if err != nil {
						b.Fatal(err)
					}
					mathDecimalBenchmarkSink = result
				}
			})
		}
	}
}
