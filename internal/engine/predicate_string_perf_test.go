package engine

import (
	"strings"
	"testing"
)

func BenchmarkShortStringEdges(b *testing.B) {
	for _, name := range []string{"LEFT", "RIGHT"} {
		b.Run(name, func(b *testing.B) {
			args := []Expr{&Literal{Val: strings.Repeat("é漢🙂", 10000)}, &Literal{Val: 4}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if name == "LEFT" {
					_, err = evalLeft(ExecEnv{}, args, nil)
				} else {
					_, err = evalRight(ExecEnv{}, args, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func BenchmarkIntegerInFloatLiterals(b *testing.B) {
	values := make([]any, 1000)
	for i := range values {
		values[i] = float64(i)
	}
	for _, negate := range []bool{false, true} {
		name := "IN"
		if negate {
			name = "NOT_IN"
		}
		b.Run(name, func(b *testing.B) {
			filter := buildInFilter(0, values, negate)
			row := []any{int64(2000)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := filter(row)
				if err != nil || got != negate {
					b.Fatal(got, err)
				}
			}
		})
	}
}
func BenchmarkBetweenNumeric(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, err := betweenResult(float64(50), float64(10), float64(100), false)
		if err != nil || got != true {
			b.Fatal(got, err)
		}
	}
}
func BenchmarkRawUpper(b *testing.B) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	ex := &FuncCall{Name: "UPPER", Args: []Expr{&VarRef{Name: "v", Lower: "v"}}}
	raw := []any{"already UPPER text"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := evalRawFuncCall(plan, raw, ex); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLikeFixedWidth(b *testing.B) {
	match := compileLikeStringMatcher("document____.json", false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !match("document2026.json") {
			b.Fatal("no match")
		}
	}
}
