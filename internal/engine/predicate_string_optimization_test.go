package engine

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestStringEdgesPreserveRuneSemantics(t *testing.T) {
	for _, s := range []string{"", "ascii", "é漢🙂abc", "\xffabc\xfe", "a\xc3\xa9\xff🙂", strings.Repeat("é漢🙂", 10000)} {
		for _, n := range []int{-1, 0, 1, 2, 4, 100000} {
			for _, right := range []bool{false, true} {
				want := s
				if n <= 0 {
					want = ""
				} else if n < utf8.RuneCountInString(s) {
					r := []rune(s)
					if right {
						want = string(r[len(r)-n:])
					} else {
						want = string(r[:n])
					}
				}
				got := stringEdge(s, s, n, right)
				if got != want {
					t.Fatalf("edge %q n=%d right=%v: %q != %q", s, n, right, got, want)
				}
			}
		}
	}
}

func TestInFilterMatchesRawEquality(t *testing.T) {
	lists := [][]any{{}, {nil}, {float64(1), nil}, {int(1), int64(2)}, {int64(9007199254740993)}, {float64(9007199254740992)}, {"x", nil}, {true, []byte("x"), float64(2), "x"}, {math.NaN()}}
	values := []any{nil, int(1), int64(2), float64(1), int64(9007199254740992), int64(9007199254740993), float64(9007199254740992), "x", "y", true, []byte("x"), math.NaN()}
	for _, list := range lists {
		for _, negate := range []bool{false, true} {
			f := buildInFilter(0, list, negate)
			for _, v := range values {
				found, null := false, v == nil
				for _, x := range list {
					if x == nil {
						null = true
					} else if v != nil && rawEqual(v, x) {
						found = true
					}
				}
				want := v != nil && ((found && !negate) || (!found && negate && !null))
				got, err := f([]any{v})
				if err != nil || got != want {
					t.Fatalf("%v IN %v negate=%v: %v, %v want %v", v, list, negate, got, err, want)
				}
			}
		}
	}
}

func TestInNullAcrossEvaluators(t *testing.T) {
	for _, v := range []any{nil, float64(1), float64(2)} {
		for _, negate := range []bool{false, true} {
			ex := &InExpr{Expr: &Literal{Val: v}, Values: []Expr{&Literal{Val: float64(1)}, &Literal{Val: nil}}, Negate: negate}
			var want any
			if v == float64(1) {
				want = !negate
			}
			general, e1 := evalIn(ExecEnv{}, ex, nil)
			raw, e2 := evalRawIn(&simpleSelectPlan{}, nil, ex)
			join, e3 := evalJoinRawIn(&simpleJoinPlan{}, nil, nil, ex)
			if e1 != nil || e2 != nil || e3 != nil || general != want || raw != want || join != want {
				t.Fatalf("v=%v negate=%v: general=%v raw=%v join=%v want=%v errors=%v/%v/%v", v, negate, general, raw, join, want, e1, e2, e3)
			}
		}
	}
	ex := &InExpr{Expr: &VarRef{Name: "v"}, Values: []Expr{&Literal{Val: 1, Parameter: true}}}
	if buildRawFilterIn(map[string]int{"v": 0}, ex) != nil {
		t.Fatal("bound parameter was captured")
	}
}

func TestBetweenFastPathMatchesComparisons(t *testing.T) {
	values := []any{nil, int(1), int(2), int64(9007199254740993), float64(1), float64(2), math.NaN(), math.Inf(1), "a", "z"}
	for _, v := range values {
		for _, lo := range values {
			for _, hi := range values {
				for _, negate := range []bool{false, true} {
					op1, op2 := ">=", "<="
					if negate {
						op1, op2 = "<", ">"
					}
					a, e1 := evalComparisonBinary(op1, v, lo)
					b, e2 := evalComparisonBinary(op2, v, hi)
					got, err := betweenResult(v, lo, hi, negate)
					if e1 != nil || e2 != nil {
						if err == nil {
							t.Fatal("lost comparison error")
						}
						continue
					}
					want := triToValue(triAnd(toTri(a), toTri(b)))
					if negate {
						want = triToValue(triOr(toTri(a), toTri(b)))
					}
					if err != nil || got != want {
						t.Fatalf("%v BETWEEN %v AND %v negate=%v got=%v want=%v err=%v", v, lo, hi, negate, got, want, err)
					}
				}
			}
		}
	}
}

func TestRawCaseConversionMatchesGeneral(t *testing.T) {
	for _, name := range []string{"UPPER", "LOWER"} {
		for _, v := range []any{nil, "é漢🙂AbC", "ABC", 123, "\xffabc"} {
			ex := &FuncCall{Name: name, Args: []Expr{&Literal{Val: v}}}
			got, err := evalRawFuncCall(&simpleSelectPlan{}, nil, ex)
			want, werr := evalFuncCall(ExecEnv{}, ex, nil)
			if err != nil || werr != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s(%v): %v/%v != %v/%v", name, v, got, err, want, werr)
			}
		}
	}
}

func TestFixedWidthLikeMatchesGeneral(t *testing.T) {
	patterns := []string{"_", "__", "a_", "_a", "é_🙂", "a__z", "document____.json", "\ufffd_", "_\\_", "_%_"}
	values := []string{"", "a", "aa", "a🙂", "é漢🙂", "a\xffz", "a\xff\xfez", "document2026.json", "document２０２６.json", "document2026.jsonx"}
	for _, p := range patterns {
		for _, s := range values {
			for _, insensitive := range []bool{false, true} {
				sp, pp := s, p
				if insensitive {
					sp, pp = strings.ToLower(s), strings.ToLower(p)
				}
				want := matchLikePattern(sp, pp, '\\')
				if got := compileLikeStringMatcher(p, insensitive)(s); got != want {
					t.Fatalf("%q LIKE %q insensitive=%v: %v want %v", s, p, insensitive, got, want)
				}
			}
		}
	}
}
