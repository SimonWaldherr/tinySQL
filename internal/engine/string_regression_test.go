package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestStringExplicitEmptyTrimCutset(t *testing.T) {
	db := storage.NewDB()
	for _, name := range []string{"TRIM", "LTRIM", "RTRIM"} {
		if got := queryScalar(t, db, name+`('  text  ', '')`); got != "  text  " {
			t.Errorf("%s with an empty cutset = %#v", name, got)
		}
		call := &FuncCall{Name: name, Args: []Expr{&VarRef{Name: "v"}, &Literal{Val: ""}}}
		plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
		got, err := evalRawFuncCall(plan, []any{"  text  "}, call)
		if err != nil || got != "  text  " {
			t.Errorf("raw %s with an empty cutset = %#v, %v", name, got, err)
		}
	}
}

func TestSubstringOutOfRangeChecksLength(t *testing.T) {
	db := storage.NewDB()
	for _, expr := range []string{`SUBSTRING('abc', 10, NULL)`, `SUBSTRING('', 1, NULL)`} {
		if got := queryScalar(t, db, expr); got != nil {
			t.Errorf("%s = %#v, want NULL", expr, got)
		}
	}
	_, err := evalSubstring(ExecEnv{}, []Expr{&Literal{Val: "abc"}, &Literal{Val: 10}, &Literal{Val: "not a number"}}, nil)
	if err == nil {
		t.Error("out-of-range substring must still validate its length")
	}
}

func TestRepeatRejectsOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	_, err := evalRepeat(ExecEnv{}, []Expr{&Literal{Val: "ab"}, &Literal{Val: maxInt}}, nil)
	if err == nil {
		t.Fatal("REPEAT must return an error when the result length overflows")
	}
}
