package engine

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"unicode"
)

func TestTrimEdgesMatchUnicode(t *testing.T) {
	check := func(s string) {
		t.Helper()
		left, _ := trimStringValue(s, s, "", trimLeft)
		if got, want := left, strings.TrimLeftFunc(s, unicode.IsSpace); got != want {
			t.Fatalf("LTRIM(%q) = %q, want %q", s, got, want)
		}
		right, _ := trimStringValue(s, s, "", trimRight)
		if got, want := right, strings.TrimRightFunc(s, unicode.IsSpace); got != want {
			t.Fatalf("RTRIM(%q) = %q, want %q", s, got, want)
		}
	}
	for _, s := range []string{
		"", "text", " \t\n\v\f\r", " \ttext \t", " \u00a0\u2003text\u202f\u3000 ",
		"\u0085", " \xfftext\xfe ", " \xc2 ", " \ufffd ", "\x00 ",
		"  " + strings.Repeat("sample", 10000) + "  ",
	} {
		check(s)
	}
	// Include every Unicode whitespace rune, combinations with ASCII spaces,
	// and invalid UTF-8 bytes that must remain in the result.
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if unicode.IsSpace(r) {
			check(" \t" + string(r) + "text" + string(r) + "\r ")
			check(string(r) + " \t" + string(r))
		}
	}
	rng := rand.New(rand.NewSource(7))
	for range 2000 {
		data := make([]byte, rng.Intn(128))
		_, _ = rng.Read(data)
		check(" \u00a0" + string(data) + "\u2003 ")
	}
}

func TestTrimEdgesEvaluatorParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, name := range []string{"LTRIM", "RTRIM", "TRIM"} {
		for _, value := range []any{nil, "  text  ", " \u2003漢🙂\u00a0 ", " \xff ", 123} {
			for _, cutset := range [][]Expr{nil, {&Literal{Val: " 漢"}}, {&Literal{Val: nil}}, {&Literal{Val: 1}}, {&VarRef{Name: "missing"}}} {
				call := &FuncCall{Name: name, Args: append([]Expr{&VarRef{Name: "v"}}, cutset...)}
				want, wantErr := evalFuncCall(ExecEnv{}, call, Row{"v": value})
				got, gotErr := evalRawFuncCall(plan, []any{value}, call)
				if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("%s(%v): raw %v/%v, general %v/%v", name, value, got, gotErr, want, wantErr)
				}
			}
		}
	}
}
