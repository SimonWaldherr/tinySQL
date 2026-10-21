package engine

import (
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestStringCharCountMatchesUTF8(t *testing.T) {
	check := func(s string) {
		t.Helper()
		if got, want := stringCharCount(s), utf8.RuneCountInString(s); got != want {
			t.Fatalf("count(%q) = %d, want %d", s, got, want)
		}
		ascii := true
		for _, b := range []byte(s) {
			ascii = ascii && b < utf8.RuneSelf
		}
		if stringIsASCII(s) != ascii {
			t.Fatalf("wrong ASCII classification for %q", s)
		}
	}
	for length := 0; length <= 80; length++ {
		base := strings.Repeat("a", length)
		check(base)
		for pos := 0; pos <= length; pos++ {
			for _, middle := range []string{"é", "漢🙂", "\xff", "\x80\xff", "\xe2\x82", "\xc0\xaf", "\x00"} {
				check(base[:pos] + middle + base[pos:])
			}
		}
	}
	rng := rand.New(rand.NewSource(31))
	for range 2000 {
		data := make([]byte, rng.Intn(512))
		_, _ = rng.Read(data)
		check(strings.Repeat("a", rng.Intn(32)) + string(data))
	}
}

func TestLengthMixedTextEvaluators(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, name := range []string{"LENGTH", "CHAR_LENGTH"} {
		call := &FuncCall{Name: name, Args: []Expr{&VarRef{Name: "v"}}}
		for _, value := range []any{"", "é漢🙂", strings.Repeat("a", 16384) + "🙂", "a\xff\xc2", []byte{0xff, 0x80, 0}, nil} {
			want, wantErr := evalFuncCall(ExecEnv{}, call, Row{"v": value})
			got, gotErr := evalRawFuncCall(plan, []any{value}, call)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("%s(%v): raw %v/%v, general %v/%v", name, value, got, gotErr, want, wantErr)
			}
		}
	}
}
