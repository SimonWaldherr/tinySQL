package engine

import (
	"regexp"
	"strings"
	"testing"
)

func TestRegexpTextOperationsMatchGo(t *testing.T) {
	columns := map[string]int{"text": 0, "replacement": 1}
	for _, pattern := range []string{"", "abc", "é", `^abc$`, `(?m)^abc$`, `\babc\b`, `a(b)(c)`, `(?i)s`, `a{3}`, `\x{FFFD}`, `\.`, `a([0-9]{2})`, `^a[0-9]{2}`, `[0-9]{2}$`, `^a[0-9]{2}$`, `aa[0-9]`} {
		re := regexp.MustCompile(pattern)
		args := []Expr{&VarRef{Name: "text"}, &Literal{Val: pattern}, &VarRef{Name: "replacement"}}
		extract := compileRawValue(columns, &FuncCall{Name: "REGEXP_EXTRACT", Args: args[:2]})
		replace := compileRawValue(columns, &FuncCall{Name: "REGEXP_REPLACE", Args: args})
		if extract == nil || replace == nil {
			t.Fatalf("could not bind %q", pattern)
		}
		for _, text := range []any{nil, "", "abc", "xabc", "abc\nx", "x abc y", "abcabc", "aaabc", "éabc", "x\xffabc", ".", 12, "a12", "xa12a34", "a12a34", "a12\n", "\xffaaa5"} {
			var wantExtract any
			if text != nil {
				if match := re.FindString(valueText(text)); match != "" {
					wantExtract = match
				}
			}
			if got, err := extract([]any{text}); err != nil || got != wantExtract {
				t.Fatalf("extract %q from %v = %v, %v; want %v", pattern, text, got, err, wantExtract)
			}
			for _, replacement := range []any{nil, "", "word", "abc", "$2$1", "$$", "${0}"} {
				var want any
				if text != nil && replacement != nil {
					want = re.ReplaceAllString(valueText(text), valueText(replacement))
				}
				if got, err := replace([]any{text, replacement}); err != nil || got != want {
					t.Fatalf("replace %q with %v in %v = %v, %v; want %v", pattern, replacement, text, got, err, want)
				}
			}
		}
	}
}

func TestRegexpProjectionErrorsAndBinding(t *testing.T) {
	columns := map[string]int{"text": 0}
	for _, sql := range []string{"REGEXP_EXTRACT(text,'[')", "REGEXP_REPLACE(text,'[','')"} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		if compileRawValue(columns, expr) != nil {
			t.Fatalf("invalid pattern bound for %s", sql)
		}
		plan := &simpleSelectPlan{colIndex: columns}
		if result, err := evalRawExpr(plan, []any{nil}, expr); err != nil || result != nil {
			t.Fatalf("NULL input evaluated an invalid pattern: %v, %v", result, err)
		}
		if _, err := evalRawExpr(plan, []any{"text"}, expr); err == nil || !strings.Contains(err.Error(), "REGEXP_") {
			t.Fatalf("invalid pattern error = %v", err)
		}
	}
	expr := mustParse(`SELECT REGEXP_REPLACE(text, 'a', 1/0)`).(*Select).Projs[0].Expr
	if bound := compileRawValue(columns, expr); bound == nil {
		t.Fatal("REGEXP_REPLACE was not bound")
	} else if _, err := bound([]any{nil}); err == nil {
		t.Fatal("NULL input skipped the replacement error")
	}
	call := mustParse(`SELECT REGEXP_EXTRACT(text, 'a')`).(*Select).Projs[0].Expr.(*FuncCall)
	call.Args[1].(*Literal).Parameter = true
	if compileRawValue(columns, call) != nil {
		t.Fatal("bound pattern must remain dynamic")
	}
}

func TestRegexpFixedReplacementShrinksLargeInput(t *testing.T) {
	pattern := `a[0-9]{2}`
	text := strings.Repeat("a12", 2000) + "tail"
	re, err := loadCachedRegexp(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if got := re.replaceText(text, text, ""); got != "tail" {
		t.Fatalf("replacement produced %q, want tail", got)
	}
}
