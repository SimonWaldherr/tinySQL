package engine

import (
	"testing"
	"time"
)

func TestEvalCoalesceAndNullif(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	// COALESCE(NULL, 5) -> 5
	v, err := evalCoalesce(env, []Expr{&Literal{Val: nil}, &Literal{Val: 5}}, row)
	if err != nil {
		t.Fatalf("evalCoalesce error: %v", err)
	}
	if v != 5 {
		t.Fatalf("expected 5, got %v", v)
	}

	// NULLIF equal -> nil
	v, err = evalNullif(env, []Expr{&Literal{Val: 2}, &Literal{Val: 2}}, row)
	if err != nil {
		t.Fatalf("evalNullif error: %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil for equal NULLIF, got %v", v)
	}

	// NULLIF different -> left value
	v, err = evalNullif(env, []Expr{&Literal{Val: 2}, &Literal{Val: 3}}, row)
	if err != nil {
		t.Fatalf("evalNullif error: %v", err)
	}
	if v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestEvalJSONGetAndSet(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	m := map[string]any{"a": map[string]any{"b": 5}}
	// JSON_GET
	v, err := evalJSONGet(env, []Expr{&Literal{Val: m}, &Literal{Val: "a.b"}}, row)
	if err != nil {
		t.Fatalf("evalJSONGet error: %v", err)
	}
	if v != 5 {
		t.Fatalf("expected 5, got %v", v)
	}

	// JSON_SET
	ex := &FuncCall{Name: "JSON_SET", Args: []Expr{&Literal{Val: m}, &Literal{Val: "a.c"}, &Literal{Val: 7}}}
	out, err := evalJSONExtended(env, ex, row)
	if err != nil {
		t.Fatalf("evalJSONExtended(JSON_SET) error: %v", err)
	}
	// ensure the new structure has the set value
	om := out.(map[string]any)
	inner, _ := om["a"].(map[string]any)
	if inner["c"] != 7 {
		t.Fatalf("expected a.c == 7, got %v", inner["c"])
	}
}

func TestCountAndAggregateSingle(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	// COUNT(*)
	ex := &FuncCall{Star: true}
	v, err := evalCountSingle(env, ex, row)
	if err != nil {
		t.Fatalf("evalCountSingle error: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %v", v)
	}

	// SUM-like aggregate (returns value)
	ex2 := &FuncCall{Name: "SUM", Args: []Expr{&Literal{Val: 42}}}
	v, err = evalAggregateSingle(env, ex2, row)
	if err != nil {
		t.Fatalf("evalAggregateSingle error: %v", err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
}

func TestParseTimeAndDateDiff(t *testing.T) {
	// parseTimeValue with string
	if _, err := parseTimeValue("2020-01-02"); err != nil {
		t.Fatalf("parseTimeValue failed: %v", err)
	}

	// parseTimeValue with nil should error
	if _, err := parseTimeValue(nil); err == nil {
		t.Fatalf("parseTimeValue expected error for nil")
	}

	env := ExecEnv{}
	row := Row{}
	ex := &FuncCall{Name: "DATEDIFF", Args: []Expr{&Literal{Val: "DAYS"}, &Literal{Val: "2020-01-01"}, &Literal{Val: "2020-01-03"}}}
	v, err := evalDateDiff(env, ex, row)
	if err != nil {
		t.Fatalf("evalDateDiff error: %v", err)
	}
	if iv, ok := v.(int); !ok || iv != 2 {
		t.Fatalf("expected 2 days diff, got %v", v)
	}

	// unsupported unit
	ex2 := &FuncCall{Name: "DATEDIFF", Args: []Expr{&Literal{Val: "FOO"}, &Literal{Val: "2020-01-01"}, &Literal{Val: "2020-01-03"}}}
	if _, err := evalDateDiff(env, ex2, row); err == nil {
		t.Fatalf("expected error for unsupported unit")
	}

	// date parsing with time.Time
	now := time.Now()
	if tval, err := parseTimeValue(now); err != nil || !tval.Equal(now) {
		t.Fatalf("parseTimeValue time.Time failed: %v %v", tval, err)
	}
}
