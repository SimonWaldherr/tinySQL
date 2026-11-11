package engine

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
)

func TestBoundTrimLengthMatchesInterpreter(t *testing.T) {
	columns := map[string]int{"value": 0, "cut": 1}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, length := range []string{"LENGTH", "LEN", "CHAR_LENGTH"} {
		for _, trim := range []string{"TRIM", "LTRIM", "RTRIM"} {
			for _, suffix := range []string{"", ",cut"} {
				sql := length + "(" + trim + "(value" + suffix + "))"
				expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
				bound := compileRawValue(columns, expr)
				if bound == nil {
					t.Fatal("trim length did not bind", sql)
				}
				for _, value := range []any{nil, "", " \tα🙂\u2003", "xxα🙂xx", "\xffx\x80", 123, false, []byte(nil), []byte{255, 0}, json.RawMessage(`{"x":1}`), (*big.Rat)(nil)} {
					for _, cut := range []any{nil, "", " x", "α🙂", "\ufffd", "\xff", 0, false, []byte("x")} {
						raw := []any{value, cut}
						want, wantErr := evalRawExpr(plan, raw, expr)
						got, err := bound(raw)
						if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
							t.Fatalf("%s (%#v): %v/%v, want %v/%v", sql, raw, got, err, want, wantErr)
						}
					}
				}
				for _, raw := range [][]any{nil, {nil}, {"x"}} {
					want, wantErr := evalRawExpr(plan, raw, expr)
					got, err := bound(raw)
					if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatalf("short row %s: %v/%v, want %v/%v", sql, got, err, want, wantErr)
					}
				}
			}
		}
	}
}

type trimLengthCoercionProbe struct{ cut *Literal }

func (value trimLengthCoercionProbe) String() string {
	value.cut.Val = "x"
	return "xxα🙂xx"
}

func TestBoundTrimLengthPreservesCoercionOrderAndParameters(t *testing.T) {
	cut := &Literal{Parameter: true}
	trim := &FuncCall{Name: "TRIM", Args: []Expr{newVarRef("value"), cut}}
	expr := &FuncCall{Name: "LENGTH", Args: []Expr{trim}}
	columns := map[string]int{"value": 0}
	bound := compileRawValue(columns, expr)
	plan := &simpleSelectPlan{colIndex: columns}
	raw := []any{trimLengthCoercionProbe{cut}}
	for _, initial := range []any{nil, 123} {
		cut.Val = initial
		got, err := bound(raw)
		if err != nil || got != 2 {
			t.Fatalf("coercion must precede cutset evaluation: %v / %v", got, err)
		}
	}
	for _, value := range []any{nil, "", "x", "α🙂", 123} {
		cut.Val = value
		for _, raw := range [][]any{{nil}, {"xxα🙂xx"}} {
			want, wantErr := evalRawExpr(plan, raw, expr)
			got, err := bound(raw)
			if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("parameter %v: %v/%v, want %v/%v", value, got, err, want, wantErr)
			}
		}
	}
}

func TestBoundTrimLengthAvoidsIntermediateStringBox(t *testing.T) {
	expr := mustParse("SELECT LENGTH(TRIM(value,'x'))").(*Select).Projs[0].Expr
	bound := compileRawValue(map[string]int{"value": 0}, expr)
	raw := []any{"xxα🙂xx"}
	if allocations := testing.AllocsPerRun(100, func() {
		got, err := bound(raw)
		if err != nil || got != 2 {
			t.Fatalf("unexpected value: %v / %v", got, err)
		}
	}); allocations != 0 {
		t.Fatalf("trim length allocated %v objects per row", allocations)
	}
}
