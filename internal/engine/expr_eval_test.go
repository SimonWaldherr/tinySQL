package engine

import (
	"fmt"
	"testing"
)

func TestEvalExpr_LiteralsVarRefUnaryBinary(t *testing.T) {
	env := ExecEnv{}
	row := Row{"id": 10, "name": "Alice"}

	// Literal
	if v, err := evalExpr(env, &Literal{Val: 5}, row); err != nil || v != 5 {
		t.Fatalf("literal eval failed: %v %v", v, err)
	}

	// VarRef existing
	if v, err := evalExpr(env, &VarRef{Name: "id"}, row); err != nil || v != 10 {
		t.Fatalf("varref eval failed: %v %v", v, err)
	}

	// VarRef unknown
	if _, err := evalExpr(env, &VarRef{Name: "missing"}, row); err == nil {
		t.Fatalf("expected error for unknown varref")
	}

	// Unary -
	if v, err := evalExpr(env, &Unary{Op: "-", Expr: &Literal{Val: 3}}, row); err != nil || fmt.Sprint(v) != "-3" {
		t.Fatalf("unary minus failed: %v %v", v, err)
	}

	// Binary arithmetic
	if v, err := evalExpr(env, &Binary{Op: "+", Left: &Literal{Val: 2}, Right: &Literal{Val: 3}}, row); err != nil || fmt.Sprint(v) != "5" {
		t.Fatalf("binary + failed: %v %v", v, err)
	}
	if v, err := evalExpr(env, &Binary{Op: "*", Left: &Literal{Val: 4}, Right: &Literal{Val: 2}}, row); err != nil || fmt.Sprint(v) != "8" {
		t.Fatalf("binary * failed: %v %v", v, err)
	}
}

func TestEvalExpr_ComparisonsInLikeCase(t *testing.T) {
	env := ExecEnv{}
	row := Row{"a": 1, "s": "hello"}

	// Comparison
	if v, err := evalExpr(env, &Binary{Op: "=", Left: &Literal{Val: 1}, Right: &Literal{Val: 1}}, row); err != nil || v != true {
		t.Fatalf("comparison = failed: %v %v", v, err)
	}

	// IN
	inExpr := &InExpr{Expr: &Literal{Val: 2}, Values: []Expr{&Literal{Val: 1}, &Literal{Val: 2}}}
	if v, err := evalExpr(env, inExpr, row); err != nil || v != true {
		t.Fatalf("IN failed: %v %v", v, err)
	}

	// LIKE
	likeExpr := &LikeExpr{Expr: &Literal{Val: "abc"}, Pattern: &Literal{Val: "a%"}}
	if v, err := evalExpr(env, likeExpr, row); err != nil || v != true {
		t.Fatalf("LIKE failed: %v %v", v, err)
	}

	// CASE simple
	caseExpr := &CaseExpr{Whens: []CaseWhen{{When: &Literal{Val: true}, Then: &Literal{Val: 5}}}, Else: &Literal{Val: 9}}
	if v, err := evalExpr(env, caseExpr, row); err != nil || v != 5 {
		t.Fatalf("CASE failed: %v %v", v, err)
	}
}

func TestEvalExpr_FuncCallUnknownAndIsNull(t *testing.T) {
	env := ExecEnv{}
	row := Row{"x": nil}

	// unknown function
	if _, err := evalExpr(env, &FuncCall{Name: "NOPE"}, row); err == nil {
		t.Fatalf("expected error for unknown function")
	}

	// IS NULL wrapper
	if v, err := evalExpr(env, &IsNull{Expr: &VarRef{Name: "x"}, Negate: false}, row); err != nil || v != true {
		t.Fatalf("IS NULL failed: %v %v", v, err)
	}
}
