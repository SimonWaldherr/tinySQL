package engine

import "testing"

func TestStringFunctions(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	// UPPER / LOWER
	if v, err := evalUpper(env, []Expr{&Literal{Val: "hello"}}, row); err != nil || v != "HELLO" {
		t.Fatalf("evalUpper failed: %v %v", v, err)
	}
	if v, err := evalLower(env, []Expr{&Literal{Val: "HeLLo"}}, row); err != nil || v != "hello" {
		t.Fatalf("evalLower failed: %v %v", v, err)
	}

	// CONCAT
	if v, err := evalConcat(env, []Expr{&Literal{Val: "a"}, &Literal{Val: "b"}, &Literal{Val: 1}}, row); err != nil || v != "ab1" {
		t.Fatalf("evalConcat failed: %v %v", v, err)
	}

	// LENGTH
	if v, err := evalLength(env, []Expr{&Literal{Val: "abc"}}, row); err != nil || v != 3 {
		t.Fatalf("evalLength failed: %v %v", v, err)
	}

	// SUBSTRING
	if v, err := evalSubstring(env, []Expr{&Literal{Val: "hello"}, &Literal{Val: 2}, &Literal{Val: 3}}, row); err != nil || v != "ell" {
		t.Fatalf("evalSubstring failed: %v %v", v, err)
	}

	// LEFT / RIGHT
	if v, err := evalLeft(env, []Expr{&Literal{Val: "abcd"}, &Literal{Val: 2}}, row); err != nil || v != "ab" {
		t.Fatalf("evalLeft failed: %v %v", v, err)
	}
	if v, err := evalRight(env, []Expr{&Literal{Val: "abcd"}, &Literal{Val: 2}}, row); err != nil || v != "cd" {
		t.Fatalf("evalRight failed: %v %v", v, err)
	}

	// REVERSE
	if v, err := evalReverse(env, []Expr{&Literal{Val: "abc"}}, row); err != nil || v != "cba" {
		t.Fatalf("evalReverse failed: %v %v", v, err)
	}

	// REPEAT
	if v, err := evalRepeat(env, []Expr{&Literal{Val: "x"}, &Literal{Val: 3}}, row); err != nil || v != "xxx" {
		t.Fatalf("evalRepeat failed: %v %v", v, err)
	}
}
