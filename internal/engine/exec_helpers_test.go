package engine

import (
	"testing"
)

func TestStringifySQLValueAndNumeric(t *testing.T) {
	if s := stringifySQLValue(nil); s != "" {
		t.Fatalf("expected empty string for nil, got %q", s)
	}
	if s := stringifySQLValue("abc"); s != "abc" {
		t.Fatalf("expected 'abc', got %q", s)
	}
	if s := stringifySQLValue([]byte("xyz")); s != "xyz" {
		t.Fatalf("expected 'xyz', got %q", s)
	}
	if s := stringifySQLValue(123); s != "123" {
		t.Fatalf("expected '123', got %q", s)
	}

	if f, ok := numeric(5); !ok || f != 5.0 {
		t.Fatalf("numeric(int) failed: %v %v", f, ok)
	}
	if f, ok := numeric(int64(7)); !ok || f != 7.0 {
		t.Fatalf("numeric(int64) failed: %v %v", f, ok)
	}
	if f, ok := numeric(3.14); !ok || f != 3.14 {
		t.Fatalf("numeric(float64) failed: %v %v", f, ok)
	}
	if _, ok := numeric("nope"); ok {
		t.Fatalf("numeric should return false for non-numeric")
	}
}

func TestTriLogic(t *testing.T) {
	if toTri(nil) != tvUnknown {
		t.Fatalf("toTri(nil) expected unknown")
	}
	if toTri(true) != tvTrue {
		t.Fatalf("toTri(true) expected true")
	}
	if toTri(false) != tvFalse {
		t.Fatalf("toTri(false) expected false")
	}

	if triNot(tvTrue) != tvFalse || triNot(tvFalse) != tvTrue || triNot(tvUnknown) != tvUnknown {
		t.Fatalf("triNot unexpected results")
	}

	if triAnd(tvTrue, tvTrue) != tvTrue {
		t.Fatalf("triAnd true,true expected true")
	}
	if triAnd(tvFalse, tvUnknown) != tvFalse {
		t.Fatalf("triAnd false,unknown expected false")
	}
	if triOr(tvFalse, tvFalse) != tvFalse {
		t.Fatalf("triOr false,false expected false")
	}
	if triOr(tvTrue, tvUnknown) != tvTrue {
		t.Fatalf("triOr true,unknown expected true")
	}
}

func TestCompareHelpers(t *testing.T) {
	if c, err := compareFloat(1.5, 2); err != nil || c != -1 {
		t.Fatalf("compareFloat expected -1 got %v err %v", c, err)
	}
	if c, err := compareFloat(2.0, 2); err != nil || c != 0 {
		t.Fatalf("compareFloat expected 0 got %v err %v", c, err)
	}
	if _, err := compareFloat(1.0, "x"); err == nil {
		t.Fatalf("compareFloat expected error for non-numeric")
	}

	if c, err := compareString("a", "b"); err != nil || c != -1 {
		t.Fatalf("compareString expected -1 got %v err %v", c, err)
	}
	if _, err := compareString("a", 1); err == nil {
		t.Fatalf("compareString expected error for non-string")
	}

	if c, err := compareBool(true, false); err != nil || c != 1 {
		t.Fatalf("compareBool expected 1 got %v err %v", c, err)
	}
	if _, err := compareBool(true, "x"); err == nil {
		t.Fatalf("compareBool expected error for non-bool")
	}
}
