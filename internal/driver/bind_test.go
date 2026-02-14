package driver

import (
	"database/sql/driver"
	"testing"
)

func nv(v any) driver.NamedValue { return driver.NamedValue{Value: v} }

func TestBindPlaceholders_Sequential(t *testing.T) {
	q := "INSERT INTO t (a,b) VALUES (?,?)"
	out, err := bindPlaceholders(q, []driver.NamedValue{nv(1), nv("O'Hara")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "INSERT INTO t (a,b) VALUES (1,'O''Hara')"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
}

func TestBindPlaceholders_Numbered(t *testing.T) {
	q := "SELECT $2, $1"
	out, err := bindPlaceholders(q, []driver.NamedValue{nv(10), nv("x")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "SELECT 'x', 10"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
}

func TestBindPlaceholders_QuotedLiteral(t *testing.T) {
	q := "SELECT '?' as q, col FROM t WHERE name='it's'"
	out, err := bindPlaceholders(q, []driver.NamedValue{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != q {
		t.Fatalf("quoted literal changed: got %q want %q", out, q)
	}
}

func TestSqlLiteral_Complex(t *testing.T) {
	if got := sqlLiteral(nil); got != "NULL" {
		t.Fatalf("nil literal: %s", got)
	}
	if got := sqlLiteral(true); got != "TRUE" {
		t.Fatalf("bool literal: %s", got)
	}
	if got := sqlLiteral(42); got != "42" {
		t.Fatalf("int literal: %s", got)
	}
	if got := sqlLiteral(3.14); got != "3.14" {
		t.Fatalf("float literal: %s", got)
	}
	if got := sqlLiteral("a'b"); got != "'a''b'" {
		t.Fatalf("string literal escaping: %s", got)
	}
}
