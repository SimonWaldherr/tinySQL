package sqlbind

import (
	"strings"
	"testing"
	"time"
)

func TestBindSkipsStringsIdentifiersAndComments(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		{"positional", "INSERT INTO t VALUES (?, ?)", []any{1, "O'Hara"}, "INSERT INTO t VALUES (1, 'O''Hara')"},
		{"numbered", "SELECT $2, :1, $2", []any{"a", 2}, "SELECT 2, 'a', 2"},
		{"string", "SELECT '?:1$1' , ?", []any{3}, "SELECT '?:1$1' , 3"},
		{"escaped string", "SELECT 'it''s ?', ?", []any{true}, "SELECT 'it''s ?', TRUE"},
		{"quoted identifier", `SELECT "why?" FROM t WHERE id = ?`, []any{4}, `SELECT "why?" FROM t WHERE id = 4`},
		{"backtick identifier", "SELECT `a?b` FROM t WHERE id = ?", []any{5}, "SELECT `a?b` FROM t WHERE id = 5"},
		{"line comment", "SELECT ? -- really?\nFROM t", []any{6}, "SELECT 6 -- really?\nFROM t"},
		{"block comment", "SELECT /* $1 ? */ ?", []any{7}, "SELECT /* $1 ? */ 7"},
		{"trailing comment without args", "SELECT 1 -- why?", nil, "SELECT 1 -- why?"},
		{"unterminated comment", "SELECT ? /* ?", []any{8}, "SELECT 8 /* ?"},
		{"blob and nil", "SELECT ?, ?", []any{[]byte{0, 255}, nil}, "SELECT X'00ff', NULL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BindValues(tc.sql, tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBindValidation(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		args []any
		want string
	}{
		{"SELECT ?", nil, "not enough args"},
		{"SELECT 1", []any{1}, "arg 1 unused"},
		{"SELECT $3", []any{1, 2}, "invalid placeholder"},
		{"SELECT $0", []any{1}, "invalid placeholder"},
		{"SELECT $99999999999999999999999", []any{1}, "invalid placeholder"},
	} {
		if _, err := BindValues(tc.sql, tc.args...); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("BindValues(%q): error %v, want %q", tc.sql, err, tc.want)
		}
	}
}

func TestLiteralKinds(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("x", 3600))
	for _, tc := range []struct {
		value any
		want  string
	}{
		{int8(-8), "-8"}, {int16(16), "16"}, {int32(32), "32"}, {uint(7), "7"},
		{uint8(8), "8"}, {uint16(16), "16"}, {uint32(32), "32"}, {uint64(1 << 63), "9223372036854775808"},
		{float32(0.5), "0.5"}, {1e-7, "0.0000001"}, {false, "FALSE"},
		{when, "'2026-01-02T02:04:05.000000006Z'"},
		{map[string]string{"k": "it's"}, `'{"k":"it''s"}'`},
	} {
		if got := Literal(tc.value); got != tc.want {
			t.Errorf("Literal(%#v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestSkipOpaque(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want int
	}{
		{"'a''b' x", 6}, {`"a""b" x`, 6}, {"-- c\nx", 4}, {"/* c */x", 7}, {"x", 0}, {"-x", 0}, {"/x", 0}, {"'open", 5},
	} {
		if got := SkipOpaque(tc.sql, 0); got != tc.want {
			t.Errorf("SkipOpaque(%q) = %d, want %d", tc.sql, got, tc.want)
		}
	}
}
