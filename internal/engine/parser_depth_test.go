// Tests for the parser's recursion depth guard (maxParseDepth in parser.go).
// Without it, a maliciously/accidentally deeply nested expression or
// subquery recurses once per nesting level through the whole precedence
// chain, which can exhaust the goroutine stack — a Go "fatal error: stack
// overflow" that recover() cannot catch, unlike an ordinary panic.
package engine

import (
	"strings"
	"testing"
)

func TestParserRejectsDeeplyNestedParens(t *testing.T) {
	depth := maxParseDepth + 50
	sql := "SELECT " + strings.Repeat("(", depth) + "1" + strings.Repeat(")", depth)
	_, err := NewParser(sql).ParseStatement()
	if err == nil {
		t.Fatal("expected a parse error for pathologically deep nesting, got nil")
	}
	if !strings.Contains(err.Error(), "nested too deeply") {
		t.Errorf("expected a 'nested too deeply' error, got: %v", err)
	}
}

func TestParserRejectsDeeplyNestedSubqueries(t *testing.T) {
	depth := maxParseDepth + 20
	sql := "SELECT * FROM t"
	for i := 0; i < depth; i++ {
		sql = "SELECT * FROM (" + sql + ") s" + itoaDepth(i)
	}
	_, err := NewParser(sql).ParseStatement()
	if err == nil {
		t.Fatal("expected a parse error for pathologically deep subquery nesting, got nil")
	}
	if !strings.Contains(err.Error(), "nested too deeply") {
		t.Errorf("expected a 'nested too deeply' error, got: %v", err)
	}
}

func itoaDepth(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

// TestParserAllowsReasonableNesting confirms the guard doesn't reject normal,
// realistic queries — only pathological input.
func TestParserAllowsReasonableNesting(t *testing.T) {
	sql := "SELECT " + strings.Repeat("(", 20) + "1" + strings.Repeat(")", 20) + " AS x"
	if _, err := NewParser(sql).ParseStatement(); err != nil {
		t.Errorf("expected 20 levels of nesting to parse fine, got: %v", err)
	}

	nested := "SELECT * FROM t"
	for i := 0; i < 10; i++ {
		nested = "SELECT * FROM (" + nested + ") s" + itoaDepth(i)
	}
	if _, err := NewParser(nested).ParseStatement(); err != nil {
		t.Errorf("expected 10 levels of subquery nesting to parse fine, got: %v", err)
	}
}
