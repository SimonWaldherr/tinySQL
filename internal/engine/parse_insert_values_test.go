package engine

import (
	"fmt"
	"reflect"
	"testing"
)

func TestInsertValueParsingMatchesExpressionParser(t *testing.T) {
	for _, value := range []string{
		"0", "2147483650", "1.5", "1e-7", "'a''b'", "x'00ff'", "NULL", "true", "false",
		"NULL IS NULL", "1 + 2 * 3", "-1", "'a' || 'b'", "(SELECT 2)", "CASE WHEN true THEN 1 END",
		"x'0'", "x'nonhex'",
	} {
		for _, delimiter := range []string{",", ")"} {
			sql := value + delimiter
			fast, regular := NewParser(sql), NewParser(sql)
			actual, actualErr := fast.parseInsertValue()
			expected, expectedErr := regular.parseExpr()
			if !reflect.DeepEqual(actual, expected) || fmt.Sprint(actualErr) != fmt.Sprint(expectedErr) || fast.cur != regular.cur {
				t.Fatalf("parse %q: got %#v, %v at %v; want %#v, %v at %v", sql, actual, actualErr, fast.cur, expected, expectedErr, regular.cur)
			}
		}
	}
	parser := NewParser("1)")
	parser.depth = maxParseDepth
	if _, err := parser.parseInsertValue(); err == nil {
		t.Fatal("literal parsing bypassed the recursion limit")
	}
}
