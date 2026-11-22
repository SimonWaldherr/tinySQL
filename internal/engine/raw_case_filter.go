package engine

import (
	"strings"
	"unicode/utf8"
)

// Match a converted text value against a string literal without building an
// intermediate string for every ASCII row. Keep this in the expression tier
// so AND/OR retain their existing evaluation order.
func buildRawCaseComparisonFilter(columns map[string]int, expr Expr) func([]any) (bool, error) {
	binary, ok := expr.(*Binary)
	if !ok || (binary.Op != "=" && binary.Op != "!=" && binary.Op != "<>") {
		return nil
	}
	call, callOK := binary.Left.(*FuncCall)
	literal, literalOK := binary.Right.(*Literal)
	if !callOK || !literalOK {
		call, callOK = binary.Right.(*FuncCall)
		literal, literalOK = binary.Left.(*Literal)
	}
	if !callOK || !literalOK || literal.Parameter || len(call.Args) != 1 ||
		call.Over != nil || call.Filter != nil || (call.Name != "LOWER" && call.Name != "UPPER") {
		return nil
	}
	want, ok := literal.Val.(string)
	if !ok {
		return nil
	}
	value := compileRawValue(columns, call.Args[0])
	if value == nil {
		return nil
	}
	upper := call.Name == "UPPER"
	negate := binary.Op != "="
	return func(raw []any) (bool, error) {
		v, err := value(raw)
		if err != nil || v == nil {
			return false, err
		}
		return equalConvertedCase(valueText(v), want, upper) != negate, nil
	}
}

func equalConvertedCase(text, want string, upper bool) bool {
	from, to := byte('A'), byte('a')
	if upper {
		from, to = to, from
	}
	for i := 0; i < min(len(text), len(want)); i++ {
		c := text[i]
		if c >= utf8.RuneSelf || want[i] >= utf8.RuneSelf {
			// Conversion can change byte widths and replace invalid UTF-8.
			// Keep the SQL scalar's exact Unicode behavior for the suffix.
			if upper {
				return strings.ToUpper(text[i:]) == want[i:]
			}
			return strings.ToLower(text[i:]) == want[i:]
		}
		if c-from < 26 {
			c = c - from + to
		}
		if c != want[i] {
			return false
		}
	}
	return len(text) == len(want)
}
