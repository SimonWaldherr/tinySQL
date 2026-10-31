package engine

import (
	"fmt"
	"strings"
)

type rawNullOperand struct {
	column  int
	name    string
	literal *Literal
}

// A null test on COALESCE needs no intermediate value. Resolve simple operands
// once, while retaining left-to-right short circuiting and live parameter reads.
func buildRawNullExpressionFilter(columns map[string]int, test *IsNull) func([]any) (bool, error) {
	negate := test.Negate
	if literal, ok := test.Expr.(*Literal); ok {
		return func([]any) (bool, error) { return (literal.Val == nil) != negate, nil }
	}
	call, ok := test.Expr.(*FuncCall)
	if !ok || call.Over != nil || call.Star || call.Distinct {
		return nil
	}
	switch call.Name {
	case "COALESCE", "IFNULL", "NVL":
	default:
		return nil
	}
	operands := make([]rawNullOperand, len(call.Args))
	for i, arg := range call.Args {
		switch value := arg.(type) {
		case *Literal:
			operands[i].literal = value
		case *VarRef:
			key := value.Lower
			if key == "" {
				key = strings.ToLower(value.Name)
			}
			column, ok := columns[key]
			if !ok {
				return nil
			}
			operands[i].column, operands[i].name = column, value.Name
		default:
			return nil
		}
	}
	return func(raw []any) (bool, error) {
		for _, operand := range operands {
			var null bool
			if operand.literal != nil {
				null = operand.literal.Val == nil
			} else {
				if operand.column < 0 || operand.column >= len(raw) {
					return false, fmt.Errorf("column %q is out of range", operand.name)
				}
				null = raw[operand.column] == nil
			}
			if !null {
				return negate, nil
			}
		}
		return !negate, nil
	}
}
