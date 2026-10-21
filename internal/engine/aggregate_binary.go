package engine

import "strings"

// A single arithmetic operation can feed SUM/AVG without allocating a boxed
// result per input row. Operands are bound to this execution's table; complex
// expressions and values needing other SQL coercions retain the evaluator.
type aggregateBinaryArg struct {
	left, right aggregateBinaryOperand
	op          byte
}

type aggregateBinaryOperand struct {
	column int // -1 for a literal
	value  any
}

func buildAggregateBinaryArg(columns map[string]int, arg Expr) *aggregateBinaryArg {
	binary, ok := arg.(*Binary)
	if !ok || (binary.Op != "+" && binary.Op != "-" && binary.Op != "*") {
		return nil
	}
	left, ok := bindAggregateBinaryOperand(columns, binary.Left)
	if !ok {
		return nil
	}
	right, ok := bindAggregateBinaryOperand(columns, binary.Right)
	if !ok {
		return nil
	}
	return &aggregateBinaryArg{left: left, right: right, op: binary.Op[0]}
}

func bindAggregateBinaryOperand(columns map[string]int, expr Expr) (aggregateBinaryOperand, bool) {
	switch expr := expr.(type) {
	case *Literal:
		return aggregateBinaryOperand{column: -1, value: expr.Val}, true
	case *VarRef:
		key := expr.Lower
		if key == "" {
			key = strings.ToLower(expr.Name)
		}
		column, ok := columns[key]
		return aggregateBinaryOperand{column: column}, ok && column >= 0
	default:
		return aggregateBinaryOperand{}, false
	}
}

func (operand aggregateBinaryOperand) read(raw []any) (any, bool) {
	if operand.column < 0 {
		return operand.value, true
	}
	if operand.column >= len(raw) {
		return nil, false
	}
	return raw[operand.column], true
}

func (arg *aggregateBinaryArg) accumulate(raw []any, state *simpleAggregateState, projection int) bool {
	// Once a decimal promotes this sum, addSum must keep accumulating exactly.
	if state.optional != nil && state.optional.useRat != nil && state.optional.useRat[projection] {
		return false
	}
	left, ok := arg.left.read(raw)
	if !ok {
		return false
	}
	right, ok := arg.right.read(raw)
	if !ok {
		return false
	}
	li, leftInt := integerOperand(left)
	ri, rightInt := integerOperand(right)
	if leftInt && rightInt {
		var value int64
		switch arg.op {
		case '+':
			value, ok = addInt64(li, ri)
		case '-':
			value, ok = subInt64(li, ri)
		case '*':
			value, ok = mulInt64(li, ri)
		}
		if ok {
			state.sums[projection].addInt(value)
			state.counts[projection]++
			return true
		}
		// An overflowing integer operation becomes REAL before accumulation,
		// using the same operand conversions as evalArithmeticBinary.
	}
	lf, leftFloat := left.(float64)
	if leftInt {
		lf, leftFloat = float64(li), true
	}
	rf, rightFloat := right.(float64)
	if rightInt {
		rf, rightFloat = float64(ri), true
	}
	if !leftFloat || !rightFloat {
		return false
	}
	var value float64
	switch arg.op {
	case '+':
		value = lf + rf
	case '-':
		value = lf - rf
	case '*':
		value = lf * rf
	}
	// Round the expression result before adding it, as the boxed evaluator
	// does. An explicit conversion prevents fused multiply-add from changing
	// the result when addFloat is inlined.
	state.sums[projection].addFloat(float64(value))
	state.counts[projection]++
	return true
}
