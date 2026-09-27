// Integer-preserving arithmetic shared by every evaluation path.
//
// tinySQL follows SQLite's rules: +, -, * and % on two integers yield an
// integer, and +, - and * fall back to REAL (float64) only when the exact
// result does not fit into 64 bits. A REAL operand makes the result REAL.
// Division deliberately keeps tinySQL's REAL quotient (7 / 2 = 3.5) instead of
// SQLite's truncating integer division; see docs/sql-feature-gaps.md.
package engine

import (
	"errors"
	"math"
	"math/big"
)

// integerOperand reports whether v is an engine integer (int or int64).
func integerOperand(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

// intResult returns r in the engine's canonical integer representation,
// int, which is what parsed literals and INT columns hold. It returns int64
// only where int is narrower than 64 bits and r does not fit.
func intResult(r int64) any {
	if int64(int(r)) == r {
		return int(r)
	}
	return r
}

func addInt64(a, b int64) (int64, bool) {
	r := a + b
	return r, (a^r)&(b^r) >= 0
}

func subInt64(a, b int64) (int64, bool) {
	r := a - b
	return r, (a^b)&(a^r) >= 0
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
		return 0, false
	}
	r := a * b
	return r, r/b == a
}

// integerArithmetic applies op to two integers. handled is false when the
// caller must use REAL arithmetic: for division, and when +, - or *
// overflows.
func integerArithmetic(op string, a, b int64) (result any, handled bool, err error) {
	var (
		r  int64
		ok bool
	)
	switch op {
	case "+":
		r, ok = addInt64(a, b)
	case "-":
		r, ok = subInt64(a, b)
	case "*":
		r, ok = mulInt64(a, b)
	case "%":
		if b == 0 {
			return nil, true, errors.New("division by zero")
		}
		// Go's % truncates toward zero like SQLite and C; MinInt64 % -1 is 0.
		return intResult(a % b), true, nil
	default:
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	return intResult(r), true, nil
}

// unaryNumeric applies unary + or - to an integer or REAL operand. Integer
// negation stays an integer except for the one value whose negation does
// not fit, -(-9223372036854775808), which becomes REAL. ok is false for
// non-numeric operands.
func unaryNumeric(op string, v any) (result any, ok bool) {
	if i, isInt := integerOperand(v); isInt {
		if op != "-" {
			return intResult(i), true
		}
		if i == math.MinInt64 {
			return -float64(i), true
		}
		return intResult(-i), true
	}
	f, isFloat := v.(float64)
	if !isFloat {
		return nil, false
	}
	if op == "-" {
		return -f, true
	}
	return f, true
}

// sumAccumulator implements SUM and AVG over integers and REALs. Integers
// are summed exactly; the first REAL input or an integer overflow switches
// the accumulator to float64 for the rest of the input, in input order. A
// REAL-only input therefore produces exactly the float64 it always did.
type sumAccumulator struct {
	intSum   int64
	floatSum float64
	isFloat  bool
}

func (a *sumAccumulator) addInt(v int64) {
	if a.isFloat {
		a.floatSum += float64(v)
		return
	}
	if sum, ok := addInt64(a.intSum, v); ok {
		a.intSum = sum
		return
	}
	a.isFloat = true
	a.floatSum = float64(a.intSum) + float64(v)
}

func (a *sumAccumulator) addFloat(f float64) {
	if !a.isFloat {
		a.isFloat = true
		a.floatSum = float64(a.intSum)
	}
	a.floatSum += f
}

// add accumulates an int, int64 or float64 and reports whether v was one.
func (a *sumAccumulator) add(v any) bool {
	switch x := v.(type) {
	case int:
		a.addInt(int64(x))
	case int64:
		a.addInt(x)
	case float64:
		a.addFloat(x)
	default:
		return false
	}
	return true
}

// sum returns the SUM result: an integer while every input was an integer
// and the total fits, otherwise float64.
func (a *sumAccumulator) sum() any {
	if a.isFloat {
		return a.floatSum
	}
	return intResult(a.intSum)
}

// average returns AVG for n accumulated values; AVG is always REAL.
func (a *sumAccumulator) average(n int) float64 {
	if a.isFloat {
		return a.floatSum / float64(n)
	}
	return float64(a.intSum) / float64(n)
}

// rat returns the exact accumulated total for promotion to DECIMAL.
func (a *sumAccumulator) rat() *big.Rat {
	if a.isFloat {
		return new(big.Rat).SetFloat64(a.floatSum)
	}
	return new(big.Rat).SetInt64(a.intSum)
}

// ratFromNumeric converts an int, int64 or float64 to an exact rational.
func ratFromNumeric(v any) (*big.Rat, bool) {
	if i, ok := integerOperand(v); ok {
		return new(big.Rat).SetInt64(i), true
	}
	if f, ok := v.(float64); ok {
		return new(big.Rat).SetFloat64(f), true
	}
	return nil, false
}
