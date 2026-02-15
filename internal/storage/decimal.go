package storage

import (
	"fmt"
	"math/big"
)

// DecimalFromAny attempts to convert a value to *big.Rat.
func DecimalFromAny(v any) (*big.Rat, bool) {
	switch t := v.(type) {
	case *big.Rat:
		return t, true
	case big.Rat:
		return &t, true
	case string:
		r := new(big.Rat)
		if _, ok := r.SetString(t); ok {
			return r, true
		}
		return nil, false
	case int:
		r := new(big.Rat).SetInt64(int64(t))
		return r, true
	case int64:
		r := new(big.Rat).SetInt64(t)
		return r, true
	case float64:
		// Convert float64 to rational approximation
		r := new(big.Rat).SetFloat64(t)
		return r, true
	default:
		return nil, false
	}
}

// DecimalAdd returns the sum of two decimal-like values as *big.Rat.
func DecimalAdd(a, b any) (*big.Rat, error) {
	ra, ok := DecimalFromAny(a)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to decimal", a)
	}
	rb, ok := DecimalFromAny(b)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to decimal", b)
	}
	res := new(big.Rat).Add(new(big.Rat).Set(ra), new(big.Rat).Set(rb))
	return res, nil
}

// DecimalToString returns a plain decimal string representation.
func DecimalToString(r *big.Rat) string {
	if r == nil {
		return ""
	}
	return r.RatString()
}

// AsBigRat returns the value as *big.Rat if it is already a rational type
// (either *big.Rat or big.Rat). Unlike DecimalFromAny this does not coerce
// ints/floats/strings and is suitable for preserving existing numeric
// semantics in the engine.
func AsBigRat(v any) (*big.Rat, bool) {
	switch t := v.(type) {
	case *big.Rat:
		return t, true
	case big.Rat:
		return &t, true
	default:
		return nil, false
	}
}
