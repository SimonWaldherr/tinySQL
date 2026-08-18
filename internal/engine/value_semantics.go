// SQL value semantics: NULL, three-valued boolean logic, and comparison and
// ordering across the storage types. Everything that decides what "equal",
// "true" and "less than" mean lives here, so those rules can be read in one
// place rather than inferred from their call sites.
package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func getVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }

func getValLower(row Row, lowerName string) (any, bool) {
	v, ok := row[lowerName]
	return v, ok
}

func putVal(row Row, key string, val any) { row[strings.ToLower(key)] = val }

func isNull(v any) bool { return v == nil }

// numeric reports whether v is a plain machine number (int/int64/float64),
// returning its float64 value when so.
//
// *big.Rat/big.Rat -- this engine's exact-decimal representation --
// deliberately do NOT match here, even though they are numeric in the SQL
// sense. Every caller that checks numeric() first (exec_fastpath_aggregate.go
// SUM/AVG, eval_aggregate.go's SUM/AVG and unary +/-) falls back to
// storage.DecimalFromAny specifically to keep DECIMAL/MONEY accumulation
// exact via big.Rat instead of collapsing it through a lossy float64. Adding
// a big.Rat case here was tried and reverted: it made numeric() report true
// for a DECIMAL value, so every one of those callers took the fast (lossy)
// branch on the very first row of a group -- before their own
// useRat/DecimalFromAny fallback ever got a chance to engage -- turning
// `SELECT SUM(decimal_col)` into a float64 approximation instead of an exact
// big.Rat sum (caught by TestAggregateFastPathDecimalSum). Call
// storage.DecimalFromAny directly wherever a big.Rat needs to be treated as
// numeric; do not extend this function to accept it.
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func coerceToFloat(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case *big.Rat:
		f, _ := x.Float64()
		return f, nil
	case big.Rat:
		f, _ := x.Float64()
		return f, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to FLOAT", x)
		}
		return f, nil
	case bool:
		if x {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to FLOAT", v)
	}
}

func isStringValue(v any) bool {
	_, ok := v.(string)
	return ok
}

func stringifySQLValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

// tri-state
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v any) int {
	if v == nil {
		return tvUnknown
	}
	if truthy(v) {
		return tvTrue
	}
	return tvFalse
}

func triNot(t int) int {
	if t == tvTrue {
		return tvFalse
	}
	if t == tvFalse {
		return tvTrue
	}
	return tvUnknown
}

func triAnd(a, b int) int {
	if a == tvFalse || b == tvFalse {
		return tvFalse
	}
	if a == tvTrue && b == tvTrue {
		return tvTrue
	}
	return tvUnknown
}

func triOr(a, b int) int {
	if a == tvTrue || b == tvTrue {
		return tvTrue
	}
	if a == tvFalse && b == tvFalse {
		return tvFalse
	}
	return tvUnknown
}

func compare(a, b any) (int, error) {
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case *big.Rat:
		return compareBigRat(ax, b)
	case big.Rat:
		return compareBigRat(&ax, b)
	case int:
		return compareInt(ax, b)
	case int64:
		return compareInt64(ax, b)
	case float64:
		return compareFloat(ax, b)
	case string:
		return compareString(ax, b)
	case bool:
		return compareBool(ax, b)
	case []byte:
		return compareBytes(ax, b)
	case time.Time:
		return compareTime(ax, b)
	}
	if valueText(a) == valueText(b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable %T and %T", a, b)
}

func compareBigRat(ax *big.Rat, b any) (int, error) {
	// Try to convert b to big.Rat
	if bx, ok := storage.DecimalFromAny(b); ok {
		rb := new(big.Rat).Set(bx)
		return ax.Cmp(rb), nil
	}
	// If b is numeric (int/float), convert
	switch bx := b.(type) {
	case int:
		rb := new(big.Rat).SetInt64(int64(bx))
		return ax.Cmp(rb), nil
	case int64:
		rb := new(big.Rat).SetInt64(bx)
		return ax.Cmp(rb), nil
	case float64:
		rb := new(big.Rat).SetFloat64(bx)
		return ax.Cmp(rb), nil
	}
	return 0, fmt.Errorf("incomparable decimal and %T", b)
}

func compareInt(ax int, b any) (int, error) {
	// fast path: avoid float64 conversion for same-type comparisons.
	switch bv := b.(type) {
	case int:
		if ax < bv {
			return -1, nil
		}
		if ax > bv {
			return 1, nil
		}
		return 0, nil
	case int64:
		ai := int64(ax)
		if ai < bv {
			return -1, nil
		}
		if ai > bv {
			return 1, nil
		}
		return 0, nil
	}
	if f, ok := numeric(b); ok {
		af := float64(ax)
		if af < f {
			return -1, nil
		}
		if af > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable int and %T", b)
}

// compareInt64 mirrors compareInt for a left-hand int64 value. Without it,
// compare()'s outer type switch had no case for int64 at all (despite
// compareInt itself handling a right-hand int64, and numeric() listing it as
// a recognized numeric kind) — any comparison with an int64 on the left fell
// through to the text-equality-only fallback at the end of compare(), which
// returns an error for any two int64 values that are not textually equal.
// That error is silently swallowed by compareForOrder (used by every
// generic-path ORDER BY), turning into "equal" — a silent wrong sort order,
// not a visible failure. int64 reaches raw rows from direct storage callers
// (see the ORDER BY float fast path's own caveat about this) even though SQL
// INSERT coercion normalizes it to int first.
func compareInt64(ax int64, b any) (int, error) {
	// fast path: avoid float64 conversion for same-type comparisons.
	switch bv := b.(type) {
	case int64:
		if ax < bv {
			return -1, nil
		}
		if ax > bv {
			return 1, nil
		}
		return 0, nil
	case int:
		bi := int64(bv)
		if ax < bi {
			return -1, nil
		}
		if ax > bi {
			return 1, nil
		}
		return 0, nil
	}
	if f, ok := numeric(b); ok {
		af := float64(ax)
		if af < f {
			return -1, nil
		}
		if af > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable int64 and %T", b)
}

func compareFloat(ax float64, b any) (int, error) {
	if f, ok := numeric(b); ok {
		if ax < f {
			return -1, nil
		}
		if ax > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable float64 and %T", b)
}

func compareString(ax string, b any) (int, error) {
	if bs, ok := b.(string); ok {
		if ax < bs {
			return -1, nil
		}
		if ax > bs {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable string and %T", b)
}

func compareBool(ax bool, b any) (int, error) {
	if bb, ok := b.(bool); ok {
		if !ax && bb {
			return -1, nil
		}
		if ax && !bb {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable bool and %T", b)
}

// compareBytes applies SQLite-style bytewise ordering to BLOBs. Equality is a
// particularly important case: BLOB values are slices and therefore cannot be
// compared with Go's == operator or with the raw fast path's scalar cases.
func compareBytes(ax []byte, b any) (int, error) {
	bx, ok := b.([]byte)
	if !ok {
		return 0, fmt.Errorf("incomparable []byte and %T", b)
	}
	return bytes.Compare(ax, bx), nil
}

// compareTime orders two time.Time values chronologically. Without this
// case, compare() had no way to order NOW()/GETDATE()/CURRENT_TIMESTAMP
// (evalNowFunc returns time.Now() directly) or DATE_ADD/DATE_SUB (which
// return the computed time.Time unformatted): both fell to compare()'s
// text-equality-only fallback. That fallback is unreliable here even for
// equality, because time.Now() carries a monotonic-clock reading that two
// separately-taken timestamps essentially never share, so even
// %v-equivalent formatting rarely produced equal strings for the "same"
// instant. `ORDER BY` on such a column silently no-op'd (compareForOrder
// swallows the resulting error into 0), and a WHERE comparison raised
// "incomparable time.Time and time.Time" instead of evaluating true/false.
// Before/Equal/After correctly prefer the monotonic reading when both
// operands have one, matching time.Time's own documented comparison
// semantics.
func compareTime(ax time.Time, b any) (int, error) {
	bx, ok := b.(time.Time)
	if !ok {
		return 0, fmt.Errorf("incomparable time.Time and %T", b)
	}
	switch {
	case ax.Before(bx):
		return -1, nil
	case ax.After(bx):
		return 1, nil
	default:
		return 0, nil
	}
}

func compareForOrder(a, b any, desc bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if desc {
			return -1
		}
		return 1
	}
	if b == nil {
		if desc {
			return 1
		}
		return -1
	}
	c, err := compare(a, b)
	if err != nil {
		return 0
	}
	return c
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
