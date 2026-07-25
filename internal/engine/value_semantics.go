// SQL value semantics: NULL, three-valued boolean logic, and comparison and
// ordering across the storage types. Everything that decides what "equal",
// "true" and "less than" mean lives here, so those rules can be read in one
// place rather than inferred from their call sites.
package engine

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func getVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }

func getValLower(row Row, lowerName string) (any, bool) {
	v, ok := row[lowerName]
	return v, ok
}

func putVal(row Row, key string, val any) { row[strings.ToLower(key)] = val }

func isNull(v any) bool { return v == nil }

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
	case float64:
		return compareFloat(ax, b)
	case string:
		return compareString(ax, b)
	case bool:
		return compareBool(ax, b)
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
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
