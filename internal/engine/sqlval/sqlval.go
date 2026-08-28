// Package sqlval holds the small, self-contained value-semantics helpers that
// both the SQL engine and its retrieval subpackage need.
//
// It exists to break an import cycle. internal/engine imports
// internal/engine/search for the FTS/vector/RAG algorithms, so search must not
// import engine back — yet the algorithms need a handful of leaf helpers
// (stringify a scalar, compare two raw values, coerce to int, check context
// cancellation, bound a cache map) that engine also uses in hundreds of places.
// Duplicating them into search would risk the two copies drifting apart, and
// valueText in particular has behaviour pinned by a test
// (TestValueTextMatchesSprintfFloatSweep), so a silent divergence there would
// change query results rather than fail loudly.
//
// Every function here is a leaf: it depends only on the standard library, never
// on engine types or state. Anything that fans out into engine's coercion,
// planning, or evaluation machinery deliberately does NOT belong here.
//
// engine keeps unexported forwarders (valueText, rawEqual, toInt, checkCtx,
// evictOverCap) so its existing call sites are unchanged and this package stays
// an implementation detail rather than a new vocabulary every caller must learn.
package sqlval

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
)

// ValueText renders a scalar exactly like fmt.Sprintf("%v", v) but without
// entering fmt for the types SQL values actually take. Scalar functions coerce
// arguments to text with this on every call of every row; fmt's reflection walk
// — and, for strings, its needless copy — dominated their profiles. Types
// without a fast case (e.g. []byte, whose %v form is the decimal byte list)
// fall back to fmt to keep output byte-identical.
func ValueText(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case int:
		return strconv.Itoa(s)
	case int64:
		return strconv.FormatInt(s, 10)
	case float64:
		// %v formats float64 as strconv's shortest 'g' form.
		return strconv.FormatFloat(s, 'g', -1, 64)
	case bool:
		if s {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// RawEqual reports whether two raw storage values are equal, treating the
// numeric kinds as mutually comparable (int/int64/float64) so a value read back
// as one kind still matches a literal written as another.
func RawEqual(a, b any) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av == bv
		case int64:
			return int64(av) == bv
		case float64:
			return float64(av) == bv
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av == int64(bv)
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av == float64(bv)
		case int64:
			return av == float64(bv)
		case float64:
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Equal(av, bv)
		}
	}
	return false
}

// ToInt coerces a numeric SQL value to int, erroring on anything else.
func ToInt(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", v)
	}
}

// CheckCtx returns ctx's error if it has been cancelled, and nil otherwise. A
// nil context is treated as never cancelled, so callers with no context to
// thread need no special case.
func CheckCtx(ctx context.Context) error {
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

// EvictOverCap removes arbitrary entries until the map is below the cap, making
// room for one more. Go's random map iteration order makes this a cheap
// pseudo-random eviction policy — deliberately not LRU, since every cache using
// it re-derives an evicted entry lazily on the next miss.
func EvictOverCap[K comparable, V any](m map[K]V, maxEntries int) {
	for k := range m {
		if len(m) < maxEntries {
			return
		}
		delete(m, k)
	}
}
