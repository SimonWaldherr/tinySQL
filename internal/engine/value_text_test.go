// valueText is a fmt-free replacement for fmt.Sprintf("%v", v) on the scalar
// types SQL values take, used by every scalar function that coerces an
// argument to text. Its entire contract is byte-for-byte equality with fmt,
// so it is pinned here differentially rather than by expected-value tables:
// a divergence would silently change query results (FTS tokens, REPLACE
// output, CAST(x AS TEXT), LIKE subjects) rather than fail loudly.
package engine

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestValueTextMatchesSprintf(t *testing.T) {
	// float64 is the case ftsWriteValue deliberately left on the fmt path,
	// warning that %v's shortest-'g' rules are hard to reproduce with
	// strconv. The exhaustive float list below is what justifies valueText
	// taking that fast path anyway: exponent-notation thresholds in both
	// directions, shortest-round-trip boundaries, subnormals, negative zero,
	// the non-finite values, and integral floats (which must not grow a
	// ".0" that fmt does not print).
	values := []any{
		// strings, including ones that look like other types
		"", "abc", "123", "true", "NaN", "  padded  ", "ünïcödé", "a\x00b",
		// int / int64 boundaries
		0, 1, -1, 42, math.MaxInt64, math.MinInt64,
		int64(0), int64(-1), int64(math.MaxInt64), int64(math.MinInt64),
		// bool
		true, false,
		// float64: integral values must print without a decimal point
		0.0, 1.0, -1.0, 100000.0, 1000000.0, 123456789.0,
		// exponent thresholds (%g switches notation at 21 digits / 1e-5)
		1e20, 1e21, 1e22, -1e21, 1e-4, 1e-5, 1e-6, 1e100, 1e-100,
		// shortest-round-trip cases
		0.1, 0.3, 1.0 / 3.0, 2.5, 1.5e-8, 3.141592653589793,
		0.000001234, 9007199254740993.0,
		// subnormal, smallest/largest normal, negative zero
		math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64,
		math.Copysign(0, -1),
		// non-finite
		math.NaN(), math.Inf(1), math.Inf(-1),
		// types with no fast case: must fall back to fmt, not be mangled
		[]byte("hi"), []byte(nil), []float64{1, 2.5}, nil,
		int32(7), uint64(9), 1.5, struct{ A int }{3}, map[string]int{"k": 1},
	}

	for _, v := range values {
		want := fmt.Sprintf("%v", v)
		if got := valueText(v); got != want {
			t.Errorf("valueText(%#v) = %q, fmt %q", v, got, want)
		}
	}
}

// TestValueTextMatchesSprintfFloatSweep widens the float64 check well past
// hand-picked values: every bit pattern reachable by scaling and by walking
// mantissa bits, so a mismatch anywhere in the shortest-'g' formatting rules
// surfaces here rather than in a user's FTS index.
func TestValueTextMatchesSprintfFloatSweep(t *testing.T) {
	check := func(f float64) {
		want := fmt.Sprintf("%v", f)
		if got := valueText(f); got != want {
			t.Fatalf("valueText(%b) = %q, fmt %q", f, got, want)
		}
	}
	for exp := -320; exp <= 308; exp++ {
		for _, mant := range []float64{1, 1.5, 2.7182818284590452, 3.3333333333333333, 9.9999999999999999} {
			f := mant * math.Pow(10, float64(exp))
			check(f)
			check(-f)
		}
	}
	// Walk consecutive representable values around a few anchors: adjacent
	// float64s differ only in their last digits, exactly where a shortest
	// representation can go wrong.
	for _, anchor := range []float64{1, 1e6, 1e-6, 1e21, 1e-5, 255.0} {
		f := anchor
		for i := 0; i < 2000; i++ {
			check(f)
			f = math.Nextafter(f, math.Inf(1))
		}
		f = anchor
		for i := 0; i < 2000; i++ {
			check(f)
			f = math.Nextafter(f, math.Inf(-1))
		}
	}
	// Integers as floats, across the range where float64 is exact.
	for i := int64(0); i < 5000; i++ {
		check(float64(i))
		check(float64(-i))
		check(float64(i) * 1e15)
	}
}

// TestFTSValueToStringMatchesValueText guards the consolidation: FTS builds
// its indexed text through ftsWriteValue, so if the two coercions ever
// disagreed, the same column value would tokenize differently depending on
// which code path reached it.
func TestFTSValueToStringMatchesValueText(t *testing.T) {
	values := []any{
		"abc", 42, int64(-7), true, false, 1.5, 1e21, 1e-5, 0.1,
		math.NaN(), math.Inf(-1), math.Copysign(0, -1), []byte("hi"), nil,
	}
	for _, v := range values {
		if got, want := ftsValueToString(v), valueText(v); got != want {
			t.Errorf("ftsValueToString(%#v) = %q, valueText %q", v, got, want)
		}
		var sb strings.Builder
		ftsWriteValue(&sb, v)
		if got, want := sb.String(), valueText(v); got != want {
			t.Errorf("ftsWriteValue(%#v) = %q, valueText %q", v, got, want)
		}
	}
}
