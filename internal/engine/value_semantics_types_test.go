// Regression tests for a family of missing-type-switch-case bugs found
// while auditing value_semantics.go and coerce.go: several of the engine's
// core scalar helpers (compare, numeric, coerceToFloat/Int/Bool, truthy,
// inferType) special-case int/int64/float64/string/bool but were missing
// *big.Rat (this engine's DECIMAL representation) and/or time.Time (what
// NOW()/DATE_ADD actually return), or — in one case — merged int and int64
// into a single type-switch case in a way that silently broke on int64.
package engine

import (
	"math/big"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestCompareTime pins compare()'s time.Time handling. Before compareTime
// existed, two time.Time values fell through to compare()'s text-equality
// fallback, which is unreliable even for equality (time.Now() carries a
// monotonic reading two separate calls essentially never share) and returns
// an outright error for any two unequal values — silently turned into a
// false "equal" by every generic-path ORDER BY via compareForOrder.
func TestCompareTime(t *testing.T) {
	earlier := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	sameAgain := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	if cmp, err := compare(earlier, later); err != nil || cmp >= 0 {
		t.Fatalf("compare(earlier, later) = %d, %v; want <0, nil", cmp, err)
	}
	if cmp, err := compare(later, earlier); err != nil || cmp <= 0 {
		t.Fatalf("compare(later, earlier) = %d, %v; want >0, nil", cmp, err)
	}
	if cmp, err := compare(earlier, sameAgain); err != nil || cmp != 0 {
		t.Fatalf("compare(earlier, sameAgain) = %d, %v; want 0, nil", cmp, err)
	}
	if _, err := compare(earlier, "not a time"); err == nil {
		t.Fatalf("compare(time.Time, string) should error, got nil")
	}
}

// TestCompareForOrderTimeDoesNotSilentlyTie is the practical failure mode:
// compareForOrder backs every generic-path ORDER BY and discards compare()'s
// error into a false "equal" (0) rather than propagating it.
func TestCompareForOrderTimeDoesNotSilentlyTie(t *testing.T) {
	earlier := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := compareForOrder(earlier, later, false); got >= 0 {
		t.Fatalf("compareForOrder(earlier, later, false) = %d; want <0", got)
	}
	if got := compareForOrder(later, earlier, false); got <= 0 {
		t.Fatalf("compareForOrder(later, earlier, false) = %d; want >0", got)
	}
}

// TestNumericExcludesBigRat pins the opposite of what an earlier version of
// this fix tried: numeric() must keep reporting big.Rat as NOT a plain
// machine number. exec_fastpath_aggregate.go and eval_aggregate.go's
// SUM/AVG/unary +- all check numeric() first and only fall back to
// storage.DecimalFromAny's exact big.Rat accumulation when numeric() says no
// -- making numeric() accept big.Rat silently collapsed every DECIMAL sum to
// a lossy float64 on the very first row of a group, caught by the existing
// TestAggregateFastPathDecimalSum. See numeric()'s doc comment.
func TestNumericExcludesBigRat(t *testing.T) {
	half := big.NewRat(11, 2) // 5.5
	if _, ok := numeric(half); ok {
		t.Fatalf("numeric(*big.Rat) reported numeric=true; must stay false so callers keep their exact-decimal fallback")
	}
	if _, ok := numeric(*half); ok {
		t.Fatalf("numeric(big.Rat) reported numeric=true; must stay false so callers keep their exact-decimal fallback")
	}
}

// TestCoerceToFloatBigRat: CAST(decimal_expr AS FLOAT) must convert
// numerically, not round-trip through a fraction string like "11/2" (which
// strconv.ParseFloat rejects outright).
func TestCoerceToFloatBigRat(t *testing.T) {
	half := big.NewRat(11, 2)
	got, err := coerceToFloat(half)
	if err != nil {
		t.Fatalf("coerceToFloat(*big.Rat) error: %v", err)
	}
	if got != 5.5 {
		t.Fatalf("coerceToFloat(*big.Rat(11/2)) = %v, want 5.5", got)
	}
}

// TestCoerceToIntBigRat: CAST(decimal_expr AS INT).
func TestCoerceToIntBigRat(t *testing.T) {
	seven := big.NewRat(7, 1)
	got, err := coerceToInt(seven)
	if err != nil {
		t.Fatalf("coerceToInt(*big.Rat) error: %v", err)
	}
	if got != 7 {
		t.Fatalf("coerceToInt(*big.Rat(7)) = %v, want 7", got)
	}
}

// TestCoerceToBoolInt64Zero pins the shared-case interface-comparison bug:
// `case int, int64: return x != 0, nil` compared an `any` holding int64(0)
// against the untyped constant 0 (defaulting to int) via interface
// equality, which requires matching dynamic types — int64 vs int are never
// interface-equal regardless of value, so int64(0) != 0 evaluated to true
// and coerceToBool(int64(0)) incorrectly returned true.
func TestCoerceToBoolInt64Zero(t *testing.T) {
	got, err := coerceToBool(int64(0))
	if err != nil {
		t.Fatalf("coerceToBool(int64(0)) error: %v", err)
	}
	if got != false {
		t.Fatalf("coerceToBool(int64(0)) = %v, want false", got)
	}
	got, err = coerceToBool(int64(5))
	if err != nil {
		t.Fatalf("coerceToBool(int64(5)) error: %v", err)
	}
	if got != true {
		t.Fatalf("coerceToBool(int64(5)) = %v, want true", got)
	}
}

// TestCoerceToBoolBigRat: CAST(decimal_expr AS BOOL).
func TestCoerceToBoolBigRat(t *testing.T) {
	zero := big.NewRat(0, 1)
	got, err := coerceToBool(zero)
	if err != nil {
		t.Fatalf("coerceToBool(*big.Rat(0)) error: %v", err)
	}
	if got != false {
		t.Fatalf("coerceToBool(*big.Rat(0)) = %v, want false", got)
	}
	half := big.NewRat(1, 2)
	got, err = coerceToBool(half)
	if err != nil {
		t.Fatalf("coerceToBool(*big.Rat(1/2)) error: %v", err)
	}
	if got != true {
		t.Fatalf("coerceToBool(*big.Rat(1/2)) = %v, want true", got)
	}
}

// TestTruthyInt64 pins truthy()'s missing int64 case: it had no case for
// int64 at all (only int and float64), so any int64 value — including a
// genuinely truthy one — fell to the default `return false`. This backs
// WHERE-clause truthiness (toTri/triAnd/triOr), so `WHERE some_int64_expr`
// used as a bare boolean predicate always evaluated to false.
func TestTruthyInt64(t *testing.T) {
	if truthy(int64(0)) != false {
		t.Errorf("truthy(int64(0)) = true, want false")
	}
	if truthy(int64(5)) != true {
		t.Errorf("truthy(int64(5)) = false, want true (was always false before the fix)")
	}
}

// TestInferTypeBigRatAndTime pins inferType()'s missing cases: it is the
// sole type-inference step for CREATE TABLE AS SELECT / CREATE VIEW
// materialization, and silently mapped both this engine's DECIMAL
// representation and time.Time (from NOW()/DATE_ADD) to JsonType.
func TestInferTypeBigRatAndTime(t *testing.T) {
	half := big.NewRat(1, 2)
	if got := inferType(half); got != storage.DecimalType {
		t.Errorf("inferType(*big.Rat) = %v, want DecimalType", got)
	}
	if got := inferType(time.Now()); got != storage.DateTimeType {
		t.Errorf("inferType(time.Time) = %v, want DateTimeType", got)
	}
}
