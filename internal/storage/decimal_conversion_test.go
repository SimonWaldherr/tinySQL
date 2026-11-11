package storage

import (
	"math"
	"math/big"
	"testing"
)

func TestDecimalConversionRejectsMissingAndNonfiniteNumbers(t *testing.T) {
	for _, value := range []any{(*big.Rat)(nil), math.NaN(), math.Inf(1), math.Inf(-1)} {
		if converted, ok := DecimalFromAny(value); ok || converted != nil {
			t.Fatalf("%T(%v): returned an unusable decimal %v / %v", value, value, converted, ok)
		}
		if _, err := DecimalAdd(big.NewRat(1, 2), value); err == nil {
			t.Fatalf("adding %T(%v) must fail", value, value)
		}
	}
	if converted, ok := AsBigRat((*big.Rat)(nil)); ok || converted != nil {
		t.Fatal("typed nil was accepted as a usable rational")
	}
	for _, value := range []any{0.0, -0.5, math.SmallestNonzeroFloat64, math.MaxFloat64} {
		converted, ok := DecimalFromAny(value)
		if !ok || converted == nil || converted.Cmp(new(big.Rat).SetFloat64(value.(float64))) != 0 {
			t.Fatalf("finite conversion changed: %v / %v", converted, ok)
		}
	}
}
