package engine

import (
	"math/big"
	"math/rand"
	"testing"
)

func exactComparisonRat(t testing.TB, value string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatalf("invalid test rational %q", value)
	}
	return r
}

func TestDecimalComparisonMatchesRat(t *testing.T) {
	values := []*big.Rat{new(big.Rat)}
	for _, text := range []string{
		"0", "1", "-1", "1/3", "-1/3", "2/7", "-2/7",
		"9223372036854775808", "-9223372036854775808",
		"18446744073709551615", "-18446744073709551615",
		"18446744073709551616", "-18446744073709551616",
		"18446744073709551615/18446744073709551614",
		"18446744073709551614/18446744073709551613",
		"-18446744073709551615/18446744073709551614",
		"-18446744073709551614/18446744073709551613",
		"18446744073709551613/18446744073709551615",
		"18446744073709551614/18446744073709551615",
		"1/18446744073709551615", "1/18446744073709551616",
		"-1/18446744073709551615", "-1/18446744073709551616",
		"123456789012345678901234567890/100000000000000000000000000001",
		"123456789012345678901234567891/100000000000000000000000000001",
		"-123456789012345678901234567890/100000000000000000000000000001",
		"123456789012345678901234567890/100000000000000000000000000003",
	} {
		values = append(values, exactComparisonRat(t, text))
	}
	for _, left := range values {
		for _, right := range values {
			checkDecimalComparison(t, left, right)
		}
	}
}

func checkDecimalComparison(t *testing.T, left, right *big.Rat) {
	t.Helper()
	leftBefore, rightBefore := left.RatString(), right.RatString()
	want := left.Cmp(right)
	for _, a := range []any{left, *left} {
		for _, b := range []any{right, *right} {
			got, err := compare(a, b)
			if err != nil || got != want {
				t.Fatalf("compare(%s, %s) = %d, %v; Rat.Cmp = %d", leftBefore, rightBefore, got, err, want)
			}
		}
	}
	if left.RatString() != leftBefore || right.RatString() != rightBefore {
		t.Fatalf("comparison changed operands %s, %s", leftBefore, rightBefore)
	}
}

func TestDecimalComparisonRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(7319))
	for i := 0; i < 10000; i++ {
		var numbers [4]*big.Int
		for j := range numbers {
			numbers[j] = new(big.Int).SetUint64(rng.Uint64())
			// Include both uint64 cross products and arbitrary-precision fallback.
			if i%4 == 0 {
				numbers[j].Lsh(numbers[j], uint(rng.Intn(192)))
			}
			if j%2 == 1 && numbers[j].Sign() == 0 {
				numbers[j].SetInt64(1)
			}
		}
		if rng.Intn(2) != 0 {
			numbers[0].Neg(numbers[0])
		}
		if rng.Intn(2) != 0 {
			numbers[2].Neg(numbers[2])
		}
		left := new(big.Rat).SetFrac(numbers[0], numbers[1])
		right := new(big.Rat).SetFrac(numbers[2], numbers[3])
		checkDecimalComparison(t, left, right)
	}
}

func FuzzDecimalComparisonMatchesRat(f *testing.F) {
	f.Add([]byte{}, []byte{}, []byte{}, []byte{})
	f.Add([]byte{1}, []byte{3}, []byte{2}, []byte{7})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, []byte{2},
		[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, []byte{3})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0}, []byte{7}, []byte{2}, []byte{11})
	f.Fuzz(func(t *testing.T, an, ad, bn, bd []byte) {
		inputs := [4][]byte{an, ad, bn, bd}
		var values [4]*big.Int
		for i, input := range inputs {
			if len(input) > 64 {
				t.Skip()
			}
			values[i] = new(big.Int).SetBytes(input)
			if i%2 == 0 {
				if len(input) > 0 && input[0]&1 != 0 {
					values[i].Neg(values[i])
				}
			} else if values[i].Sign() == 0 {
				values[i].SetInt64(1)
			}
		}
		left := new(big.Rat).SetFrac(values[0], values[1])
		right := new(big.Rat).SetFrac(values[2], values[3])
		checkDecimalComparison(t, left, right)
	})
}

func BenchmarkDecimalComparison(b *testing.B) {
	for _, tc := range []struct{ name, left, right string }{
		{"EqualDenominator", "123456789/100", "123456787/100"},
		{"MixedDenominators", "123456789/100", "123456787/25"},
		{"Uint64CrossProducts", "18446744073709551615/18446744073709551614", "18446744073709551614/18446744073709551613"},
		{"Negative", "-123456789/100", "-123456787/25"},
		{"BigNumerator", "123456789012345678901234567891/100", "123456789012345678901234567897/25"},
		{"BigDenominator", "123456789/18446744073709551617", "123456787/18446744073709551619"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			left, right := exactComparisonRat(b, tc.left), exactComparisonRat(b, tc.right)
			want := left.Cmp(right)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := compare(left, right)
				if err != nil || got != want {
					b.Fatalf("compare = %d, %v; want %d", got, err, want)
				}
			}
		})
	}
}
