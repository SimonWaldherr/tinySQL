package engine

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"
)

// The text shortcut must agree with the general SQL comparator, including
// its NULL placement and mixed-type ties, for both ordering directions.
func TestTextOrderingMatchesSQLComparison(t *testing.T) {
	values := []any{
		nil, "", "a", "aa", "ab", "b", "A", "\x00", "a\x00", "ä", "日本語", "\xff",
		strings.Repeat("prefix", 100) + "a", strings.Repeat("prefix", 100) + "b",
		0, int64(1), 1.5, math.NaN(), true, []byte("a"), big.NewRat(1, 3),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, desc := range []bool{false, true} {
		for _, left := range values {
			for _, right := range values {
				_, leftText := left.(string)
				_, rightText := right.(string)
				if !leftText && !rightText {
					continue
				}
				want := compareForOrder(left, right, desc)
				if desc {
					want = -want
				}
				if want < 0 {
					want = -1
				} else if want > 0 {
					want = 1
				}
				if got := compareOrderedValue(left, right, desc); got != want {
					t.Fatalf("compareOrderedValue(%#v, %#v, %v) = %d, want %d", left, right, desc, got, want)
				}
			}
		}
	}
}

func BenchmarkOrderTextComparison(b *testing.B) {
	for _, prefix := range []string{"", strings.Repeat("shared-prefix-", 32)} {
		b.Run(fmt.Sprintf("prefix=%d", len(prefix)), func(b *testing.B) {
			values := []any{prefix + "a", prefix + "b", prefix + "c", prefix + "a"}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := compareOrderedValue(values[i%4], values[(i+1)%4], false); got < -1 || got > 1 {
					b.Fatal(got)
				}
			}
		})
	}
}
