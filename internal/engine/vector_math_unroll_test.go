package engine

import (
	"math/rand"
	"testing"
)

// naiveHamming is the trivial (non-unrolled) reference implementation of
// Hamming distance, used to cross-check vectorHammingUnrolled.
func naiveHamming(a, b []float64) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	count := 0
	for i := 0; i < n; i++ {
		ai := a[i] > 0
		bi := b[i] > 0
		if ai != bi {
			count++
		}
	}
	return count
}

// naiveAccumulate is the trivial (non-unrolled) reference implementation of
// element-wise accumulation, used to cross-check vectorAccumulateUnrolled.
func naiveAccumulate(dst, src []float64) {
	for i := range dst {
		dst[i] += src[i]
	}
}

func TestVecHammingUnrolledMatchesNaive(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 4, 5, 7, 8, 16, 17, 63, 64, 768}

	for _, n := range sizes {
		t.Run(sizeName(n), func(t *testing.T) {
			cases := map[string][2][]float64{
				"all-zero":     {zeros(n), zeros(n)},
				"all-equal":    {pattern(n, 1), pattern(n, 1)},
				"all-different": {zeros(n), pattern(n, 1)},
				"alternating":  {alternating(n), zeros(n)},
				"negative":     {negatives(n), zeros(n)},
				"random":       {randomVec(n, 1), randomVec(n, 2)},
			}
			for name, ab := range cases {
				a, b := ab[0], ab[1]
				want := naiveHamming(a, b)
				got := vectorHammingUnrolled(a, b)
				if got != want {
					t.Errorf("case %s size %d: vectorHammingUnrolled=%d naiveHamming=%d", name, n, got, want)
				}
				// vectorHammingDistance also clamps to min length; verify the
				// public entry point too.
				gotPublic := vectorHammingDistance(a, b)
				if gotPublic != want {
					t.Errorf("case %s size %d: vectorHammingDistance=%d want=%d", name, n, gotPublic, want)
				}
			}
		})
	}
}

func TestVecHammingUnrolledMismatchedLengths(t *testing.T) {
	a := pattern(10, 1)
	b := pattern(6, 1)
	want := naiveHamming(a, b)
	got := vectorHammingDistance(a, b)
	if got != want {
		t.Errorf("mismatched lengths: got=%d want=%d", got, want)
	}
}

func TestVecCentroidAccumulateMatchesNaive(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 4, 5, 7, 8, 16, 17, 63, 64, 768}

	for _, n := range sizes {
		t.Run(sizeName(n), func(t *testing.T) {
			cases := map[string][]float64{
				"zeros":    zeros(n),
				"ones":     pattern(n, 1),
				"negative": negatives(n),
				"random":   randomVec(n, 3),
			}
			for name, src := range cases {
				dstWant := pattern(n, 2)
				dstGot := make([]float64, n)
				copy(dstGot, dstWant)

				naiveAccumulate(dstWant, src)
				vectorAccumulateUnrolled(dstGot, src)

				for i := range dstWant {
					if dstGot[i] != dstWant[i] {
						t.Errorf("case %s size %d idx %d: got=%v want=%v", name, n, i, dstGot[i], dstWant[i])
					}
				}
			}
		})
	}
}

// --- small local helpers for building test vectors ---

func sizeName(n int) string {
	switch n {
	case 0:
		return "n=0"
	default:
		return "n=" + itoa(n)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func zeros(n int) []float64 {
	return make([]float64, n)
}

func pattern(n int, v float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func negatives(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = -float64(i + 1)
	}
	return out
}

func alternating(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		if i%2 == 0 {
			out[i] = 1
		} else {
			out[i] = -1
		}
	}
	return out
}

func randomVec(n int, seed int64) []float64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([]float64, n)
	for i := range out {
		out[i] = rng.NormFloat64()
	}
	return out
}
