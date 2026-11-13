package search

import (
	"math"
	"math/rand"
	"testing"
)

type vectorL2PrefixKernel struct {
	name string
	fn   func([]float64, []float64) float64
}

// A 64-coordinate prefix ends on every current kernel's accumulator groups:
// NEON/AVX2 use 16 lanes, SSE2 uses 8, and the portable loop uses 4. Extending
// these nonnegative accumulators and applying the same reduction cannot lower
// the result. Engine pruning relies on this floating-point property, not only
// on the corresponding real-arithmetic inequality.
func TestVectorL2AlignedPrefixIsLowerBound(t *testing.T) {
	kernels := []vectorL2PrefixKernel{{"dispatch", VectorL2Squared}, {"portable", VectorL2SquaredUnrolled}}
	kernels = append(kernels, vectorL2PrefixNativeKernels()...)
	for _, kernel := range kernels {
		t.Run(kernel.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(712))
			for _, dims := range []int{256, 257, 263, 264, 271, 272, 383, 384, 385, 511, 512, 513, 768, 769, 1536} {
				for _, scale := range []float64{0, 1e-155, 1e-80, 1, 1e80, 1e150, 1e155} {
					for sample := 0; sample < 8; sample++ {
						a, b := make([]float64, dims), make([]float64, dims)
						for d := range a {
							a[d], b[d] = rng.NormFloat64()*scale, rng.NormFloat64()*scale
							if sample&1 == 0 {
								b[d] = math.Nextafter(a[d], math.Inf(1))
							}
							// With an all-zero tail, even a one-ulp change in
							// reduction grouping can invalidate the lower bound.
							if d >= 64 && sample&2 == 0 {
								b[d] = a[d]
							}
						}
						prefix, full := kernel.fn(a[:64], b[:64]), kernel.fn(a, b)
						if prefix > full || math.IsNaN(prefix) && !math.IsNaN(full) {
							t.Fatalf("dims=%d scale=%g sample=%d: prefix=%g exceeds full=%g", dims, scale, sample, prefix, full)
						}
					}
				}
			}
		})
	}
}
