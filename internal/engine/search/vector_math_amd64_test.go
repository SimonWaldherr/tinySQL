//go:build amd64

package search

import "testing"

func TestAMD64FeatureDispatch(t *testing.T) {
	if vectorUseAVX2 != detectAVX2FMA() {
		t.Fatalf("init AVX2/FMA selection=%t, feature probe=%t", vectorUseAVX2, detectAVX2FMA())
	}
	if got, want := VectorMathBackend, vectorAMD64Backend(vectorUseAVX2); got != want {
		t.Fatalf("backend=%q, want %q", got, want)
	}
	for useAVX2, want := range map[bool]string{true: "amd64-avx2-fma", false: "amd64-sse2"} {
		if got := vectorAMD64Backend(useAVX2); got != want {
			t.Errorf("useAVX2=%t: backend=%q, want %q", useAVX2, got, want)
		}
	}
}
