package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// A cold build of the flat vector column cache over a RAG-sized embedding column.
// Every iteration starts from a collected heap (the build allocates the whole
// array, about 600 MB at 100k rows, so what a run inherits from the previous one
// would otherwise dominate the figure); compare serial and parallel with
// -count=5 and benchstat rather than a single run.
func BenchmarkBuildVecColumnSegment(b *testing.B) {
	const dims = 768
	run := func(b *testing.B, table [][]any, norms bool, build func([][]any, int, bool, int) vecColumnSegment) {
		b.SetBytes(int64(len(table)) * dims * 8)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			runtime.GC()
			b.StartTimer()
			_ = build(table, 1, norms, 0)
		}
	}
	rng := rand.New(rand.NewSource(1))
	floats := make([][]any, 100_000)
	for i := range floats {
		v := make([]float64, dims)
		for j := range v {
			v[j] = rng.Float64()
		}
		floats[i] = []any{i, v}
	}
	for _, norms := range []bool{false, true} {
		name := fmt.Sprintf("rows=%d/dims=%d/norms=%t", len(floats), dims, norms)
		b.Run(name+"/serial", func(b *testing.B) { run(b, floats, norms, buildVecColumnSegmentSerial) })
		b.Run(name+"/parallel", func(b *testing.B) { run(b, floats, norms, buildVecColumnSegment) })
	}
	// Vectors stored as JSON text must be parsed first, which is where the
	// parallel validity pass pays off most.
	texts := make([][]any, 20_000)
	for i := range texts {
		parts := make([]string, dims)
		for j := range parts {
			parts[j] = strconv.FormatFloat(rng.Float64(), 'f', 6, 64)
		}
		texts[i] = []any{i, "[" + strings.Join(parts, ",") + "]"}
	}
	name := fmt.Sprintf("text/rows=%d/dims=%d/norms=true", len(texts), dims)
	b.Run(name+"/serial", func(b *testing.B) { run(b, texts, true, buildVecColumnSegmentSerial) })
	b.Run(name+"/parallel", func(b *testing.B) { run(b, texts, true, buildVecColumnSegment) })
}
