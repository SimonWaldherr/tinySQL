package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
)

var vectorNumericResult any

// Keep the former implementation as an allocation control. Argument
// conversion and the accumulation kernel are shared with the current code.
func vectorCentroidSliceControl(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_CENTROID", ex, 2, 256); err != nil {
		return nil, err
	}
	vecs := make([][]float64, 0, len(ex.Args))
	for i, arg := range ex.Args {
		v, err := toVec(env, arg, row)
		if err != nil {
			return nil, fmt.Errorf("VEC_CENTROID arg%d: %w", i+1, err)
		}
		if len(vecs) > 0 && len(v) != len(vecs[0]) {
			return nil, fmt.Errorf("VEC_CENTROID: dimension mismatch at arg%d: %d vs %d", i+1, len(v), len(vecs[0]))
		}
		vecs = append(vecs, v)
	}
	out := make([]float64, len(vecs[0]))
	for _, v := range vecs {
		search.VectorAccumulate(out, v)
	}
	n := float64(len(vecs))
	for i := range out {
		out[i] /= n
	}
	return out, nil
}

func BenchmarkVectorCentroidAccumulation(b *testing.B) {
	for _, dims := range []int{64, 768} {
		for _, count := range []int{8, 64} {
			call := &FuncCall{Name: "VEC_CENTROID", Args: make([]Expr, count)}
			for i := range call.Args {
				v := make([]float64, dims)
				for j := range v {
					v[j] = float64(i+j) / float64(count+dims)
				}
				call.Args[i] = &Literal{Val: v}
			}
			for _, candidate := range []struct {
				name string
				eval func(ExecEnv, *FuncCall, Row) (any, error)
			}{{"control", vectorCentroidSliceControl}, {"direct", evalVecCentroid}} {
				b.Run(fmt.Sprintf("%dx%d/%s", count, dims, candidate.name), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						v, err := candidate.eval(ExecEnv{}, call, nil)
						if err != nil {
							b.Fatal(err)
						}
						vectorNumericResult = v
					}
				})
			}
		}
	}
}
