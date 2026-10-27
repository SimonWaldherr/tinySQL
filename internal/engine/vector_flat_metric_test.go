package engine

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
)

// vecCheckedDistance is the generic reference for ranking and exclusion.
func vecCheckedDistance(metric string, a, b []float64, normA, normB float64) (float64, bool) {
	dist, ok := search.VectorRankingDistance(metric, a, b, normA, normB)
	if !ok || math.IsNaN(dist) {
		return 0, false
	}
	return dist, true
}

func TestVecDistanceFuncMatchesGeneric(t *testing.T) {
	queries := [][]float64{
		nil, {1, -2}, {0, 0}, {math.MaxFloat64, 1}, {math.SmallestNonzeroFloat64, 0},
		{math.NaN(), 1}, {math.Inf(1), 0}, {math.Copysign(0, -1), 0},
	}
	vectors := [][]float64{
		nil, {3, 4}, {0, 0}, {1}, {math.NaN(), 1},
		{math.Inf(1), -2}, {math.MaxFloat64, -math.MaxFloat64},
		{math.SmallestNonzeroFloat64, 0},
	}
	// Cover SIMD loop boundaries as well as the scalar tail. These vectors
	// also exercise non-finite data in both argument positions.
	for _, n := range []int{8, 16, 64, 65, 768} {
		query := make([]float64, n)
		vec := make([]float64, n)
		for i := range query {
			query[i] = float64(i%11-5) / 8
			vec[i] = float64(i%7-3) / 4
		}
		badQuery := append([]float64(nil), query...)
		badQuery[n/2] = math.NaN()
		badVec := append([]float64(nil), vec...)
		badVec[n/2] = math.Inf(1)
		queries = append(queries, query, badQuery)
		vectors = append(vectors, vec, badVec)
	}
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot", "unknown"} {
		t.Run(metric, func(t *testing.T) {
			for _, query := range queries {
				queryNorm := vectorL2Norm(query)
				distance := buildVecDistanceFunc(metric, query, queryNorm)
				for _, vec := range vectors {
					norm := vectorL2Norm(vec)
					got, gotOK := distance(vec, norm)
					want, wantOK := vecCheckedDistance(metric, vec, query, norm, queryNorm)
					if gotOK != wantOK || math.Float64bits(got) != math.Float64bits(want) {
						t.Errorf("query=%v vec=%v: got (%v,%t), want (%v,%t)", query, vec, got, gotOK, want, wantOK)
					}
				}
			}
		})
	}
}

var flatMetricBenchmarkDistance float64
var flatMetricBenchmarkValid bool

func BenchmarkVecDistanceFunc(b *testing.B) {
	for _, dims := range []int{8, 64, 768} {
		query := make([]float64, dims)
		vec := make([]float64, dims)
		for i := range query {
			query[i] = float64(i+1) / float64(dims)
			vec[i] = float64(i%7-3) / 7
		}
		for _, metric := range []string{"l2", "manhattan", "dot"} {
			distance := buildVecDistanceFunc(metric, query, 0)
			b.Run(fmt.Sprintf("%s/%d", metric, dims), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					flatMetricBenchmarkDistance, flatMetricBenchmarkValid = distance(vec, 0)
				}
			})
		}
	}
}

func BenchmarkVecSearchFlatMetrics(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	f := &VecSearchTableFunc{}
	for _, metric := range []string{"l2", "manhattan", "dot"} {
		b.Run(metric, func(b *testing.B) {
			args := []Expr{
				&Literal{Val: "rag_hybrid"}, &Literal{Val: "embedding"},
				&Literal{Val: query}, &Literal{Val: 20}, &Literal{Val: metric},
				&Literal{Val: "flat"},
			}
			if _, err := f.Execute(context.Background(), args, env, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := f.Execute(context.Background(), args, env, nil)
				if err != nil || len(result.Rows) != 20 {
					b.Fatalf("unexpected result or error: %v", err)
				}
			}
		})
	}
}
