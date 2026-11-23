package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecL2FlatScanMatchesGeneric(t *testing.T) {
	for _, dims := range []int{8, 64, 768, 769, 1536} {
		t.Run(fmt.Sprint(dims), func(t *testing.T) {
			query := make([]float64, dims)
			for i := range query {
				query[i] = float64(i%13 - 6)
			}
			rows := make([][]any, 120)
			for i := range rows {
				vector := slices.Clone(query)
				vector[i%dims] += float64(i % 9)
				if i >= 24 && i%3 == 0 {
					vector[0] += 32
				}
				rows[i] = []any{vector}
			}
			rows[0] = []any{nil}
			rows[1] = []any{[]float64{1, 2}}
			rows[2][0].([]float64)[0] = math.Inf(1)
			rows[3][0].([]float64)[dims-1] = math.NaN()
			rows[43] = []any{slices.Clone(query)}
			cache := segmentScanCache(rows, []int{len(rows)}, false)
			for _, k := range []int{1, 5, 20, 64, len(rows) + 1} {
				got, err := vecSearchFlat(t.Context(), rows, "l2", query, 0, k, cache)
				want, refErr := vecSearchTopK(t.Context(), rows, len(query), k, cache, buildVecDistanceFunc("l2", query, 0), false)
				if err != nil || refErr != nil || !slices.EqualFunc(got, want, func(a, b vecScoredRow) bool {
					return a.rowIdx == b.rowIdx && math.Float64bits(a.distance) == math.Float64bits(b.distance)
				}) {
					t.Fatalf("k=%d: got %v/%v, want %v/%v", k, got, err, want, refErr)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := vecSearchFlat(ctx, rows, "l2", query, 0, 5, cache); err != context.Canceled {
				t.Fatalf("got %v, want context cancellation", err)
			}
		})
	}
}

func BenchmarkVecSearchL2Wide(b *testing.B) {
	const count, k = 4096, 20
	for _, dims := range []int{768, 1536} {
		for _, clustered := range []bool{false, true} {
			b.Run(fmt.Sprintf("dims=%d/clustered=%t", dims, clustered), func(b *testing.B) {
				db := storage.NewDB()
				b.Cleanup(func() { _ = db.Close() })
				table := storage.NewTable("l2_wide", []storage.Column{
					{Name: "v", Type: storage.VectorType},
				}, false)
				b.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
				rng := rand.New(rand.NewSource(91))
				for range count {
					vector := make([]float64, dims)
					group := rng.Intn(8)
					for d := range vector {
						vector[d] = rng.Float64() - 0.5
						if clustered && d < 64 {
							vector[d] += float64(group)
						}
					}
					table.Rows = append(table.Rows, []any{vector})
				}
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
				env := ExecEnv{ctx: b.Context(), db: db, tenant: "default"}
				args := []Expr{
					&Literal{Val: table.Name}, &Literal{Val: "v"}, &Literal{Val: make([]float64, dims)},
					&Literal{Val: k}, &Literal{Val: "l2"}, &Literal{Val: "flat"},
				}
				fn := &VecSearchTableFunc{}
				if _, err := fn.Execute(b.Context(), args, env, nil); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					result, err := fn.Execute(b.Context(), args, env, nil)
					if err != nil || len(result.Rows) != k {
						b.Fatalf("unexpected search result: %v", err)
					}
				}
			})
		}
	}
}
