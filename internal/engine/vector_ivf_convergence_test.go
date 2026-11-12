package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ivfConvergenceFixture(rows, dims int, settled bool) (*storage.Table, vecSearchColumnCacheEntry) {
	table := storage.NewTable("ivf_convergence", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, rows)
	rng := rand.New(rand.NewSource(73))
	for row := range table.Rows {
		vector := make([]float64, dims)
		if settled {
			// Repeated orthogonal vectors have exact means. Distinct and empty
			// duplicate centroids both remain at their initial coordinates.
			vector[(row*chooseIVFListCount(rows)/rows)%dims] = 1
		} else {
			for d := range vector {
				vector[d] = rng.NormFloat64()
			}
		}
		table.Rows[row] = []any{vector}
	}
	return table, buildVecColumnCache(table, 0, true)
}

func assertIVFConvergenceParity(t *testing.T, table *storage.Table, cache vecSearchColumnCacheEntry, metric string, dims int) {
	t.Helper()
	got, err := buildVecIVFIndex(t.Context(), table, metric, dims, cache)
	if err != nil {
		t.Fatal(err)
	}
	want, err := legacyIVFBuild(t.Context(), table, metric, dims, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.lists, want.lists) || len(got.centroids) != len(want.centroids) {
		t.Fatal("centroid count or stable partition membership changed")
	}
	for c, centroid := range got.centroids {
		for d, value := range centroid {
			if math.Float64bits(value) != math.Float64bits(want.centroids[c][d]) {
				t.Fatalf("centroid %d dimension %d: bits %x, want %x", c, d,
					math.Float64bits(value), math.Float64bits(want.centroids[c][d]))
			}
		}
		if math.Float64bits(got.centroidNorms[c]) != math.Float64bits(want.centroidNorms[c]) {
			t.Fatalf("centroid %d norm changed", c)
		}
		if cap(got.lists[c]) != len(got.lists[c]) {
			t.Fatalf("centroid %d list retains unused capacity", c)
		}
	}
	query := make([]float64, dims)
	for d := range query {
		query[d] = float64(d%3) - 1
	}
	for _, k := range []int{1, 20, cache.rowCount() + 1} {
		rows, err := got.search(t.Context(), query, vectorL2Norm(query), k, cache)
		reference, refErr := want.search(t.Context(), query, vectorL2Norm(query), k, cache)
		if fmt.Sprint(err) != fmt.Sprint(refErr) || !reflect.DeepEqual(rows, reference) {
			t.Fatalf("k%d: search results changed", k)
		}
	}
}

func TestIVFConvergencePreservesTrainingResults(t *testing.T) {
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		for _, settled := range []bool{true, false} {
			for _, shape := range [][2]int{{0, 3}, {1, 3}, {128, 8}, {4096, 64}} {
				t.Run(fmt.Sprintf("%s/settled%t/rows%d/dims%d", metric, settled, shape[0], shape[1]), func(t *testing.T) {
					table, cache := ivfConvergenceFixture(shape[0], shape[1], settled)
					assertIVFConvergenceParity(t, table, cache, metric, shape[1])
				})
			}
		}
	}
}

func TestIVFConvergencePreservesExceptionalValues(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	for _, vectors := range [][][]float64{
		{{negativeZero, 0}, {0, negativeZero}, {negativeZero, negativeZero}, {0, 0}},
		{{1, 0}, {-1, 0}, {0, 1}, {0, -1}, {0, 0}, {1, 0}, {-1, 0}},
		{{math.MaxFloat64, 1}, {math.MaxFloat64, 1}, {-math.MaxFloat64, -1}, {math.SmallestNonzeroFloat64, 0}},
		{{math.NaN(), 1}, {math.Inf(1), 1}, {math.Inf(-1), 1}, {1}, nil, {1, 0}},
	} {
		for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
			t.Run(fmt.Sprintf("%s/%v", metric, vectors), func(t *testing.T) {
				table := storage.NewTable("ivf_exceptional", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
				for range 8 {
					for _, vector := range vectors {
						table.Rows = append(table.Rows, []any{vector})
					}
				}
				cache := buildVecColumnCache(table, 0, true)
				assertIVFConvergenceParity(t, table, cache, metric, 2)
			})
		}
	}
}

// Cancel at an exact cooperative check while keeping Context's stable Done
// channel. This covers a cancellation during scatter after training settles.
type ivfConvergenceCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
	after  int
}

func (ctx *ivfConvergenceCancelContext) Done() <-chan struct{} {
	ctx.checks++
	if ctx.checks == ctx.after {
		ctx.cancel()
	}
	return ctx.Context.Done()
}

func TestIVFConvergenceCancellation(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		table, cache := ivfConvergenceFixture(0, 3, true)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		got, err := buildVecIVFIndex(ctx, table, "l2", 3, cache)
		want, refErr := legacyIVFBuild(ctx, table, "l2", 3, cache)
		if fmt.Sprint(err) != fmt.Sprint(refErr) || !reflect.DeepEqual(got, want) {
			t.Fatalf("empty build changed cancellation behavior: %v, %v", err, refErr)
		}
	})
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		for _, settled := range []bool{true, false} {
			for _, after := range []int{1, 2, 3} {
				t.Run(fmt.Sprintf("%s/settled%t/check%d", metric, settled, after), func(t *testing.T) {
					table, cache := ivfConvergenceFixture(256, 8, settled)
					base, cancel := context.WithCancel(t.Context())
					defer cancel()
					ctx := &ivfConvergenceCancelContext{Context: base, cancel: cancel, after: after}
					idx, err := buildVecIVFIndex(ctx, table, metric, 8, cache)
					if idx != nil || !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation returned %v, %v", idx, err)
					}
				})
			}
		}
	}
}
