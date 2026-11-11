package engine

import (
	"math"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecWarmShapeKeepsLegacyClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows [][]any
		want [5]int // rows, valid vectors, chosen dimension, distinct lengths, excluded
	}{
		{"empty", nil, [5]int{}},
		{"invalid", [][]any{nil, {}, {nil}, {"not a vector"}, {[]any{1.0}}}, [5]int{5, 0, 0, 0, 0}},
		{"zero-length", [][]any{{[]float64{}}, {[]float64(nil)}}, [5]int{2, 2, 0, 1, 0}},
		{"mixed", [][]any{nil, {}, {nil}, {[]float64{}}, {[]float64{1, 2}}, {[]float64{3, 4, 5}}, {"[8,9]"}, {"not a vector"}, {[]float64{math.NaN(), 0}}, {[]float64{}}, {[]float64(nil)}}, [5]int{11, 7, 2, 3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := storage.NewTable("warm_shape_"+tc.name, []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
			table.Rows = tc.rows
			t.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
			// A norm-only cache upgrade must preserve the same summary.
			for _, metric := range []string{"l2", "l2", "cosine", "cosine"} {
				rows, vectors, dims, distinct, excluded, err := warmVectorStructures(t.Context(), "default", table, 0, metric, vecIndexFlat)
				got := [5]int{rows, vectors, dims, distinct, excluded}
				if err != nil || got != tc.want {
					t.Fatalf("%s: stats=%v err=%v, want %v", metric, got, err, tc.want)
				}
			}
		})
	}
}

func TestVecWarmShapeTracksVersionsAndPreservesOldViews(t *testing.T) {
	table := storage.NewTable("warm_shape_changes", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = [][]any{{[]float64{}}, {[]float64{1, 2}}, {[]float64{3, 4, 5}}, {nil}}
	t.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
	check := func(want [5]int) {
		t.Helper()
		rows, vectors, dims, distinct, excluded, err := warmVectorStructures(t.Context(), "default", table, 0, "l2", vecIndexFlat)
		if got := [5]int{rows, vectors, dims, distinct, excluded}; err != nil || got != want {
			t.Fatalf("stats=%v err=%v, want %v", got, err, want)
		}
	}
	check([5]int{4, 3, 2, 3, 2})
	old := getVecColumnCache("default", table, 0, false)
	oldShape := old.columnShape()
	table.Rows = append(table.Rows, []any{[]float64{6, 7, 8, 9}})
	table.Version++
	check([5]int{5, 4, 2, 4, 3})
	appended := getVecColumnCache("default", table, 0, false)
	appendShape := appended.columnShape()
	table.Rows[1] = []any{[]float64{}}
	table.MarkRowUpdated(1)
	table.Version++
	check([5]int{5, 4, 3, 3, 3})
	if old.columnShape() != oldShape || appended.columnShape() != appendShape {
		t.Fatal("later UPDATE changed a prior immutable cache summary")
	}
	// Removing the first nonzero vector must choose the next row's length.
	table.Rows = append(table.Rows[:2], table.Rows[3:]...)
	table.MarkDirtyFrom(-1)
	table.Version++
	check([5]int{4, 3, 4, 2, 2})
}

func TestVecWarmShapeConcurrentFirstUse(t *testing.T) {
	table := storage.NewTable("warm_shape_concurrent", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	for i := range 1024 {
		table.Rows = append(table.Rows, []any{make([]float64, 2+i%3)})
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
	cache := getVecColumnCache("default", table, 0, false)
	const readers = 32
	results := make(chan vecColumnShape, readers)
	var group sync.WaitGroup
	for range readers {
		group.Go(func() { results <- cache.columnShape() })
	}
	group.Wait()
	close(results)
	for shape := range results {
		if shape != (vecColumnShape{vectors: 1024, dims: 2, distinctDims: 3, excluded: 682}) {
			t.Fatalf("concurrent summary=%+v", shape)
		}
	}
}
