package engine

import (
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Time the first column-cache follow after a small deletion. The cached and
// current rows are fixed across iterations so each follow processes one DELETE.
func BenchmarkVecColumnFollowSparseDelete(b *testing.B) {
	const count, dims = 24000, 512
	table := storage.NewTable("benchmark_follow", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	vector := make([]float64, dims)
	vector[0] = 1
	table.Rows = make([][]any, count)
	for i := range table.Rows {
		table.Rows[i] = []any{vector}
	}
	before := buildVecColumnCache(table, 0, true)
	dead := []int{count/2 - 1, count / 2, count/2 + 1}
	table.Rows = slices.Delete(table.Rows, dead[0], dead[len(dead)-1]+1)
	table.Version++
	changes := storage.TableChanges{Dead: dead, Survivors: len(table.Rows)}
	b.ReportAllocs()
	for b.Loop() {
		cache, ok := followVecColumnCache(before, table, 0, true, changes)
		if !ok || cache.rows != changes.Survivors {
			b.Fatal("cache failed to follow the deletion")
		}
	}
}
