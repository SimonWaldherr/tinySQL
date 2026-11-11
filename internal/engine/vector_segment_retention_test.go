package engine

import (
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecColumnCompactionReleasesSupersededSegments(t *testing.T) {
	table := storage.NewTable("compaction", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = [][]any{{[]float64{1, 2}}, {nil}, {[]float64{3}}, {[]float64{4, 5, 6}}}
	// Sizes 2,1,1 trigger two merges, with both discarded headers needing
	// clearing. Each old segment owns packed data and pointer-bearing headers.
	segments := []vecColumnSegment{
		buildVecColumnSegment(table.Rows[:2], 0, true, 0),
		buildVecColumnSegment(table.Rows[2:3], 0, true, 2),
		buildVecColumnSegment(table.Rows[3:], 0, true, 3),
	}
	backing := segments
	merged := mergeVecColumnSegments(segments, true)
	if len(merged) != 1 || merged[0].start != 0 || len(merged[0].vectors) != 4 {
		t.Fatalf("compacted segments = %#v", merged)
	}
	for i, segment := range backing[len(merged):] {
		if segment.start != 0 || segment.data != nil || segment.vectors != nil || segment.norms != nil || segment.valid != nil {
			t.Fatalf("discarded segment %d retains packed arrays", i+len(merged))
		}
	}
	for i, want := range [][]float64{{1, 2}, nil, {3}, {4, 5, 6}} {
		if !slices.Equal(merged[0].vectors[i], want) || merged[0].valid[i] != (want != nil) {
			t.Fatalf("row %d changed during compaction", i)
		}
		if want != nil && merged[0].norms[i] != vectorL2Norm(want) {
			t.Fatalf("row %d norm changed during compaction", i)
		}
	}
}
