package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecColumnCacheBoundsPackedSegments(t *testing.T) {
	// Two equally long batches whose packed vectors cannot fit together.
	vector := make([]float64, vecColumnSegmentMaxFloats/2+1)
	vector[0], vector[len(vector)-1] = math.Copysign(0, -1), math.Nextafter(3, 4)
	batch := [][]any{{vector}, {"[3,4]"}, {nil}, {[]float64{}}, {"invalid"}}
	table := storage.NewTable("bounded_cache", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	table.Rows = slices.Clone(batch)
	base := buildVecColumnCache(table, 0, true)
	table.Rows = append(table.Rows, batch...)
	table.Version++
	appended := extendVecColumnCache(base, table, 0, true)
	if len(appended.segments) != 2 || &appended.segments[0].data[0] != &base.segments[0].data[0] {
		t.Fatal("append merged or replaced the existing packed segment")
	}
	cold := buildVecColumnCache(table, 0, true)
	for _, cache := range []vecSearchColumnCacheEntry{appended, cold} {
		vcCheckEntry(t, cache, len(table.Rows), "bounded")
		if len(cache.segments) != 2 {
			t.Fatalf("cache has %d segments, want 2", len(cache.segments))
		}
		for _, segment := range cache.segments {
			if len(segment.data) > vecColumnSegmentMaxFloats || len(segment.vectors) > vecColumnSegmentMaxRows {
				t.Fatalf("segment has %d floats and %d rows", len(segment.data), len(segment.vectors))
			}
		}
		for _, offset := range []int{0, len(batch)} {
			got, norm, valid := cache.resolveRow(offset, true)
			if !valid || !slices.Equal(got, vector) || math.Float64bits(got[0]) != math.Float64bits(vector[0]) || norm != vectorL2Norm(vector) {
				t.Fatalf("native vector at row %d changed", offset)
			}
			if got, norm, valid := cache.resolveRow(offset+1, true); !valid || !slices.Equal(got, []float64{3, 4}) || norm != 5 {
				t.Fatalf("legacy vector at row %d changed", offset+1)
			}
			if cache.validAt(offset+2) || !cache.validAt(offset+3) || cache.validAt(offset+4) {
				t.Fatalf("missing, empty or invalid vector changed at batch %d", offset)
			}
		}
	}
}

func TestVecColumnCacheBoundsNullRowMetadata(t *testing.T) {
	table := storage.NewTable("bounded_nulls", nil, false)
	table.Rows = make([][]any, vecColumnSegmentMaxRows+1)
	cache := buildVecColumnCache(table, 0, false)
	vcCheckEntry(t, cache, len(table.Rows), "null rows")
	if len(cache.segments) != 2 || len(cache.segments[0].vectors) != vecColumnSegmentMaxRows || len(cache.segments[1].vectors) != 1 {
		t.Fatalf("unexpected segments for %d null rows", len(table.Rows))
	}
}

func TestVecColumnCacheFollowReclaimsOnlyAffectedSegments(t *testing.T) {
	const first, total = 8, 21
	rows := make([][]any, total)
	for i := range rows {
		rows[i] = []any{[]float64{float64(i), 2}}
	}
	before := segmentScanCache(rows, []int{first, total}, true)
	for _, tc := range []struct {
		name     string
		dead     []int
		segments int
	}{
		{"full segment", []int{0, 1, 2, 3, 4, 5, 6, 7}, 1},
		{"partial segment", []int{0, 2, 4, 6}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := storage.NewTable("follow_budget", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
			for i, row := range rows {
				if !slices.Contains(tc.dead, i) {
					table.Rows = append(table.Rows, row)
				}
			}
			changes := storage.TableChanges{Dead: tc.dead, Survivors: len(table.Rows)}
			followed, ok := followVecColumnCache(before, table, 0, true, changes)
			if !ok || len(followed.segments) != tc.segments {
				t.Fatalf("removal followed=%t segments=%d, want %d", ok, len(followed.segments), tc.segments)
			}
			kept := followed.segments[len(followed.segments)-1]
			if &kept.data[0] != &before.segments[1].data[0] || &kept.vectors[0] != &before.segments[1].vectors[0] {
				t.Fatal("removal copied an untouched segment")
			}
			for _, segment := range followed.segments {
				if segment.garbage != 0 {
					t.Fatal("garbage retained after segment removal or compaction")
				}
			}
			vcCheckEntry(t, followed, len(table.Rows), tc.name)
			vcCompareEntries(t, followed, buildVecColumnCache(table, 0, true), tc.name)
		})
	}
	if before.segments[0].garbage != 0 || len(before.segments[0].vectors) != first || !slices.Equal(before.vector(0), []float64{0, 2}) {
		t.Fatal("following modified the previous cache snapshot")
	}
}
