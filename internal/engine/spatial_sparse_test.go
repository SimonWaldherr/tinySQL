package engine

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

func sparseTestGrid(duplicates bool) *geoGridIndex {
	idx := &geoGridIndex{cellSizeLon: 1, cellSizeLat: 1, bounds: geoEditBBox{MinX: 0, MinY: 0, MaxX: 127, MaxY: 127, Set: true}, cells: make(map[geoCellID][]int32), valid: make([]bool, 129), uniqueCells: !duplicates}
	for i := int32(0); i < 128; i++ {
		idx.cells[geoCellID{i, i}] = []int32{i}
		idx.valid[i] = true
		if duplicates {
			idx.cells[geoCellID{i, (i + 1) % 128}] = []int32{i}
		}
	}
	idx.valid[128] = true
	idx.overflow = []int32{128}
	return idx
}

func TestSparseGridCandidates(t *testing.T) {
	for _, duplicates := range []bool{false, true} {
		idx := sparseTestGrid(duplicates)
		rng := rand.New(rand.NewSource(42))
		for trial := 0; trial < 500; trial++ {
			x1, x2 := rng.Float64()*127, rng.Float64()*127
			y1, y2 := rng.Float64()*127, rng.Float64()*127
			minX, maxX := int32(math.Floor(min(x1, x2))), int32(math.Floor(max(x1, x2)))
			minY, maxY := int32(math.Floor(min(y1, y2))), int32(math.Floor(max(y1, y2)))
			want := []int32{128}
			seen := map[int32]bool{128: true}
			for cell, rows := range idx.cells {
				if cell.X < minX || cell.X > maxX || cell.Y < minY || cell.Y > maxY {
					continue
				}
				for _, row := range rows {
					if !seen[row] {
						want = append(want, row)
						seen[row] = true
					}
				}
			}
			got := idx.candidatesBBox(x1, y1, x2, y2)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("duplicates=%v trial=%d got %v want %v", duplicates, trial, got, want)
			}
		}
	}
}

func BenchmarkSparseGridCandidates(b *testing.B) {
	idx := sparseTestGrid(false)
	for _, tc := range []struct {
		name string
		end  float64
	}{{"broad", 126}, {"selective", 1}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(idx.candidatesBBox(0, 0, tc.end, tc.end)) == 0 {
					b.Fatal("missing candidates")
				}
			}
		})
	}
}
