package engine

import (
	"context"
	"testing"
)

func TestGeoGridCandidatePathsMatchFullScan(t *testing.T) {
	for _, polygons := range []bool{false, true} {
		table := geoSpeedTable(t, polygons)
		table.Rows = append(table.Rows, table.Rows...)
		// This geometry exceeds the cell fan-out limit and lives only in overflow.
		table.Rows = append(table.Rows, []any{len(table.Rows), `{"type":"Polygon","coordinates":[[[0,0],[20,0],[20,10],[0,10],[0,0]]]}`})
		table.Rows = append(table.Rows, []any{len(table.Rows), nil})
		idx, err := buildGeoGridIndex(context.Background(), table, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(idx.overflow) == 0 {
			t.Fatal("fixture did not exercise overflow")
		}
		if idx.uniqueCells == polygons {
			t.Fatalf("unexpected uniqueCells=%t for polygons=%t", idx.uniqueCells, polygons)
		}
		for _, box := range [][4]float64{{4, 4, 4.01, 4.01}, {2, 2, 18, 8}, {-180, -90, 180, 90}, {50, 50, 60, 60}} {
			candidates := idx.candidatesBBox(box[0], box[1], box[2], box[3])
			seen := make(map[int32]bool)
			for _, row := range candidates {
				if row < 0 || int(row) >= len(idx.valid) || !idx.valid[row] {
					t.Fatalf("invalid candidate %d", row)
				}
				if seen[row] {
					t.Fatalf("duplicate candidate %d for polygons=%t box=%v", row, polygons, box)
				}
				seen[row] = true
			}
			for row, bounds := range idx.bboxes {
				intersects := idx.valid[row] && bounds.MaxX >= box[0] && bounds.MinX <= box[2] && bounds.MaxY >= box[1] && bounds.MinY <= box[3]
				if intersects && !seen[int32(row)] {
					t.Fatalf("missing row %d for polygons=%t box=%v", row, polygons, box)
				}
			}
		}
	}
}
