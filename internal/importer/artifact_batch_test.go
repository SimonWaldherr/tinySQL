//go:build !js && !wasm && !baremetal

package importer

import (
	"testing"
)

func TestArtifactRowBatchOwnershipAndReset(t *testing.T) {
	batch, err := newArtifactRowBatch(2, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{2, 1} {
		for i := range size {
			row := batch.row()
			row[0], row[1], row[2], row[3] = i, "value", []byte{byte(i)}, map[string]any{"id": i}
			if len(row) != 4 || cap(row) != 4 {
				t.Fatalf("row width/capacity = %d/%d", len(row), cap(row))
			}
		}
		if size == 2 {
			// Appending to one exposed row cannot overwrite the next row's cells.
			extended := append(batch.rows[0], "extra")
			if extended[4] != "extra" || batch.rows[1][0] != 1 {
				t.Fatal("adjacent rows share append capacity")
			}
		}
		batch.reset()
		if len(batch.rows) != 0 || cap(batch.rows) != 2 {
			t.Fatal("reset did not preserve reusable row capacity")
		}
		for i, cell := range batch.cells {
			if cell != nil {
				t.Errorf("cell %d retains %v", i, cell)
			}
		}
		for i, row := range batch.rows[:cap(batch.rows)] {
			if row != nil {
				t.Errorf("discarded row %d remains reachable", i)
			}
		}
	}
}

func TestArtifactRowBatchRejectsInvalidSizes(t *testing.T) {
	for _, size := range [][2]int{{0, 4}, {-1, 4}, {1, 0}, {int(^uint(0) >> 1), 4}} {
		if _, err := newArtifactRowBatch(size[0], size[1]); err == nil {
			t.Fatalf("accepted invalid size/width %v", size)
		}
	}
}
