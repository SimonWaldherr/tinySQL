package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecColumnCacheCompactsDenseUpdatesInTouchedSegments(t *testing.T) {
	const middle, tail = vecColumnSegmentMaxRows, 2 * vecColumnSegmentMaxRows
	const tableName = "dense_update_docs"
	table := storage.NewTable(tableName, []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	t.Cleanup(func() { purgeVectorCachesFor("default", tableName) })
	original := []any{[]float64{1, 2}}
	table.Rows = make([][]any, tail+1)
	for row := range table.Rows {
		table.Rows[row] = original
	}
	base := getVecColumnCache("default", table, 0, false)
	if len(base.segments) != 3 {
		t.Fatalf("cold cache has %d segments, want 3", len(base.segments))
	}
	update := func(row int, value any) {
		table.Rows[row] = []any{value}
		table.MarkRowUpdatedCols(row, storage.ColumnBit(0))
	}
	limit := vecColumnOverrideLimit(len(table.Rows))
	update(0, []float64{5, 12})
	update(middle+limit, []float64{8, 15})
	table.Version++
	before := getVecColumnCache("default", table, 0, false)
	if !slices.Equal(before.overrideRows, []int{0, middle + limit}) {
		t.Fatalf("sparse update overrides = %v", before.overrideRows)
	}
	for row := middle; row < middle+limit; row++ {
		update(row, []float64{0, -3})
	}
	update(middle, []float64{math.Copysign(0, -1), math.Nextafter(3, 4)})
	update(middle+1, nil)
	update(middle+2, "[3,4,12]")
	appendedRow := len(table.Rows)
	table.Rows = append(table.Rows, original)
	update(appendedRow, []float64{7, 24})
	table.Version++
	after := getVecColumnCache("default", table, 0, true)
	if after.shape == before.shape || after.version != table.Version || after.structVersion != table.StructVersion() || !after.normsReady {
		t.Fatal("refreshed cache did not publish the current identity and norms")
	}
	if !slices.Equal(after.overrideRows, []int{0}) || len(after.overrides) != 1 {
		t.Fatalf("refreshed cache has %v overrides, want only row 0", after.overrideRows)
	}
	if &after.segments[0].data[0] != &before.segments[0].data[0] ||
		&after.segments[2].data[0] != &before.segments[2].data[0] {
		t.Fatal("refresh repacked an untouched segment")
	}
	if &after.segments[1].data[0] == &before.segments[1].data[0] {
		t.Fatal("refreshed cache aliases the replaced segment")
	}
	if before.normsReady || before.overrides[0].norm != 0 || !slices.Equal(before.vector(middle), []float64{1, 2}) || len(before.overrideRows) != 2 {
		t.Fatal("refresh changed the previous snapshot")
	}
	if math.Float64bits(after.vector(middle)[0]) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal("refresh changed the sign of zero")
	}
	vcCheckEntry(t, after, len(table.Rows), "dense refresh")
	vcCompareEntries(t, after, buildVecColumnCache(table, 0, true), "dense refresh")
}

// Measure refreshing a warm 94 MiB embedding column after a batch UPDATE
// that exceeds the override limit but only touches one packed segment.
func BenchmarkVecColumnRefreshDenseLocalUpdate(b *testing.B) {
	const count, dims = 24000, 512
	table := storage.NewTable("benchmark_dense_update", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	vector := make([]float64, dims)
	vector[0] = 1
	original := []any{vector}
	table.Rows = make([][]any, count)
	for row := range table.Rows {
		table.Rows[row] = original
	}
	before := buildVecColumnCache(table, 0, true)
	updated := slices.Clone(vector)
	updated[0] = 2
	newRow := []any{updated}
	rows := make([]int, vecColumnOverrideLimit(count)+1)
	for i := range rows {
		row := count/2 + i
		rows[i] = row
		table.Rows[row] = newRow
		table.MarkRowUpdatedCols(row, storage.ColumnBit(0))
	}
	table.Version++
	b.ReportAllocs()
	for b.Loop() {
		after := refreshVecColumnCache(before, table, 0, true, rows)
		if after.vector(rows[0])[0] != 2 {
			b.Fatal("refreshed vector is stale")
		}
	}
}
