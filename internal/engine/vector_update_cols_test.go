package engine

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	vecMaskColLoaded = 0
	vecMaskColVector = 1
)

func vecMaskTable(t *testing.T, db *storage.DB, rows int) *storage.Table {
	t.Helper()
	table := storage.NewTable("vec_mask", []storage.Column{
		{Name: "loaded_at", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	for row := range rows {
		table.Rows = append(table.Rows, []any{1, persistentAppendVector(row)})
	}
	table.Version = 1
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
	return table
}

// touchLoadedAt mimics UPDATE ... SET loaded_at = ... of the given rows.
func touchLoadedAt(table *storage.Table, stamp int, rows ...int) {
	for _, row := range rows {
		table.Rows[row] = append([]any(nil), table.Rows[row]...)
		table.Rows[row][vecMaskColLoaded] = stamp
		table.MarkRowUpdatedCols(row, storage.ColumnMask(vecMaskColLoaded))
	}
	table.Version++
}

// setVector mimics UPDATE ... SET embedding = ... (changed columns known).
func setVector(table *storage.Table, row int, vector []float64, cols uint64) {
	table.Rows[row] = append([]any(nil), table.Rows[row]...)
	table.Rows[row][vecMaskColVector] = vector
	table.MarkRowUpdatedCols(row, cols)
	table.Version++
}

func TestVecColumnCacheSurvivesUpdateOfOtherColumns(t *testing.T) {
	table := vecMaskTable(t, storage.NewDB(), 64)
	first := getVecColumnCache("default", table, vecMaskColVector, true)
	shape := first.columnShape()

	// A touch batch longer than the legacy 4096-delta log.
	for i := 0; i < 5000; i++ {
		touchLoadedAt(table, 2+i, i%64)
	}
	second := getVecColumnCache("default", table, vecMaskColVector, true)
	if &second.segments[0].data[0] != &first.segments[0].data[0] || len(second.segments) != len(first.segments) {
		t.Fatal("a touch rebuilt the vector column")
	}
	if len(second.overrides) != 0 {
		t.Fatalf("a touch created %d overrides", len(second.overrides))
	}
	if second.structVersion != table.StructVersion() || second.version != table.Version {
		t.Fatalf("stamps not moved: cache (%d,%d) table (%d,%d)", second.version, second.structVersion, table.Version, table.StructVersion())
	}
	if second.shape != first.shape || second.columnShape() != shape {
		t.Fatal("the column shape was recomputed although no vector changed")
	}

	// Appended rows after a touch are picked up and the old data stays shared.
	table.Rows = append(table.Rows, []any{1, persistentAppendVector(64)})
	table.Version++
	touchLoadedAt(table, 7, 3)
	third := getVecColumnCache("default", table, vecMaskColVector, true)
	if third.rowCount() != 65 || &third.segments[0].data[0] != &first.segments[0].data[0] {
		t.Fatalf("append after touch: rows=%d, base segment shared=%t", third.rowCount(), &third.segments[0].data[0] == &first.segments[0].data[0])
	}
	if !reflect.DeepEqual(third.vector(64), persistentAppendVector(64)) {
		t.Fatalf("appended vector = %v", third.vector(64))
	}

	// A real change of the vector column (and an unknown column set) is seen.
	changed := []float64{9, 9, 9}
	setVector(table, 5, changed, storage.ColumnMask(vecMaskColVector))
	setVector(table, 6, persistentAppendVector(6), 0) // unknown column set: revisited, same value
	fourth := getVecColumnCache("default", table, vecMaskColVector, true)
	if !reflect.DeepEqual(fourth.vector(5), changed) {
		t.Fatalf("updated vector = %v, want %v", fourth.vector(5), changed)
	}
	if _, ok := fourth.overrides[6]; !ok {
		t.Fatal("a delta without a column set was not revisited")
	}
	if _, ok := fourth.overrides[3]; ok {
		t.Fatal("a loaded_at touch produced an override")
	}
	// And a mixed update of both columns.
	setVector(table, 8, []float64{4, 4, 4}, storage.ColumnMask(vecMaskColLoaded, vecMaskColVector))
	fifth := getVecColumnCache("default", table, vecMaskColVector, true)
	if !reflect.DeepEqual(fifth.vector(8), []float64{4, 4, 4}) {
		t.Fatalf("mixed update vector = %v", fifth.vector(8))
	}
}

func TestVecHNSWSurvivesUpdateOfOtherColumns(t *testing.T) {
	table := vecMaskTable(t, storage.NewDB(), 96)
	index := func() (*vecHNSWIndex, vecSearchColumnCacheEntry) {
		cache := getVecColumnCache("default", table, vecMaskColVector, true)
		idx, err := getVecHNSWIndex(context.Background(), "default", table, vecMaskColVector, "cosine", 3, cache)
		if err != nil {
			t.Fatal(err)
		}
		return idx, cache
	}
	idx, cache := index()
	query := persistentAppendVector(20)
	want, err := idx.search(context.Background(), query, vectorL2Norm(query), 8, cache)
	if err != nil {
		t.Fatal(err)
	}
	levels := append([]int(nil), idx.levels...)
	neighbors := fingerprintVecHNSWIndex(idx)

	for i := 0; i < 5000; i++ {
		touchLoadedAt(table, 2+i, i%96)
	}
	again, cache := index()
	if again != idx {
		t.Fatal("a touch rebuilt the HNSW graph")
	}
	if len(again.deltaRows) != 0 || !reflect.DeepEqual(again.levels, levels) {
		t.Fatalf("a touch changed the graph: deltas=%v", again.deltaRows)
	}
	requireEqualVecHNSWFingerprint(t, fingerprintVecHNSWIndex(again), neighbors)
	if again.structVersion != table.StructVersion() || again.version != table.Version {
		t.Fatalf("stamps not moved: graph (%d,%d) table (%d,%d)", again.version, again.structVersion, table.Version, table.StructVersion())
	}
	got, err := again.search(context.Background(), query, vectorL2Norm(query), 8, cache)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("results after touch differ: %v vs %v (%v)", got, want, err)
	}

	// An append between touches extends the very same graph.
	table.Rows = append(table.Rows, []any{1, persistentAppendVector(96)})
	table.Version++
	touchLoadedAt(table, 5, 1, 2, 3)
	extended, _ := index()
	if extended != idx || len(extended.levels) != 97 {
		t.Fatalf("append after touch: same graph=%t levels=%d", extended == idx, len(extended.levels))
	}

	// A vector update is recorded as a delta; an unknown column set too.
	setVector(table, 10, []float64{3, 1, 2}, storage.ColumnMask(vecMaskColVector))
	setVector(table, 11, persistentAppendVector(11), 0)
	updated, _ := index()
	if updated != idx || !reflect.DeepEqual(updated.deltaRows, []int{10, 11}) {
		t.Fatalf("vector update: same graph=%t deltas=%v, want [10 11]", updated == idx, updated.deltaRows)
	}
	// Touching afterwards keeps the deltas and adds none.
	touchLoadedAt(table, 6, 40, 41)
	// The answers follow the moved rows exactly, also after touches that left the
	// deltas alone: a moved row is found at its new vector (distance 0), never at
	// the vector the graph was built with.
	after, afterCache := index()
	hit := func(query []float64, row int) (float64, bool) {
		t.Helper()
		got, err := after.search(context.Background(), query, vectorL2Norm(query), 6, afterCache)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			if r.rowIdx == row {
				return r.distance, true
			}
		}
		return 0, false
	}
	if d, ok := hit([]float64{3, 1, 2}, 10); !ok || d > 1e-9 {
		t.Fatalf("moved row 10 at its new vector: found=%t distance=%v", ok, d)
	}
	if d, ok := hit(persistentAppendVector(10), 10); ok && d < 1e-9 {
		t.Fatalf("moved row 10 still matches its old vector (distance %v)", d)
	}
	if d, ok := hit(persistentAppendVector(11), 11); !ok || d > 1e-9 {
		t.Fatalf("row 11 (unknown column set, same vector): found=%t distance=%v", ok, d)
	}
	if after != idx || !reflect.DeepEqual(after.deltaRows, []int{10, 11}) {
		t.Fatalf("touch after vector update: same graph=%t deltas=%v", after == idx, after.deltaRows)
	}

	// A DELETE-like invalidation still forces a rebuild.
	table.MarkDirtyFrom(-1)
	table.Version++
	rebuilt, _ := index()
	if rebuilt == idx {
		t.Fatal("a structural change did not rebuild the graph")
	}
}

// The persisted topology must follow the table's structVersion across touches,
// or a reopen would have to throw the graph away and rebuild it.
func TestPersistentHNSWSurvivesTouchAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "database")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	table := vecMaskTable(t, db, 80)
	cache := getVecColumnCache("default", table, vecMaskColVector, true)
	idx, err := getVecHNSWIndex(context.Background(), "default", table, vecMaskColVector, "cosine", 3, cache)
	if err != nil {
		t.Fatal(err)
	}
	key := vecPersistentHNSWKey(vecMaskColVector, "cosine")
	if table.VectorIndexes[key] == nil {
		t.Fatal("graph was not persisted")
	}

	original := table.VectorIndexes[key]
	touchLoadedAt(table, 2, 0, 1, 2, 3, 4, 5, 6, 7)
	cache = getVecColumnCache("default", table, vecMaskColVector, true)
	if _, err = getVecHNSWIndex(context.Background(), "default", table, vecMaskColVector, "cosine", 3, cache); err != nil {
		t.Fatal(err)
	}
	// The stamp moves without copying the topology again.
	restamped := table.VectorIndexes[key]
	if restamped == original || restamped.StructVersion != table.StructVersion() || &restamped.Levels[0] != &original.Levels[0] {
		t.Fatalf("touch did not restamp the stored graph in place of recloning it: same=%t stamp=%d want %d shared=%t",
			restamped == original, restamped.StructVersion, table.StructVersion(), &restamped.Levels[0] == &original.Levels[0])
	}
	if original.StructVersion == table.StructVersion() {
		t.Fatal("the previous immutable snapshot was modified")
	}
	// Also a small append, so the deferred-snapshot rule is exercised.
	table.Rows = append(table.Rows, []any{1, persistentAppendVector(80)})
	table.Version++
	touchLoadedAt(table, 3, 9)
	cache = getVecColumnCache("default", table, vecMaskColVector, true)
	idx, err = getVecHNSWIndex(context.Background(), "default", table, vecMaskColVector, "cosine", 3, cache)
	if err != nil {
		t.Fatal(err)
	}
	stored := table.VectorIndexes[key]
	if stored == nil || stored.StructVersion != table.StructVersion() {
		t.Fatalf("persisted graph stamp %v does not follow the table's structVersion %d", stored, table.StructVersion())
	}
	query := persistentAppendVector(80)
	want, err := idx.search(context.Background(), query, vectorL2Norm(query), 8, cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Get("default", table.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", loaded.Name) })
	previous := vecPersistentHNSWHydrateHook
	var hydrated bool
	vecPersistentHNSWHydrateHook = func() { hydrated = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = previous })
	cache = getVecColumnCache("default", loaded, vecMaskColVector, true)
	idx, err = getVecHNSWIndex(context.Background(), "default", loaded, vecMaskColVector, "cosine", 3, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !hydrated {
		t.Fatal("the reopened table rebuilt the graph instead of hydrating the persisted one")
	}
	got, err := idx.search(context.Background(), query, vectorL2Norm(query), 8, cache)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("results after reopen differ: %v vs %v (%v)", got, want, err)
	}
}
