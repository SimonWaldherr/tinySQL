package engine

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func persistentAppendTable(t *testing.T, db *storage.DB, rows int, empty bool) *storage.Table {
	t.Helper()
	table := storage.NewTable("persistent_append", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	for row := range rows {
		var vector any
		if !empty {
			vector = persistentAppendVector(row)
		}
		table.Rows = append(table.Rows, []any{vector})
	}
	table.Version = 1
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
	return table
}

func persistentAppendVector(row int) []float64 {
	return []float64{float64(row + 1), float64(row*7%17 + 1), 1}
}

func persistentAppendIndex(t *testing.T, table *storage.Table) (*vecHNSWIndex, vecSearchColumnCacheEntry) {
	t.Helper()
	cache := getVecColumnCache("default", table, 0, true)
	idx, err := getVecHNSWIndex(context.Background(), "default", table, 0, "cosine", 3, cache)
	if err != nil {
		t.Fatal(err)
	}
	return idx, cache
}

func appendPersistentVector(table *storage.Table) {
	table.Rows = append(table.Rows, []any{persistentAppendVector(len(table.Rows))})
	table.Version++
}

func TestPersistentHNSWAppendSnapshotBatching(t *testing.T) {
	db := storage.NewDB()
	table := persistentAppendTable(t, db, 64, false)
	idx, _ := persistentAppendIndex(t, table)
	key := vecPersistentHNSWKey(0, "cosine")
	old := table.VectorIndexes[key]
	before := clonePersistentVecIndex(old)
	generation, _ := table.VectorIndexesPersistenceState()
	limit := vecPersistentAppendLimit(old.BuiltRows)
	for range limit - 1 {
		appendPersistentVector(table)
		updated, _ := persistentAppendIndex(t, table)
		if updated != idx || len(updated.levels) != len(table.Rows) {
			t.Fatal("runtime graph was rebuilt or left behind the table")
		}
		if table.VectorIndexes[key] != old {
			t.Fatal("small append recopied the persistent graph")
		}
	}
	if got, _ := table.VectorIndexesPersistenceState(); got != generation {
		t.Fatal("deferred publication marked an unchanged snapshot dirty")
	}
	appendPersistentVector(table)
	idx, cache := persistentAppendIndex(t, table)
	stored := table.VectorIndexes[key]
	if stored == old || stored.BuiltRows != len(table.Rows) || stored.Version != table.Version {
		t.Fatal("append threshold did not publish the current graph")
	}
	if got, _ := table.VectorIndexesPersistenceState(); got != generation+1 {
		t.Fatalf("generation = %d, want %d", got, generation+1)
	}
	if !reflect.DeepEqual(old, before) {
		t.Fatal("runtime extension changed the previous immutable snapshot")
	}
	full, err := buildVecHNSWIndex(context.Background(), table, "cosine", 3, cache)
	if err != nil || !reflect.DeepEqual(idx.levels, full.levels) || !reflect.DeepEqual(idx.neighbors, full.neighbors) {
		t.Fatalf("batched publication changed runtime graph topology: %v", err)
	}
}

func TestPersistentHNSWDeferredAppendSnapshotReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "database")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	table := persistentAppendTable(t, db, 64, false)
	_, _ = persistentAppendIndex(t, table)
	for range 7 {
		appendPersistentVector(table)
	}
	idx, cache := persistentAppendIndex(t, table)
	query := persistentAppendVector(len(table.Rows) - 1)
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
	if stored := loaded.VectorIndexes[vecPersistentHNSWKey(0, "cosine")]; stored == nil || stored.BuiltRows != 64 || len(loaded.Rows) != 71 {
		t.Fatal("close/reopen lost the valid older prefix or current rows")
	}
	previous := vecPersistentHNSWHydrateHook
	var hydrated bool
	vecPersistentHNSWHydrateHook = func() { hydrated = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = previous })
	idx, cache = persistentAppendIndex(t, loaded)
	got, err := idx.search(context.Background(), query, vectorL2Norm(query), 8, cache)
	if err != nil || !hydrated || !reflect.DeepEqual(got, want) {
		t.Fatalf("hydrated append prefix changed search results: hydrated=%t, err=%v", hydrated, err)
	}
	rows, vectors, dims, _, excluded, err := warmVectorStructures(context.Background(), "default", loaded, 0, "cosine", "hnsw")
	if err != nil || rows != 71 || vectors != 71 || dims != 3 || excluded != 0 || len(idx.levels) != 71 {
		t.Fatal("warm-up did not describe and prepare the current runtime graph", err)
	}
}

func TestPersistentHNSWAppendPublishesMissingOrIncompatibleSnapshot(t *testing.T) {
	for _, name := range []string{"missing", "format", "structure", "dimensions", "current_partial"} {
		t.Run(name, func(t *testing.T) {
			db := storage.NewDB()
			table := persistentAppendTable(t, db, 64, false)
			_, _ = persistentAppendIndex(t, table)
			key := vecPersistentHNSWKey(0, "cosine")
			old := table.VectorIndexes[key]
			switch name {
			case "missing":
				delete(table.VectorIndexes, key)
			case "format":
				old.Format--
			case "structure":
				old.StructVersion--
			case "dimensions":
				old.Dims++
			case "current_partial":
				old.Version++ // becomes current after the next append
			}
			appendPersistentVector(table)
			_, cache := persistentAppendIndex(t, table)
			stored := table.VectorIndexes[key]
			if stored == old || stored.BuiltRows != 65 || !usablePersistentVecHNSW(stored, table, 0, "cosine", 3, cache) {
				t.Fatal("extension retained a missing or incompatible snapshot")
			}
		})
	}
}

func TestPersistentHNSWAppendPublishesFirstValidEntry(t *testing.T) {
	db := storage.NewDB()
	table := persistentAppendTable(t, db, 64, true)
	_, _ = persistentAppendIndex(t, table)
	key := vecPersistentHNSWKey(0, "cosine")
	old := table.VectorIndexes[key]
	if old.Entry != -1 {
		t.Fatal("empty fixture has an entry")
	}
	appendPersistentVector(table)
	_, cache := persistentAppendIndex(t, table)
	stored := table.VectorIndexes[key]
	if stored == old || stored.Entry != 64 || !usablePersistentVecHNSW(stored, table, 0, "cosine", 3, cache) {
		t.Fatal("first valid vector was left out of the published graph")
	}
}

func TestPersistentHNSWAppendLimit(t *testing.T) {
	for _, tc := range [][2]int{{0, 32}, {64, 32}, {1024, 64}, {16384, 1024}, {10000000, 1024}} {
		if got := vecPersistentAppendLimit(tc[0]); got != tc[1] {
			t.Fatalf("append limit at %d rows = %d, want %d", tc[0], got, tc[1])
		}
	}
}
