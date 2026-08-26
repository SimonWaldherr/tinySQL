package engine

import (
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSPersistentIndexExtendsAppendOnlyAndRebuildsAfterUpdate(t *testing.T) {
	table := storage.NewTable("fts_persistent_incremental", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = [][]any{{"alpha beta"}, {"beta gamma"}}
	table.Version = 1

	first := getFTSDocCache("fts-persistent-test", table, []int{0})
	index := table.FTSIndexes["0"]
	if index == nil || index.BuiltRows != 2 || index.Version != table.Version {
		t.Fatalf("initial persistent index = %#v, want two indexed rows at version %d", index, table.Version)
	}
	if got := first.docFreq("beta"); got != 2 {
		t.Fatalf("initial beta document frequency = %d, want 2", got)
	}

	table.Rows = append(table.Rows, []any{"delta beta"})
	table.Version++
	table.MarkDirtyFrom(2) // pure append: StructVersion must remain stable
	second := getFTSDocCache("fts-persistent-test", table, []int{0})
	if table.FTSIndexes["0"] != index {
		t.Fatal("append-only refresh replaced the persistent index instead of extending it")
	}
	if index.BuiltRows != 3 || second.docFreq("beta") != 3 || second.docFreq("delta") != 1 {
		t.Fatalf("incremental index did not include appended row: built=%d beta=%d delta=%d",
			index.BuiltRows, second.docFreq("beta"), second.docFreq("delta"))
	}

	table.Rows[0][0] = "epsilon"
	table.Version++
	table.MarkRowUpdated(0)
	third := getFTSDocCache("fts-persistent-test", table, []int{0})
	if table.FTSIndexes["0"] == index {
		t.Fatal("in-place update reused an append-only persistent index")
	}
	if third.docFreq("alpha") != 0 || third.docFreq("epsilon") != 1 {
		t.Fatalf("rebuilt index retained stale terms: alpha=%d epsilon=%d", third.docFreq("alpha"), third.docFreq("epsilon"))
	}
}

func TestFTSLazyBuildIsPersistedByClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	table := storage.NewTable("fts_close_persistence", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = [][]any{{"alpha beta"}}
	table.Version = 1
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	_ = getFTSDocCache("default", table, []int{0})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Get("default", table.Name)
	if err != nil {
		t.Fatal(err)
	}
	index := loaded.FTSIndexes["0"]
	if index == nil || index.BuiltRows != 1 || index.Postings["alpha"][0] != 0 {
		t.Fatalf("lazy FTS build was not persisted by Close: %#v", index)
	}
}

func TestFTSCacheHydratesFromPersistentIndex(t *testing.T) {
	table := storage.NewTable("fts_persistent_hydrate", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = [][]any{{"alpha beta"}}
	table.Version = 7

	_ = getFTSDocCache("fts-hydrate-test", table, []int{0})
	persisted := table.FTSIndexes["0"]
	if persisted == nil {
		t.Fatal("FTS search did not create a persistent index")
	}

	key := ftsDocCacheKey{tenant: "fts-hydrate-test", table: table.Name, cols: "0"}
	ftsDocCacheMu.Lock()
	delete(ftsDocCache, key)
	ftsDocCacheMu.Unlock()

	hydrated := getFTSDocCache("fts-hydrate-test", table, []int{0})
	if hydrated.docFreq("alpha") != 1 || hydrated.docFreq("beta") != 1 {
		t.Fatalf("hydrated cache has wrong postings: alpha=%d beta=%d",
			hydrated.docFreq("alpha"), hydrated.docFreq("beta"))
	}
	if table.FTSIndexes["0"] != persisted {
		t.Fatal("hydration replaced an already-current persistent index")
	}
}
