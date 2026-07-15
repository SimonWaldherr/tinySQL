package storage

import (
	"path/filepath"
	"testing"
)

func TestTableStatsPersistAcrossSnapshot(t *testing.T) {
	db := NewDB()
	table := NewTable("events", []Column{{Name: "id", Type: IntType}, {Name: "category", Type: TextType}}, false)
	table.Rows = [][]any{{1, "a"}, {2, "b"}, {3, "a"}}
	table.Analyze()
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "stats.gob")
	if err := SaveToFile(db, path); err != nil {
		t.Fatal(err)
	}
	reopened, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := reopened.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	stats := restored.Statistics()
	if stats == nil || stats.Stale || stats.RowCount != 3 || stats.Columns["category"].DistinctCount != 2 {
		t.Fatalf("restored stats = %#v", stats)
	}
}
