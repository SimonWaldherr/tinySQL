package storage

import (
	"path/filepath"
	"testing"
)

func TestTableAnalyzeKeepsDistinctValuesTypeAware(t *testing.T) {
	table := NewTable("measurements", []Column{{Name: "value", Type: TextType}}, false)
	table.Rows = [][]any{
		{int(7)},
		{int64(7)}, // integer values share the SQL index representation.
		{float64(7)},
		{"7"},
		{[]byte("7")},
		{nil},
	}

	stats := table.Analyze()
	column := stats.Columns["value"]
	if column.DistinctCount != 4 || column.NullCount != 1 {
		t.Fatalf("column stats = %#v, want four typed values and one NULL", column)
	}
	if table.Stats == stats {
		t.Fatal("Analyze returned the stored statistics instead of a defensive copy")
	}
}

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
	// Close releases the WAL handle LoadFromFile attaches; without this the
	// TempDir cleanup fails on Windows (file in use).
	defer reopened.Close()
	restored, err := reopened.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	stats := restored.Statistics()
	if stats == nil || stats.Stale || stats.RowCount != 3 || stats.Columns["category"].DistinctCount != 2 {
		t.Fatalf("restored stats = %#v", stats)
	}
}
