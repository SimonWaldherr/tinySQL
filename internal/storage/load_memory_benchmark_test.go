package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func loadBenchmarkTable() *Table {
	table := NewTable("load_rows", []Column{{Name: "id", Type: IntType}, {Name: "label", Type: TextType}}, false)
	table.Rows = make([][]any, 50000)
	for i := range table.Rows {
		table.Rows[i] = []any{i, "database loading and replication fixture"}
	}
	return table
}

func BenchmarkSnapshotLoadReader(b *testing.B) {
	db := NewDB()
	if err := db.Put("default", loadBenchmarkTable()); err != nil {
		b.Fatal(err)
	}
	data, err := SaveToBytes(db)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		loaded, err := LoadFromReader(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		table, _ := loaded.Get("default", "load_rows")
		if len(table.Rows) != 50000 {
			b.Fatal("lost rows")
		}
	}
}

func BenchmarkLegacyReplicationSnapshot(b *testing.B) {
	db, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: filepath.Join(b.TempDir(), "primary")})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Put("default", loadBenchmarkTable()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		data, _, _, err := SnapshotWithWatermark(db, db.AdvancedWAL())
		if err != nil {
			b.Fatal(err)
		}
		if _, err := LoadFromBytes(data); err != nil {
			b.Fatal(err)
		}
	}
}
