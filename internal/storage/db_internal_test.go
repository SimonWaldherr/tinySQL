package storage

import (
	"bytes"
	"testing"
)

func TestSaveAndLoadBytes(t *testing.T) {
	db := NewDB()
	table := NewTable("people", []Column{{Name: "id", Type: IntType}, {Name: "name", Type: TextType}}, false)
	table.Rows = append(table.Rows, []any{1, "Alice"})
	table.Version = 1
	if err := db.Put("tenant", table); err != nil {
		t.Fatalf("failed to insert table: %v", err)
	}

	data, err := SaveToBytes(db)
	if err != nil {
		t.Fatalf("SaveToBytes failed: %v", err)
	}

	loaded, err := LoadFromBytes(data)
	if err != nil {
		t.Fatalf("LoadFromBytes failed: %v", err)
	}
	loadedTable, err := loaded.Get("tenant", "people")
	if err != nil {
		t.Fatalf("loaded table missing: %v", err)
	}
	if len(loadedTable.Rows) != 1 || loadedTable.Rows[0][1] != "Alice" {
		t.Fatalf("unexpected loaded rows: %#v", loadedTable.Rows)
	}

	var buf bytes.Buffer
	if err := SaveToWriter(db, &buf); err != nil {
		t.Fatalf("SaveToWriter failed: %v", err)
	}
	readerDB, err := LoadFromReader(&buf)
	if err != nil {
		t.Fatalf("LoadFromReader failed: %v", err)
	}
	if _, err := readerDB.Get("tenant", "people"); err != nil {
		t.Fatalf("expected table after LoadFromReader: %v", err)
	}
}

func TestCollectWALChanges(t *testing.T) {
	prev := NewDB()
	next := NewDB()

	demoPrev := NewTable("demo", []Column{{Name: "id", Type: IntType}}, false)
	demoPrev.Version = 1
	if err := prev.Put("tenant", demoPrev); err != nil {
		t.Fatalf("failed to put demoPrev: %v", err)
	}

	toDrop := NewTable("ghost", []Column{{Name: "id", Type: IntType}}, false)
	toDrop.Version = 1
	if err := prev.Put("tenant", toDrop); err != nil {
		t.Fatalf("failed to put ghost: %v", err)
	}

	demoNext := NewTable("demo", []Column{{Name: "id", Type: IntType}}, false)
	demoNext.Version = 2
	if err := next.Put("tenant", demoNext); err != nil {
		t.Fatalf("failed to put demoNext: %v", err)
	}

	newTable := NewTable("newbie", []Column{{Name: "id", Type: IntType}}, false)
	newTable.Version = 1
	if err := next.Put("tenant", newTable); err != nil {
		t.Fatalf("failed to put newbie: %v", err)
	}

	changes := CollectWALChanges(prev, next)
	if len(changes) != 3 {
		t.Fatalf("expected 3 WAL changes, got %d: %#v", len(changes), changes)
	}

	found := map[string]bool{}
	for _, ch := range changes {
		key := ch.Name
		if ch.Drop {
			key = "drop:" + key
		}
		found[key] = true
		if !ch.Drop && ch.Table == nil {
			t.Fatalf("expected table data for non-drop change %#v", ch)
		}
	}

	if !found["demo"] {
		t.Fatalf("expected updated demo table change: %#v", changes)
	}
	if !found["newbie"] {
		t.Fatalf("expected new table change: %#v", changes)
	}
	if !found["drop:ghost"] {
		t.Fatalf("expected drop change for ghost: %#v", changes)
	}
}

func TestDeepCloneIndependence(t *testing.T) {
	db := NewDB()
	table := NewTable("items", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = append(table.Rows, []any{1})
	table.Version = 7
	if err := db.Put("tenant", table); err != nil {
		t.Fatalf("failed to put table: %v", err)
	}

	clone := db.DeepClone()
	if clone == db {
		t.Fatal("expected DeepClone to return a distinct DB")
	}

	cloneTable, err := clone.Get("tenant", "items")
	if err != nil {
		t.Fatalf("clone table missing: %v", err)
	}
	cloneTable.Rows[0][0] = 42

	original, err := db.Get("tenant", "items")
	if err != nil {
		t.Fatalf("original table missing: %v", err)
	}
	if original.Rows[0][0] == 42 {
		t.Fatalf("expected DeepClone to copy rows, original mutated: %#v", original.Rows)
	}
	if cloneTable.Version != original.Version {
		t.Fatalf("expected clone version to match original: %d vs %d", cloneTable.Version, original.Version)
	}
}
