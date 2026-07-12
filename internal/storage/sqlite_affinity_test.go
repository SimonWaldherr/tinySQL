package storage

import (
	"path/filepath"
	"testing"
)

func TestSQLiteAffinitySchemaPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "affinity.gob")
	db := NewDB()
	table := NewTable("places", []Column{
		{Name: "name", Type: TextType, DeclaredType: "VARCHAR(255)", Affinity: AffinityText},
		{Name: "rank", Type: DecimalType, DeclaredType: "NUMERIC(12,2)", Affinity: AffinityNumeric},
		{Name: "metadata", Type: InterfaceType, DeclaredType: "ANY", Affinity: AffinityBlob},
	}, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("put table: %v", err)
	}
	if err := SaveToFile(db, path); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer loaded.Close()
	got, err := loaded.Get("default", "places")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if got.Cols[0].DeclaredType != "VARCHAR(255)" || got.Cols[0].Affinity != AffinityText {
		t.Fatalf("text schema metadata = %#v", got.Cols[0])
	}
	if got.Cols[1].DeclaredType != "NUMERIC(12,2)" || got.Cols[1].Affinity != AffinityNumeric {
		t.Fatalf("numeric schema metadata = %#v", got.Cols[1])
	}
	if got.Cols[2].DeclaredType != "ANY" || got.Cols[2].Affinity != AffinityBlob {
		t.Fatalf("any schema metadata = %#v", got.Cols[2])
	}
}
