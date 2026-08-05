package tiles_test

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/tiles"
)

func TestKeyAndRangeValidation(t *testing.T) {
	for _, key := range []tiles.Key{
		{Z: 0, X: 0, Y: 0},
		{Z: 30, X: (1 << 30) - 1, Y: (1 << 30) - 1},
	} {
		if err := key.Validate(); err != nil {
			t.Fatalf("valid key %s: %v", key, err)
		}
	}
	for _, key := range []tiles.Key{{Z: -1}, {Z: 31}, {Z: 2, X: 4}, {Z: 2, Y: 4}} {
		if err := key.Validate(); err == nil {
			t.Fatalf("invalid key accepted: %#v", key)
		}
	}
	if err := (tiles.Range{Z: 2, XMin: 0, XMax: 3, YMin: 1, YMax: 3}).Validate(); err != nil {
		t.Fatalf("valid range: %v", err)
	}
	if err := (tiles.Range{Z: 2, XMin: 3, XMax: 2, YMin: 0, YMax: 0}).Validate(); err == nil {
		t.Fatal("inverted range accepted")
	}
}

func TestArtifactInfoCloneIsDeep(t *testing.T) {
	info := tiles.ArtifactInfo{
		Provenance:      map[string]any{"nested": map[string]any{"name": "source"}},
		PhysicalIndexes: map[string]string{"tiles": "tiles_zxy"},
		Checksums:       map[string]string{"manifest.json": "abc"},
		Tables: []tiles.Table{{
			Name:    "tiles",
			Columns: []string{"z", "x", "y", "tile_data"},
			Indexes: []tiles.Index{{Name: "tiles_zxy", Columns: []string{"z", "x", "y"}, Unique: true}},
		}},
	}
	clone := info.Clone()
	clone.Provenance["nested"].(map[string]any)["name"] = "changed"
	clone.PhysicalIndexes["tiles"] = "changed"
	clone.Checksums["manifest.json"] = "changed"
	clone.Tables[0].Columns[0] = "changed"
	clone.Tables[0].Indexes[0].Columns[0] = "changed"

	if got := info.Provenance["nested"].(map[string]any)["name"]; got != "source" {
		t.Fatalf("provenance aliased: %v", got)
	}
	if info.PhysicalIndexes["tiles"] != "tiles_zxy" || info.Checksums["manifest.json"] != "abc" {
		t.Fatalf("maps aliased: %#v", info)
	}
	if info.Tables[0].Columns[0] != "z" || info.Tables[0].Indexes[0].Columns[0] != "z" {
		t.Fatalf("table slices aliased: %#v", info.Tables[0])
	}
}
