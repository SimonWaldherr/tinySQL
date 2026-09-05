package offline

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestTileKeyAndRangeValidation(t *testing.T) {
	valid := TileKey{Z: 30, X: (1 << 30) - 1, Y: (1 << 30) - 1}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid tile rejected: %v", err)
	}
	for _, key := range []TileKey{{Z: -1}, {Z: 31}, {Z: 2, X: 4, Y: 0}, {Z: 2, X: 0, Y: 4}} {
		if err := key.Validate(); err == nil {
			t.Fatalf("invalid tile accepted: %#v", key)
		}
	}
	tileRange := TileRange{Z: 2, XMin: 1, XMax: 2, YMin: 0, YMax: 1}
	count, err := tileRange.Count()
	if err != nil || count != 4 {
		t.Fatalf("range count=%d err=%v, want 4 nil", count, err)
	}
	var visited []TileKey
	if err := tileRange.Visit(context.Background(), func(key TileKey) error {
		visited = append(visited, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []TileKey{{Z: 2, X: 1, Y: 0}, {Z: 2, X: 1, Y: 1}, {Z: 2, X: 2, Y: 0}, {Z: 2, X: 2, Y: 1}}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("visit order=%#v, want %#v", visited, want)
	}
	if err := tileRange.Visit(context.Background(), nil); err == nil {
		t.Fatal("nil visitor accepted")
	}
}

func TestManifestTileURLAndValidation(t *testing.T) {
	manifest := Manifest{
		FormatVersion:    ProtocolVersion,
		Dataset:          "regional map",
		Revision:         "release/2026 08",
		CoordinateSystem: "tMs",
		TileURLTemplate:  "https://tiles.example.invalid/{revision}/{z}/{x}/{y}?style=base",
	}
	url, err := manifest.TileURL(TileKey{Z: 4, X: 3, Y: 2})
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://tiles.example.invalid/release%2F2026%2008/4/3/2?style=base"; url != want {
		t.Fatalf("tile URL=%q, want %q", url, want)
	}
	for _, invalid := range []Manifest{
		{FormatVersion: ProtocolVersion + 1, Dataset: "demo", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"},
		{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: "r1", CoordinateSystem: "XYZ", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"},
		{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}"},
		{FormatVersion: ProtocolVersion, Dataset: "bad\x00dataset", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"},
	} {
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid manifest accepted: %#v", invalid)
		}
	}
}

func TestTileChecksumAndClone(t *testing.T) {
	tile := checkedTile([]byte("immutable"))
	tile.Checksum = strings.ToUpper(tile.Checksum)
	if err := verifyTile(tile); err != nil {
		t.Fatalf("uppercase checksum rejected: %v", err)
	}
	clone := tile.Clone()
	clone.Data[0] = 'I'
	if string(tile.Data) != "immutable" {
		t.Fatalf("clone mutated source: %q", tile.Data)
	}
	if err := verifyTile(Tile{Data: []byte("immutable"), Checksum: "invalid"}); err == nil {
		t.Fatal("invalid checksum accepted")
	}
}
