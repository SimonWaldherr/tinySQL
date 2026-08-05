//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestMBTilesArtifactPreservesChecksummedProvenance(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB); INSERT INTO tiles VALUES (1, 1, 1, X'0102')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	provenance := map[string]any{
		"kind":       "osm-pbf",
		"pbf_inputs": []any{map[string]any{"name": "region.osm.pbf", "bytes": int64(123)}},
		"generator":  map[string]any{"name": "karte-preprocess", "minzoom": 8},
	}
	artifact := filepath.Join(t.TempDir(), "dataset.tinysql")
	result, err := ImportMBTilesArtifact(context.Background(), source, artifact, &MBTilesArtifactOptions{MaxMemoryBytes: 16 << 20, Provenance: provenance})
	if err != nil {
		t.Fatal(err)
	}
	provenance["kind"] = "mutated-after-import"
	provenance["generator"].(map[string]any)["name"] = "mutated-after-import"
	if result.Manifest.Provenance["kind"] != "osm-pbf" {
		t.Fatalf("result provenance was not copied: %#v", result.Manifest.Provenance)
	}
	if generator, ok := result.Manifest.Provenance["generator"].(map[string]any); !ok || generator["name"] != "karte-preprocess" {
		t.Fatalf("nested result provenance was not copied: %#v", result.Manifest.Provenance)
	}
	validated, err := ValidateMBTilesArtifact(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Provenance["kind"] != "osm-pbf" {
		t.Fatalf("validated provenance=%#v", validated.Provenance)
	}
}

func TestMBTilesArtifactRejectsNonJSONProvenanceBeforePublishing(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB); INSERT INTO tiles VALUES (1, 1, 1, X'01')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "dataset.tinysql")
	if _, err := ImportMBTilesArtifact(context.Background(), source, target, &MBTilesArtifactOptions{Provenance: map[string]any{"invalid": func() {}}}); err == nil {
		t.Fatal("non-JSON provenance accepted")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("invalid provenance left target: %v", err)
	}
}
