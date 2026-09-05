//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

func TestCLIArtifactLifecycle(t *testing.T) {
	temp := t.TempDir()
	source := filepath.Join(temp, "source.mbtiles")
	if err := createFlatMBTilesFixture(source); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(temp, "region.ttiles")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"import", "--batch", "2", "--min-free", "0", source, artifact}, &stdout, &stderr); code != 0 {
		t.Fatalf("import code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "preflight tiles=12") || !strings.Contains(stdout.String(), "published tiles=12") {
		t.Fatalf("import progress missing from stdout=%q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", artifact}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "valid schema=flat") {
		t.Fatalf("validate code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"inspect", artifact}, &stdout, &stderr); code != 0 {
		t.Fatalf("inspect code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var manifest tiles.ArtifactInfo
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		t.Fatalf("inspect JSON: %v\n%s", err, stdout.String())
	}
	if manifest.Schema != tiles.SchemaFlat || len(manifest.Tables) != 2 {
		t.Fatalf("inspect manifest schema=%q tables=%d", manifest.Schema, len(manifest.Tables))
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"tile", artifact, "8", "2", "17"}, &stdout, &stderr); code != 0 || !bytes.Equal(stdout.Bytes(), []byte{1, 2, 17}) {
		t.Fatalf("tile code=%d data=%x stderr=%q", code, stdout.Bytes(), stderr.String())
	}
	tileFile := filepath.Join(temp, "tile.pbf")
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"tile", "-out", tileFile, artifact, "8", "2", "17"}, &stdout, &stderr); code != 0 {
		t.Fatalf("tile -out code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	data, err := os.ReadFile(tileFile)
	if err != nil || !bytes.Equal(data, []byte{1, 2, 17}) {
		t.Fatalf("tile output=%x err=%v", data, err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"tile", artifact, "8", "200", "200"}, &stdout, &stderr); code != 3 {
		t.Fatalf("missing tile code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	// Exercise close/reopen behaviour through the public reader API as a CLI
	// artifact is expected to survive independent serving process lifecycles.
	for cycle := 0; cycle < 3; cycle++ {
		reader, err := tiles.OpenArtifact(context.Background(), artifact, tiles.OpenOptions{MaxMemoryBytes: 8 << 20})
		if err != nil {
			t.Fatalf("open reader cycle %d: %v", cycle, err)
		}
		tile, found, err := reader.Lookup(context.Background(), tiles.Key{Z: 8, X: 2, Y: 17})
		closeErr := reader.Close()
		if err != nil || closeErr != nil || !found || !bytes.Equal(tile.Data, []byte{1, 2, 17}) {
			t.Fatalf("reader cycle %d found=%t data=%x lookup=%v close=%v", cycle, found, tile.Data, err, closeErr)
		}
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"benchmark", "--source", source, "--artifact", artifact, "--requests", "10"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "full-parity\ttiles=") || !strings.Contains(stdout.String(), "gate\tp95 <= 2x SQLite\tPASS") {
		t.Fatalf("benchmark code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	// Full parity includes metadata, not only the sampled tile corpus.
	sourceDB, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceDB.Exec(`UPDATE metadata SET value='changed after import' WHERE name='name'`); err != nil {
		_ = sourceDB.Close()
		t.Fatal(err)
	}
	if err := sourceDB.Close(); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"benchmark", "--source", source, "--artifact", artifact, "--requests", "10"}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "full parity") {
		t.Fatalf("changed source benchmark code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCommandValidationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"benchmark", "--requests", "9"}, &stdout, &stderr); code != 2 {
		t.Fatalf("invalid benchmark code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"tile", "dataset.ttiles", "8", "x", "1"}, &stdout, &stderr); code != 2 {
		t.Fatalf("invalid tile coordinate code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCommandBenchmarkSupportsNormalizedSource(t *testing.T) {
	temp := t.TempDir()
	source := filepath.Join(temp, "normalized.mbtiles")
	if err := createNormalizedMBTilesFixture(source); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(temp, "normalized.ttiles")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"import", "--min-free", "0", source, artifact}, &stdout, &stderr); code != 0 {
		t.Fatalf("import code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"benchmark", "--source", source, "--artifact", artifact, "--requests", "10"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "full-parity\ttiles=") || !strings.Contains(stdout.String(), "gate\tp95 <= 2x SQLite\tPASS") {
		t.Fatalf("benchmark code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func createFlatMBTilesFixture(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata(name, value) VALUES ('format', 'pbf'), ('name', 'flat fixture');
	`); err != nil {
		return err
	}
	statement, err := db.Prepare(`INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for x := 1; x <= 12; x++ {
		if _, err := statement.Exec(8, x, x+15, []byte{1, byte(x), byte(x + 15)}); err != nil {
			return err
		}
	}
	return nil
}

func createNormalizedMBTilesFixture(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE map (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_id TEXT);
		CREATE TABLE images (tile_id TEXT PRIMARY KEY, tile_data BLOB);
		INSERT INTO metadata(name, value) VALUES ('format', 'pbf'), ('name', 'normalized fixture');
	`); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	mapInsert, err := tx.Prepare(`INSERT INTO map(zoom_level,tile_column,tile_row,tile_id) VALUES(?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	imageInsert, err := tx.Prepare(`INSERT INTO images(tile_id,tile_data) VALUES(?,?)`)
	if err != nil {
		_ = mapInsert.Close()
		_ = tx.Rollback()
		return err
	}
	for x := 1; x <= 12; x++ {
		id := fmt.Sprintf("tile-%d", x)
		if _, err := imageInsert.Exec(id, []byte{2, byte(x), byte(x + 15)}); err != nil {
			_ = imageInsert.Close()
			_ = mapInsert.Close()
			_ = tx.Rollback()
			return err
		}
		if _, err := mapInsert.Exec(8, x, x+15, id); err != nil {
			_ = imageInsert.Close()
			_ = mapInsert.Close()
			_ = tx.Rollback()
			return err
		}
	}
	if err := imageInsert.Close(); err != nil {
		_ = mapInsert.Close()
		_ = tx.Rollback()
		return err
	}
	if err := mapInsert.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
