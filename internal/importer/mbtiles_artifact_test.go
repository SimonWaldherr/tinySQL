//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

func TestMBTilesArtifactFlatRoundTripAndRejectsCorruption(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE metadata (name TEXT, value TEXT);
CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO metadata VALUES ('name','fixture'),('format','pbf'),('minzoom','1'),('maxzoom','2')`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tile := range []struct {
		z, x, y int
		data    []byte
	}{
		{1, 0, 0, []byte{0, 1, 2}}, {1, 1, 1, []byte{3, 4}}, {2, 2, 1, bytes.Repeat([]byte{7}, 8193)},
	} {
		if _, err := db.Exec(`INSERT INTO tiles VALUES (?,?,?,?)`, tile.z, tile.x, tile.y, tile.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	artifact := filepath.Join(t.TempDir(), "dataset.tinysql")
	result, err := ImportMBTilesArtifact(context.Background(), source, artifact, &MBTilesArtifactOptions{Schema: MBTilesSchemaNormalized, BatchSize: 2, MaxMemoryBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.Schema != MBTilesSchemaNormalized || result.Manifest.Tables[1].Rows != 3 {
		t.Fatalf("unexpected manifest: %#v", result.Manifest)
	}
	if _, err := os.Stat(filepath.Join(artifact, "COMPLETE")); err != nil {
		t.Fatalf("completion marker: %v", err)
	}
	opened, manifest, err := OpenMBTilesArtifact(context.Background(), artifact, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != MBTilesSchemaNormalized {
		t.Fatalf("opened schema %q", manifest.Schema)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenMBTilesReader(context.Background(), artifact, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, found, err := reader.LookupTile(context.Background(), 1, 1, 1)
	if err != nil || !found || !bytes.Equal(data, []byte{3, 4}) {
		t.Fatalf("point lookup: found=%v data=%v err=%v", found, data, err)
	}
	var ranged int
	if err := reader.ScanTileRange(context.Background(), 1, 0, 1, 0, 1, func(_, _, _ int, _ []byte) bool { ranged++; return true }); err != nil {
		t.Fatal(err)
	}
	if ranged != 2 {
		t.Fatalf("spatial lookup returned %d tiles", ranged)
	}

	data, err = os.ReadFile(filepath.Join(artifact, "database", "tinysql.pages"))
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(artifact, "database", "tinysql.pages"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateMBTilesArtifact(context.Background(), artifact); err == nil {
		t.Fatal("corrupted artifact validated")
	}
}

func TestMBTilesArtifactResourceFailureLeavesNoTarget(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB); INSERT INTO tiles VALUES (0,0,0,zeroblob(4096));`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	target := filepath.Join(t.TempDir(), "dataset.tinysql")
	if _, err := ImportMBTilesArtifact(context.Background(), source, target, &MBTilesArtifactOptions{BatchSize: 1, MaxMemoryBytes: 512}); err == nil {
		t.Fatal("resource failure unexpectedly succeeded")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("failed import left target: %v", err)
	}
}

func TestMBTilesArtifactParallelReadersAcrossRestarts(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE metadata (name TEXT, value TEXT); CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB); INSERT INTO metadata VALUES ('name','parallel');`); err != nil {
		t.Fatal(err)
	}
	for z := 0; z <= 3; z++ {
		limit := 1 << z
		for x := 0; x < limit; x++ {
			for y := 0; y < limit; y++ {
				if _, err := db.Exec(`INSERT INTO tiles VALUES (?,?,?,?)`, z, x, y, []byte{byte(z), byte(x), byte(y)}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	_ = db.Close()
	artifact := filepath.Join(t.TempDir(), "dataset.tinysql")
	if _, err := ImportMBTilesArtifact(context.Background(), source, artifact, &MBTilesArtifactOptions{Schema: MBTilesSchemaFlat, BatchSize: 3, MaxMemoryBytes: 16 << 20}); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 3; restart++ {
		for _, readers := range []int{1, 4, 8} {
			var wg sync.WaitGroup
			errs := make(chan error, readers)
			for worker := 0; worker < readers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					reader, err := OpenMBTilesReader(context.Background(), artifact, 4<<20)
					if err != nil {
						errs <- err
						return
					}
					defer reader.Close()
					for i := 0; i < 100; i++ {
						z := (worker + i) % 4
						limit := 1 << z
						x, y := (worker+i*3)%limit, (worker+i*5)%limit
						data, found, lookupErr := reader.LookupTile(context.Background(), z, x, y)
						if lookupErr != nil || !found || len(data) != 3 || data[0] != byte(z) || data[1] != byte(x) || data[2] != byte(y) {
							errs <- fmt.Errorf("worker %d point z/x/y=%d/%d/%d found=%v data=%v err=%v", worker, z, x, y, found, data, lookupErr)
							return
						}
					}
					count := 0
					if err := reader.ScanTileRange(context.Background(), 2, 0, 3, 0, 3, func(_, _, _ int, _ []byte) bool { count++; return true }); err != nil || count != 16 {
						errs <- fmt.Errorf("worker %d range count=%d err=%v", worker, count, err)
					}
				}(worker)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
		}
	}
}

func TestMBTilesArtifactNormalizedSourcePreservesSharedImages(t *testing.T) {
	source := filepath.Join(t.TempDir(), "normalized.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE metadata(name TEXT,value TEXT); CREATE TABLE map(zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_id TEXT); CREATE TABLE images(tile_id TEXT,tile_data BLOB); INSERT INTO metadata VALUES('format','pbf'); INSERT INTO images VALUES('shared',X'010203'); INSERT INTO map VALUES(2,0,0,'shared'),(2,1,0,'shared');`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	target := filepath.Join(t.TempDir(), "dataset.tinysql")
	result, err := ImportMBTilesArtifact(context.Background(), source, target, &MBTilesArtifactOptions{Schema: MBTilesSchemaNormalized, BatchSize: 1, MaxMemoryBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.Tables[1].Rows != 2 || result.Manifest.Tables[2].Rows != 1 {
		t.Fatalf("shared image counts not preserved: %#v", result.Manifest.Tables)
	}
	reader, err := OpenMBTilesReader(context.Background(), target, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, x := range []int{0, 1} {
		data, found, err := reader.LookupTile(context.Background(), 2, x, 0)
		if err != nil || !found || !bytes.Equal(data, []byte{1, 2, 3}) {
			t.Fatalf("normalized source lookup x=%d: data=%v found=%v err=%v", x, data, found, err)
		}
	}
}
