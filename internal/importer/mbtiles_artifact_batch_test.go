//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMBTilesArtifactBatchConversions(t *testing.T) {
	for _, normalizedSource := range []bool{false, true} {
		for _, schema := range []MBTilesArtifactSchema{MBTilesSchemaFlat, MBTilesSchemaNormalized} {
			t.Run(fmt.Sprintf("normalized=%t/to=%s", normalizedSource, schema), func(t *testing.T) {
				sourcePath := filepath.Join(t.TempDir(), "source.mbtiles")
				source, err := sql.Open("sqlite", sourcePath)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = source.Close() })
				ddl := `CREATE TABLE metadata(name TEXT,value TEXT); INSERT INTO metadata VALUES('format','pbf');`
				if normalizedSource {
					ddl += `CREATE TABLE map(zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_id TEXT); CREATE TABLE images(tile_id TEXT,tile_data BLOB);`
				} else {
					ddl += `CREATE TABLE tiles(zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_data BLOB);`
				}
				if _, err := source.Exec(ddl); err != nil {
					t.Fatal(err)
				}
				payloads := make([][]byte, 7)
				for i := range payloads {
					id := i
					if normalizedSource {
						id /= 2
					}
					payloads[i] = bytes.Repeat([]byte{byte(id)}, 8193/(id+1))
					if normalizedSource {
						if _, err := source.Exec(`INSERT INTO map VALUES(4,0,?,?)`, i, fmt.Sprint(id)); err != nil {
							t.Fatal(err)
						}
						if i%2 == 0 {
							if _, err := source.Exec(`INSERT INTO images VALUES(?,?)`, fmt.Sprint(id), payloads[i]); err != nil {
								t.Fatal(err)
							}
						}
					} else if _, err := source.Exec(`INSERT INTO tiles VALUES(4,0,?,?)`, i, payloads[i]); err != nil {
						t.Fatal(err)
					}
				}
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(t.TempDir(), "dataset.tinysql")
				if _, err := ImportMBTilesArtifact(context.Background(), sourcePath, target, &MBTilesArtifactOptions{Schema: schema, BatchSize: 3, MaxMemoryBytes: 16 << 20}); err != nil {
					t.Fatal(err)
				}
				reader, err := OpenMBTilesReader(context.Background(), target, 4<<20)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = reader.Close() })
				for i, want := range payloads {
					got, found, err := reader.LookupTile(context.Background(), 4, 0, i)
					if err != nil || !found || !bytes.Equal(got, want) {
						t.Fatalf("tile %d: found=%v bytes=%d, want %d, err=%v", i, found, len(got), len(want), err)
					}
				}
			})
		}
	}
}
