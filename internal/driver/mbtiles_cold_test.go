package driver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The artifact is much larger than the query allocation allowance. This catches
// accidental whole-table decoding even when the retained page cache looks small.
func TestPagedTilesColdReadAllocationBound(t *testing.T) {
	const rows, blobBytes = 1024, 32768
	dir := filepath.Join(t.TempDir(), "tiles")
	buildMBTilesLikeIndexArtifact(t, dir, rows, blobBytes, storage.ModePagedIndex)
	for reopen := 0; reopen < 3; reopen++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		db, err := sql.Open("tinysql", "file:"+dir+"?mode=paged_index&read_only=1&max_memory_bytes=64KiB")
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer db.Close()
			n := rows - 1 - reopen*137
			var id string
			if err := db.QueryRow(`SELECT tile_id FROM map WHERE zoom_level=? AND tile_column=? AND tile_row=?`, 12, n, 0).Scan(&id); err != nil {
				t.Fatal(err)
			}
			var payload []byte
			if err := db.QueryRow(`SELECT tile_data FROM images WHERE tile_id=?`, id).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4<<20 {
				t.Fatalf("cold lookup allocated %d bytes for a %d-byte tile", allocated, blobBytes)
			}
			want := make([]byte, blobBytes)
			want[0], want[blobBytes-1] = byte(n), byte(n>>8)
			if !bytes.Equal(payload, want) {
				t.Fatal("tile parity failed")
			}
			c, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err = c.Raw(func(raw any) error {
				s := raw.(*conn).srv.db.BackendStats()
				if s.LoadCount != 0 || s.PageReads > 32 || s.CachedPages > s.MaxCachePages || s.TransientFrames != 0 {
					return fmt.Errorf("unbounded cold lookup: %+v", s)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT tile_data FROM images WHERE tile_id=?`, "missing").Scan(&payload); err != sql.ErrNoRows {
				t.Fatalf("missing tile: %v", err)
			}
		}()
	}
}

func TestPagedUniqueTileLookupWithResidualAndOffset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tiles")
	buildMBTilesLikeIndexArtifact(t, dir, 8, 64, storage.ModePagedIndex)
	db, err := sql.Open("tinysql", "file:"+dir+"?mode=paged_index&read_only=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const query = `SELECT tile_id FROM map WHERE zoom_level=? AND tile_column=? AND tile_row=? AND tile_id=?`
	const id = "12/5/0"
	for _, tc := range []struct {
		suffix, filter, wantID string
		wantErr                error
	}{
		{" LIMIT 1 OFFSET 0", id, id, nil},
		{" LIMIT 1 OFFSET 1", id, "", sql.ErrNoRows},
		{" LIMIT 0", id, "", sql.ErrNoRows},
		{"", "12/6/0", "", sql.ErrNoRows},
	} {
		var got string
		err := db.QueryRow(query+tc.suffix, 12, 5, 0, tc.filter).Scan(&got)
		if err != tc.wantErr || got != tc.wantID {
			t.Fatalf("suffix %q, filter %q: got %q, err %v", tc.suffix, tc.filter, got, err)
		}
	}
}
