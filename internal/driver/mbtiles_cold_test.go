package driver

import (
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
			if len(payload) != blobBytes || payload[0] != byte(n) || payload[len(payload)-1] != byte(n>>8) {
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
