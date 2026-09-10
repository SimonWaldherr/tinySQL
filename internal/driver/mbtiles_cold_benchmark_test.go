package driver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// BenchmarkPagedTilesColdOpen includes opening a fresh pager and the first two
// SQL point reads. The OS file cache is not flushed; this is application-cold.
func BenchmarkPagedTilesColdOpen(b *testing.B) {
	for _, tc := range []struct{ rows, blob int }{{256, 16384}, {4096, 16384}, {64, 1 << 20}} {
		rows, blob := tc.rows, tc.blob
		b.Run(fmt.Sprintf("rows%d_blob%d", rows, blob), func(b *testing.B) {
			dir := filepath.Join(b.TempDir(), "tiles")
			buildMBTilesLikeIndexArtifact(b, dir, rows, blob, storage.ModePagedIndex)
			var pageReads int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db, err := sql.Open("tinysql", "file:"+dir+"?mode=paged_index&read_only=1&max_memory_bytes=64KiB")
				if err != nil {
					b.Fatal(err)
				}
				n := i % rows
				var id string
				err = db.QueryRow(`SELECT tile_id FROM map WHERE zoom_level=? AND tile_column=? AND tile_row=?`, 12, n, 0).Scan(&id)
				if err != nil {
					b.Fatal(err)
				}
				var payload []byte
				err = db.QueryRow(`SELECT tile_data FROM images WHERE tile_id=?`, id).Scan(&payload)
				if err != nil {
					b.Fatal(err)
				}
				if len(payload) != blob || payload[0] != byte(n) || payload[len(payload)-1] != byte(n>>8) {
					b.Fatal("payload mismatch")
				}
				c, err := db.Conn(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				err = c.Raw(func(raw any) error {
					stats := raw.(*conn).srv.db.BackendStats()
					if stats.LoadCount != 0 || stats.CachedPages > stats.MaxCachePages || stats.TransientFrames != 0 {
						return fmt.Errorf("unbounded lookup: %+v", stats)
					}
					pageReads += stats.PageReads
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				c.Close()
				if err = db.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(pageReads)/float64(b.N), "pages/open")
		})
	}
}

// BenchmarkPagedTilesRandomReads measures prepared two-seek serving with a small
// cache under sequential and parallel load, including tail latency samples.
func BenchmarkPagedTilesRandomReads(b *testing.B) {
	const rows, blob = 1024, 16384
	dir := filepath.Join(b.TempDir(), "tiles")
	buildMBTilesLikeIndexArtifact(b, dir, rows, blob, storage.ModePagedIndex)
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("parallel%v", parallel), func(b *testing.B) {
			db, err := sql.Open("tinysql", "file:"+dir+"?mode=paged_index&read_only=1&max_memory_bytes=64KiB&pool_readers=8")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(8)
			mapStmt, err := db.Prepare(`SELECT tile_id FROM map WHERE zoom_level=? AND tile_column=? AND tile_row=?`)
			if err != nil {
				b.Fatal(err)
			}
			defer mapStmt.Close()
			imageStmt, err := db.Prepare(`SELECT tile_data FROM images WHERE tile_id=?`)
			if err != nil {
				b.Fatal(err)
			}
			defer imageStmt.Close()
			samples := make([]int64, b.N)
			var sequence atomic.Int64
			read := func() bool {
				i := int(sequence.Add(1) - 1)
				// Odd multiplicative permutation visits every row before repeating.
				n := (i*733 + 97) % rows
				started := time.Now()
				var id string
				if err := mapStmt.QueryRow(12, n, 0).Scan(&id); err != nil {
					b.Error(err)
					return false
				}
				var payload []byte
				if err := imageStmt.QueryRow(id).Scan(&payload); err != nil {
					b.Error(err)
					return false
				}
				samples[i] = time.Since(started).Nanoseconds()
				if len(payload) != blob || payload[0] != byte(n) || payload[len(payload)-1] != byte(n>>8) {
					b.Error("tile parity failed")
					return false
				}
				return true
			}
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if !read() {
							return
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					if !read() {
						break
					}
				}
			}
			b.StopTimer()
			slices.Sort(samples)
			if len(samples) > 0 {
				b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "p95-ns")
				b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "p99-ns")
			}
			c, err := db.Conn(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			if err = c.Raw(func(raw any) error {
				s := raw.(*conn).srv.db.BackendStats()
				if s.LoadCount != 0 || s.CachedPages > s.MaxCachePages || s.TransientFrames != 0 {
					return fmt.Errorf("unbounded serving: %+v", s)
				}
				b.ReportMetric(float64(s.PageReads)/float64(b.N), "pages/op")
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		})
	}
}
