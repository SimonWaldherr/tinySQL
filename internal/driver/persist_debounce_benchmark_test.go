package driver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// BenchmarkPersistDebounce compares how many actual durable syncs a burst of
// rapid single-row INSERTs pays for, immediate persist (persist_debounce_ms
// unset — the default, unchanged behavior) vs debounced persist. The two
// sub-benchmarks run the identical workload (one CREATE TABLE plus
// rowsPerIter INSERTs, each its own autocommit statement, exactly the shape
// of a client issuing many single-row INSERTs in a tight loop) against the
// same legacy-autosave-to-file scheme, differing only in persistDebounce.
//
// The metric that matters here is not ns/op — both variants do the same
// SQL work — but actual-syncs/op, i.e. how many times persistNow (and so
// storage.SaveToFile) actually ran per iteration. That count is the whole
// point of Stage 2: a burst of statements against one connection pool
// should collapse into far fewer actual backend syncs than statements
// issued, without changing default (debounce=0) behavior at all.
func BenchmarkPersistDebounce(b *testing.B) {
	const rowsPerIter = 200

	b.Run("Immediate", func(b *testing.B) {
		benchmarkPersistDebounceBurst(b, rowsPerIter, 0)
	})
	b.Run("Debounced_50ms", func(b *testing.B) {
		benchmarkPersistDebounceBurst(b, rowsPerIter, 50*time.Millisecond)
	})
}

func benchmarkPersistDebounceBurst(b *testing.B, rows int, debounce time.Duration) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.gob")
	ctx := context.Background()

	var totalSyncs uint64
	var totalStatements uint64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_ = os.Remove(path)
		s := newServer(storage.NewDB(), cfg{filePath: path, autosave: true, persistDebounce: debounce})
		d := &drv{srv: s}
		rawConn, err := d.Open("")
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		c := rawConn.(*conn)
		if _, err := c.ExecContext(ctx, "CREATE TABLE bench (id INT)", nil); err != nil {
			b.Fatalf("create table: %v", err)
		}
		b.StartTimer()

		for r := 0; r < rows; r++ {
			if _, err := c.ExecContext(ctx, "INSERT INTO bench VALUES ("+strconv.Itoa(r)+")", nil); err != nil {
				b.Fatalf("insert: %v", err)
			}
		}
		// Force any still-pending debounced sync now, exactly what the
		// connector's Close does at shutdown, so the comparison covers the
		// whole burst's eventual sync count, not just whatever happened to
		// fire on its own during the loop above.
		if err := s.flushPersist(); err != nil {
			b.Fatalf("flush: %v", err)
		}

		s.persistMu.Lock()
		totalSyncs += s.persistSyncCount
		s.persistMu.Unlock()
		totalStatements += uint64(rows) + 1 // +1 for the CREATE TABLE
	}
	b.ReportMetric(float64(totalSyncs)/float64(b.N), "actual-syncs/op")
	b.ReportMetric(float64(totalStatements)/float64(b.N), "statements/op")
}
