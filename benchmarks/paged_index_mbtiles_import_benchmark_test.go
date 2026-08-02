//go:build sqliteimport && !js && !wasm && !baremetal

package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Benchmarks the ModePagedIndex import fast path (storage.DB.AppendRowsFast,
// wired up in internal/importer.insertTypedRows) against building the same
// artifact the way SaveTable requires: the whole destination table resident
// as one in-memory []Row before it is ever persisted.
//
// This is what "optimize for a continent-scale mbtiles" actually turns on.
// SaveTable "replaces the entire table contents (drop + recreate of the
// tree)" (see its doc comment in internal/storage/pager/backend.go), so
// building a large table by calling it repeatedly costs the size of
// everything imported so far on every call, not just the new batch -- and
// either way, the complete row set has to be resident in memory at the
// moment of that call. AppendRows instead inserts each batch into the
// existing B+Tree in place, so the destination-side memory cost stays
// proportional to one batch, not to how much of the table already exists.
//
// Run, for example:
//
//	go test ./benchmarks -run '^$' -bench BenchmarkPagedIndexMBTilesImport -benchmem -benchtime=1x
func BenchmarkPagedIndexMBTilesImportAppendRows(b *testing.B) {
	benchmarkPagedIndexMBTilesImport(b, true)
}

func BenchmarkPagedIndexMBTilesImportSaveTableRebuild(b *testing.B) {
	benchmarkPagedIndexMBTilesImport(b, false)
}

func benchmarkPagedIndexMBTilesImport(b *testing.B, useAppendRows bool) {
	const rows = 50_000
	const batchSize = 1000

	srcPath := filepath.Join(b.TempDir(), "src.mbtiles")
	buildMBTilesImportSource(b, srcPath, rows)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("dst-%d", i))
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()

		start := time.Now()
		if useAppendRows {
			importMBTilesViaAppendRows(b, dir, srcPath, batchSize)
		} else {
			importMBTilesViaSaveTableRebuild(b, dir, srcPath, batchSize)
		}
		elapsed := time.Since(start)

		b.StopTimer()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/op")
		b.ReportMetric(float64(rows)/elapsed.Seconds(), "rows/s")
		b.ReportMetric(float64(after.HeapAlloc), "heap_after_B")
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "heap_churn_B")
		if rss := benchmarkPeakRSSBytes(); rss > 0 {
			b.ReportMetric(float64(rss), "peak_rss_B")
		}
		b.StartTimer()
	}
}

func buildMBTilesImportSource(tb testing.TB, path string, rows int) {
	tb.Helper()
	src, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatal(err)
	}
	defer src.Close()
	ctx := context.Background()
	if _, err := src.ExecContext(ctx, `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES ('name', 'import-benchmark');`); err != nil {
		tb.Fatal(err)
	}
	tx, err := src.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO tiles VALUES (?, ?, ?, ?)`)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := stmt.ExecContext(ctx, 14, i, 0, mbtilesBenchmarkPayload(i)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := stmt.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func openImportBenchmarkDB(tb testing.TB, dir string) *storage.DB {
	tb.Helper()
	db, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 32 << 20,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func importMBTilesViaAppendRows(tb testing.TB, dir, srcPath string, batchSize int) {
	tb.Helper()
	db := openImportBenchmarkDB(tb, dir)
	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	if err := tiles.CreateSecondaryIndex("tile_index", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		tb.Fatal(err)
	}
	if err := db.Put("default", tiles); err != nil {
		tb.Fatal(err)
	}
	if _, err := importer.ImportMBTiles(context.Background(), db, "default", "tiles", srcPath, &importer.ImportOptions{
		CreateTable: false,
		BatchSize:   batchSize,
	}); err != nil {
		tb.Fatal(err)
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

// importMBTilesViaSaveTableRebuild reproduces the pre-AppendRows behavior for
// comparison: the whole table is assembled in memory (mirroring what
// insertTypedRows did before it could call AppendRowsFast) and handed to
// SaveTable/db.Put once, which frees and rebuilds the tree from that
// complete row set.
func importMBTilesViaSaveTableRebuild(tb testing.TB, dir, srcPath string, batchSize int) {
	tb.Helper()
	db := openImportBenchmarkDB(tb, dir)

	src, err := sql.Open("sqlite", "file:"+srcPath+"?mode=ro")
	if err != nil {
		tb.Fatal(err)
	}
	defer src.Close()
	ctx := context.Background()
	sqlRows, err := src.QueryContext(ctx, `SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles`)
	if err != nil {
		tb.Fatal(err)
	}
	defer sqlRows.Close()

	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	for sqlRows.Next() {
		var zoom, col, row int
		var data []byte
		if err := sqlRows.Scan(&zoom, &col, &row, &data); err != nil {
			tb.Fatal(err)
		}
		tiles.Rows = append(tiles.Rows, []any{zoom, col, row, data})
	}
	if err := sqlRows.Err(); err != nil {
		tb.Fatal(err)
	}
	if err := tiles.CreateSecondaryIndex("tile_index", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		tb.Fatal(err)
	}
	if err := db.Put("default", tiles); err != nil {
		tb.Fatal(err)
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}
