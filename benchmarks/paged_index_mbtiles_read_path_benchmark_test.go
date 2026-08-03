package benchmarks

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

// This file extends BenchmarkPagedIndexMBTilesAccess (paged_index_mbtiles_benchmark_test.go)
// with the remaining shapes the MBTiles pager fix's performance mandate asks
// for: isolated BLOB size classes (rather than one mixed distribution),
// concurrent readers against a shared artifact, and the one-time cost of
// opening a published artifact, separate from steady-state lookup latency --
// each measured against modernc.org/sqlite on the same request corpus, the
// same way BenchmarkPagedIndexMBTilesAccess already compares warm/cold.

// BenchmarkPagedIndexMBTilesAccessBySize isolates the BLOB size classes named
// in the fixed bug report: a small inline payload, two right at the
// inline/overflow boundary (1,569 is the literal size from "need 1569, have
// 1536 free"; 2,500 is the top of the reported critical band), and a large
// payload that always overflows. Each class gets its own fixed-size artifact
// per backend, so a regression in one class -- or a crossover point where
// SQLite overtakes tinySQL or vice versa -- cannot hide behind an average
// over the others the way BenchmarkPagedIndexMBTilesAccess's mixed corpus
// could.
func BenchmarkPagedIndexMBTilesAccessBySize(b *testing.B) {
	const rows = 4_096
	classes := []struct {
		name string
		size int
	}{
		{"inline_small_256B", 256},
		{"boundary_1569B", 1_569},
		{"boundary_2500B", 2_500},
		{"overflow_50000B", 50_000},
	}
	for _, class := range classes {
		b.Run(class.name, func(b *testing.B) {
			requests := mbtilesBenchmarkRequests(rows, 512)

			b.Run("tinySQL/paged_index", func(b *testing.B) {
				dir := filepath.Join(b.TempDir(), "paged")
				buildFixedSizeMBTilesArtifact(b, dir, rows, class.size)
				reader, err := storage.NewPagedIndexBackend(dir, 32<<20, true)
				if err != nil {
					b.Fatal(err)
				}
				defer reader.Close()
				for _, request := range requests {
					lookupFixedSizeMBTile(b, reader, request, class.size)
				}
				benchmarkLatencyCorpus(b, artifactSize(b, filepath.Join(dir, "tinysql.pages")), func(i int) {
					lookupFixedSizeMBTile(b, reader, requests[i%len(requests)], class.size)
				})
			})

			b.Run("SQLite/immutable", func(b *testing.B) {
				path := filepath.Join(b.TempDir(), "tiles.mbtiles")
				buildFixedSizeSQLiteMBTilesArtifact(b, path, rows, class.size)
				db := openReadOnlySQLite(b, path)
				defer db.Close()
				mapStmt, imageStmt := prepareSQLiteMBTileLookups(b, db)
				defer mapStmt.Close()
				defer imageStmt.Close()
				for _, request := range requests {
					lookupSQLiteMBTileFixedSize(b, mapStmt, imageStmt, request, class.size)
				}
				benchmarkLatencyCorpus(b, artifactSize(b, path), func(i int) {
					lookupSQLiteMBTileFixedSize(b, mapStmt, imageStmt, requests[i%len(requests)], class.size)
				})
			})
		})
	}
}

func buildFixedSizeMBTilesArtifact(b *testing.B, dir string, rows, size int) {
	b.Helper()
	backend, err := storage.NewPagedIndexBackend(dir, 64<<20, false)
	if err != nil {
		b.Fatal(err)
	}
	mapTable := storage.NewTable("map", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_id", Type: storage.TextType},
	}, false)
	imagesTable := storage.NewTable("images", []storage.Column{
		{Name: "tile_id", Type: storage.TextType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	for i := 0; i < rows; i++ {
		z, x, y := mbtilesBenchmarkCoordinates(i)
		tileID := mbtilesBenchmarkTileID(i)
		payload := fixedSizeTilePayload(i, size)
		mapTable.Rows = append(mapTable.Rows, []any{z, x, y, tileID})
		imagesTable.Rows = append(imagesTable.Rows, []any{tileID, payload})
	}
	if err := mapTable.CreateSecondaryIndex("map_zxy", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		b.Fatal(err)
	}
	if err := imagesTable.CreateSecondaryIndex("images_tile_id", []string{"tile_id"}, true); err != nil {
		b.Fatal(err)
	}
	mapTable.Version, imagesTable.Version = 1, 1
	for _, table := range []*storage.Table{mapTable, imagesTable} {
		if err := backend.SaveTable("default", table); err != nil {
			_ = backend.Close()
			b.Fatal(err)
		}
	}
	if err := backend.Close(); err != nil {
		b.Fatal(err)
	}
}

// buildFixedSizeSQLiteMBTilesArtifact is buildSQLiteMBTilesBenchmarkArtifact
// (paged_index_mbtiles_benchmark_test.go) with every payload pinned to size
// instead of the mixed mbtilesBenchmarkPayload distribution, so the SQLite
// side of a size-class comparison is built the same way as the tinySQL side.
func buildFixedSizeSQLiteMBTilesArtifact(b *testing.B, path string, rows, size int) string {
	b.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE map (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_id TEXT)`,
		`CREATE TABLE images (tile_id TEXT, tile_data BLOB)`,
		`CREATE UNIQUE INDEX map_zxy ON map(zoom_level, tile_column, tile_row)`,
		`CREATE UNIQUE INDEX images_tile_id ON images(tile_id)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			b.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	mapInsert, err := tx.Prepare(`INSERT INTO map VALUES (?, ?, ?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	imageInsert, err := tx.Prepare(`INSERT INTO images VALUES (?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		z, x, y := mbtilesBenchmarkCoordinates(i)
		tileID := mbtilesBenchmarkTileID(i)
		if _, err := mapInsert.Exec(z, x, y, tileID); err != nil {
			b.Fatal(err)
		}
		if _, err := imageInsert.Exec(tileID, fixedSizeTilePayload(i, size)); err != nil {
			b.Fatal(err)
		}
	}
	if err := mapInsert.Close(); err != nil {
		b.Fatal(err)
	}
	if err := imageInsert.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return path
}

func fixedSizeTilePayload(i, size int) []byte {
	payload := make([]byte, size)
	for j := range payload {
		payload[j] = byte(i*29 + j*13)
	}
	return payload
}

func lookupFixedSizeMBTile(b *testing.B, backend *storage.PagedIndexBackend, request mbtilesBenchmarkRequest, size int) {
	b.Helper()
	mapRows, handled, err := backend.LookupIndexRows("default", "map", "map_zxy", []any{request.z, request.x, request.y})
	if err != nil || !handled || len(mapRows) != 1 {
		b.Fatalf("map lookup: rows=%d handled=%v err=%v", len(mapRows), handled, err)
	}
	tileID, ok := mapRows[0][3].(string)
	if !ok || tileID != request.tileID {
		b.Fatalf("map tile_id=%#v want %q", mapRows[0][3], request.tileID)
	}
	imageRows, handled, err := backend.LookupIndexRows("default", "images", "images_tile_id", []any{tileID})
	if err != nil || !handled || len(imageRows) != 1 {
		b.Fatalf("image lookup: rows=%d handled=%v err=%v", len(imageRows), handled, err)
	}
	if got, ok := imageRows[0][1].([]byte); !ok || len(got) != size {
		b.Fatalf("tile payload has unexpected shape: %d bytes, want %d", len(got), size)
	}
}

func lookupSQLiteMBTileFixedSize(b *testing.B, mapStmt, imageStmt *sql.Stmt, request mbtilesBenchmarkRequest, size int) {
	b.Helper()
	var tileID string
	if err := mapStmt.QueryRow(request.z, request.x, request.y).Scan(&tileID); err != nil {
		b.Fatal(err)
	}
	if tileID != request.tileID {
		b.Fatalf("SQLite map tile_id=%q want %q", tileID, request.tileID)
	}
	var data []byte
	if err := imageStmt.QueryRow(tileID).Scan(&data); err != nil {
		b.Fatal(err)
	}
	if len(data) != size {
		b.Fatalf("SQLite tile payload has unexpected shape: %d bytes, want %d", len(data), size)
	}
}

// BenchmarkPagedIndexMBTilesAccessParallel measures concurrent readers
// against one shared, already-open artifact -- the shape a tile server
// actually runs under (many HTTP handlers sharing one backend connection/
// pool). SetParallelism scales the number of concurrent goroutines; p50/p95/
// p99 are reported alongside ns/op so contention shows up even when the mean
// does not. tinySQL and SQLite share the same request corpus and the same
// row count, opened read-only the same way BenchmarkPagedIndexMBTilesAccess
// does.
func BenchmarkPagedIndexMBTilesAccessParallel(b *testing.B) {
	const rows = 4_096
	requests := mbtilesBenchmarkRequests(rows, 1_024)
	parallelisms := []int{1, 4, 16}

	b.Run("tinySQL/paged_index", func(b *testing.B) {
		dir := filepath.Join(b.TempDir(), "paged")
		buildPagedMBTilesBenchmarkArtifact(b, dir, rows)
		reader, err := storage.NewPagedIndexBackend(dir, 32<<20, true)
		if err != nil {
			b.Fatal(err)
		}
		defer reader.Close()
		for _, request := range requests {
			lookupPagedMBTile(b, reader, request)
		}
		for _, parallelism := range parallelisms {
			b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
				runParallelLatencyBenchmark(b, parallelism, func(i int) {
					lookupPagedMBTile(b, reader, requests[i%len(requests)])
				})
			})
		}
	})

	b.Run("SQLite/immutable", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "tiles.mbtiles")
		buildSQLiteMBTilesBenchmarkArtifact(b, path, rows)
		db := openReadOnlySQLite(b, path)
		defer db.Close()
		// database/sql pools connections internally; a modest pool lets
		// concurrent goroutines actually run in parallel instead of queuing
		// on a single connection, matching how a real server would configure it.
		db.SetMaxOpenConns(16)
		mapStmt, imageStmt := prepareSQLiteMBTileLookups(b, db)
		defer mapStmt.Close()
		defer imageStmt.Close()
		for _, request := range requests {
			lookupSQLiteMBTile(b, mapStmt, imageStmt, request)
		}
		for _, parallelism := range parallelisms {
			b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
				runParallelLatencyBenchmark(b, parallelism, func(i int) {
					lookupSQLiteMBTile(b, mapStmt, imageStmt, requests[i%len(requests)])
				})
			})
		}
	})
}

// runParallelLatencyBenchmark runs lookup(i) under b.RunParallel at the given
// parallelism, recording per-call latency (not just the aggregate ns/op Go
// reports, which divides by the *total* op count across every goroutine) plus
// GC cycles and peak RSS.
func runParallelLatencyBenchmark(b *testing.B, parallelism int, lookup func(i int)) {
	b.Helper()
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		local := make([]time.Duration, 0, 64)
		for pb.Next() {
			start := time.Now()
			lookup(i)
			i++
			local = append(local, time.Since(start))
		}
		latencyMu.Lock()
		latencies = append(latencies, local...)
		latencyMu.Unlock()
	})
	b.StopTimer()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(percentileDuration(latencies, 50))/1e3, "p50_us")
	b.ReportMetric(float64(percentileDuration(latencies, 95))/1e3, "p95_us")
	b.ReportMetric(float64(percentileDuration(latencies, 99))/1e3, "p99_us")
	b.ReportMetric(float64(after.NumGC-before.NumGC), "gc_cycles")
	if rss := benchmarkPeakRSSBytes(); rss > 0 {
		b.ReportMetric(float64(rss), "peak_rss_B")
	}
}

// BenchmarkPagedIndexMBTilesOpenReopen isolates the cost of opening a
// published artifact read-only -- catalog/schema decode plus the first page
// reads -- separately from any lookup that follows it. This is what a tile
// server pays once per process start (or per reload after a tileset
// republish), as opposed to the steady-state per-request cost the other
// benchmarks in this file measure. tinySQL's paged-index open and SQLite's
// immutable open are measured the same way, at the same row counts.
func BenchmarkPagedIndexMBTilesOpenReopen(b *testing.B) {
	for _, rows := range []int{1_024, 16_384} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.Run("tinySQL/paged_index", func(b *testing.B) {
				dir := filepath.Join(b.TempDir(), "paged")
				buildPagedMBTilesBenchmarkArtifact(b, dir, rows)
				artifactBytes := artifactSize(b, filepath.Join(dir, "tinysql.pages"))

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					reader, err := storage.NewPagedIndexBackend(dir, 32<<20, true)
					if err != nil {
						b.Fatal(err)
					}
					if err := reader.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(artifactBytes), "artifact_B")
			})

			b.Run("SQLite/immutable", func(b *testing.B) {
				path := filepath.Join(b.TempDir(), "tiles.mbtiles")
				buildSQLiteMBTilesBenchmarkArtifact(b, path, rows)
				artifactBytes := artifactSize(b, path)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					db := openReadOnlySQLite(b, path)
					if err := db.Ping(); err != nil {
						b.Fatal(err)
					}
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(artifactBytes), "artifact_B")
			})
		})
	}
}
