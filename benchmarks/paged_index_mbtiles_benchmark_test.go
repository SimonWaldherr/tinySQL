package benchmarks

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

// BenchmarkPagedIndexMBTilesAccess measures the actual MBTiles access shape:
// z/x/y -> tile_id in map -> tile_data in images. Run with a fixed iteration
// count when comparing machines, for example:
//
// go test ./benchmarks -run '^$' -bench '^BenchmarkPagedIndexMBTilesAccess$' -benchmem -benchtime=1000x -count=5
//
// The output includes p50/p95/p99 latency, live heap, peak RSS and artifact
// bytes in addition to Go's ns/op and allocation metrics. The persistent
// paged-index artifact and SQLite file use the same generated rows and the
// same deterministic request corpus.
func BenchmarkPagedIndexMBTilesAccess(b *testing.B) {
	const rows = 4_096
	dir := b.TempDir()
	requests := mbtilesBenchmarkRequests(rows, 1_024)
	pagedDir := buildPagedMBTilesBenchmarkArtifact(b, filepath.Join(dir, "paged"), rows)
	sqlitePath := buildSQLiteMBTilesBenchmarkArtifact(b, filepath.Join(dir, "tiles.mbtiles"), rows)

	b.Run("tinySQL/paged_index/warm", func(b *testing.B) {
		reader, err := storage.NewPagedIndexBackend(pagedDir, 32<<20, true)
		if err != nil {
			b.Fatal(err)
		}
		defer reader.Close()
		// Keep the artifact open for the measured serving path; one corpus pass
		// establishes the bounded page cache without retaining BLOBs as rows.
		for _, request := range requests {
			lookupPagedMBTile(b, reader, request)
		}
		benchmarkLatencyCorpus(b, artifactSize(b, filepath.Join(pagedDir, "tinysql.pages")), func(i int) {
			lookupPagedMBTile(b, reader, requests[i%len(requests)])
		})
	})

	b.Run("SQLite/immutable/warm", func(b *testing.B) {
		db := openReadOnlySQLite(b, sqlitePath)
		defer db.Close()
		mapStmt, imageStmt := prepareSQLiteMBTileLookups(b, db)
		defer mapStmt.Close()
		defer imageStmt.Close()
		for _, request := range requests {
			lookupSQLiteMBTile(b, mapStmt, imageStmt, request)
		}
		benchmarkLatencyCorpus(b, artifactSize(b, sqlitePath), func(i int) {
			lookupSQLiteMBTile(b, mapStmt, imageStmt, requests[i%len(requests)])
		})
	})

	b.Run("tinySQL/paged_index/cold", func(b *testing.B) {
		benchmarkLatencyCorpus(b, artifactSize(b, filepath.Join(pagedDir, "tinysql.pages")), func(i int) {
			reader, err := storage.NewPagedIndexBackend(pagedDir, 32<<20, true)
			if err != nil {
				b.Fatal(err)
			}
			lookupPagedMBTile(b, reader, requests[i%len(requests)])
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("SQLite/immutable/cold", func(b *testing.B) {
		benchmarkLatencyCorpus(b, artifactSize(b, sqlitePath), func(i int) {
			db := openReadOnlySQLite(b, sqlitePath)
			mapStmt, imageStmt := prepareSQLiteMBTileLookups(b, db)
			lookupSQLiteMBTile(b, mapStmt, imageStmt, requests[i%len(requests)])
			_ = mapStmt.Close()
			_ = imageStmt.Close()
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
		})
	})
}

type mbtilesBenchmarkRequest struct {
	row     int
	z, x, y int
	tileID  string
}

func mbtilesBenchmarkRequests(rows, n int) []mbtilesBenchmarkRequest {
	rng := rand.New(rand.NewSource(0xB16B00B5))
	requests := make([]mbtilesBenchmarkRequest, n)
	for i := range requests {
		row := rng.Intn(rows)
		z, x, y := mbtilesBenchmarkCoordinates(row)
		requests[i] = mbtilesBenchmarkRequest{row: row, z: z, x: x, y: y, tileID: mbtilesBenchmarkTileID(row)}
	}
	return requests
}

func buildPagedMBTilesBenchmarkArtifact(b *testing.B, dir string, rows int) string {
	b.Helper()
	backend, err := storage.NewPagedIndexBackend(dir, 64<<20, false)
	if err != nil {
		b.Fatal(err)
	}
	mapTable, imagesTable := mbtilesBenchmarkTables(rows)
	for _, table := range []*storage.Table{mapTable, imagesTable} {
		if err := backend.SaveTable("default", table); err != nil {
			_ = backend.Close()
			b.Fatal(err)
		}
	}
	if err := backend.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

func buildSQLiteMBTilesBenchmarkArtifact(b *testing.B, path string, rows int) string {
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
		if _, err := imageInsert.Exec(tileID, mbtilesBenchmarkPayload(i)); err != nil {
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

func mbtilesBenchmarkTables(rows int) (*storage.Table, *storage.Table) {
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
		mapTable.Rows = append(mapTable.Rows, []any{z, x, y, tileID})
		imagesTable.Rows = append(imagesTable.Rows, []any{tileID, mbtilesBenchmarkPayload(i)})
	}
	if err := mapTable.CreateSecondaryIndex("map_zxy", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		panic(err)
	}
	if err := imagesTable.CreateSecondaryIndex("images_tile_id", []string{"tile_id"}, true); err != nil {
		panic(err)
	}
	mapTable.Version = 1
	imagesTable.Version = 1
	return mapTable, imagesTable
}

func lookupPagedMBTile(b *testing.B, backend *storage.PagedIndexBackend, request mbtilesBenchmarkRequest) {
	b.Helper()
	mapRows, handled, err := backend.LookupIndexRows("default", "map", "map_zxy", []any{request.z, request.x, request.y})
	if err != nil || !handled || len(mapRows) != 1 {
		b.Fatalf("paged map lookup: rows=%d handled=%v err=%v", len(mapRows), handled, err)
	}
	tileID, ok := mapRows[0][3].(string)
	if !ok || tileID != request.tileID {
		b.Fatalf("paged map tile_id=%#v want %q", mapRows[0][3], request.tileID)
	}
	imageRows, handled, err := backend.LookupIndexRows("default", "images", "images_tile_id", []any{tileID})
	if err != nil || !handled || len(imageRows) != 1 {
		b.Fatalf("paged image lookup: rows=%d handled=%v err=%v", len(imageRows), handled, err)
	}
	if got, ok := imageRows[0][1].([]byte); !ok || len(got) != mbtilesBenchmarkPayloadSize(request.row) {
		b.Fatalf("paged tile payload has unexpected shape")
	}
}

func openReadOnlySQLite(b *testing.B, path string) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func prepareSQLiteMBTileLookups(b *testing.B, db *sql.DB) (*sql.Stmt, *sql.Stmt) {
	b.Helper()
	mapStmt, err := db.Prepare(`SELECT tile_id FROM map WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`)
	if err != nil {
		b.Fatal(err)
	}
	imageStmt, err := db.Prepare(`SELECT tile_data FROM images WHERE tile_id = ?`)
	if err != nil {
		_ = mapStmt.Close()
		b.Fatal(err)
	}
	return mapStmt, imageStmt
}

func lookupSQLiteMBTile(b *testing.B, mapStmt, imageStmt *sql.Stmt, request mbtilesBenchmarkRequest) {
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
	if len(data) != mbtilesBenchmarkPayloadSize(request.row) {
		b.Fatalf("SQLite tile payload has unexpected shape")
	}
}

func benchmarkLatencyCorpus(b *testing.B, artifactBytes int64, lookup func(int)) {
	b.Helper()
	latencies := make([]time.Duration, 0, b.N)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		lookup(i)
		latencies = append(latencies, time.Since(start))
	}
	b.StopTimer()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(percentileDuration(latencies, 50))/1e3, "p50_us")
	b.ReportMetric(float64(percentileDuration(latencies, 95))/1e3, "p95_us")
	b.ReportMetric(float64(percentileDuration(latencies, 99))/1e3, "p99_us")
	b.ReportMetric(float64(after.HeapAlloc), "heap_B")
	b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "heap_delta_B")
	if rss := benchmarkPeakRSSBytes(); rss > 0 {
		b.ReportMetric(float64(rss), "peak_rss_B")
	}
	b.ReportMetric(float64(artifactBytes), "artifact_B")
}

func percentileDuration(sorted []time.Duration, percentile int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted) - 1) * percentile / 100
	return sorted[idx]
}

func artifactSize(b *testing.B, path string) int64 {
	b.Helper()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	return info.Size()
}

func mbtilesBenchmarkCoordinates(i int) (z, x, y int) {
	return i % 9, (i * 37) % 8_192, (i * 73) % 8_192
}

func mbtilesBenchmarkTileID(i int) string { return fmt.Sprintf("tile-%05d-%08x", i, i*0x9e3779b1) }

func mbtilesBenchmarkPayload(i int) []byte {
	payload := make([]byte, mbtilesBenchmarkPayloadSize(i))
	for j := range payload {
		payload[j] = byte(i*29 + j*13)
	}
	return payload
}

func mbtilesBenchmarkPayloadSize(i int) int {
	switch i % 8 {
	case 0:
		return 48
	case 7:
		return 9_000 + i%997
	default:
		return 1_400 + (i*113)%1_101
	}
}
