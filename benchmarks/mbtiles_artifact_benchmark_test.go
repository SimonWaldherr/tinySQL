//go:build sqliteimport && !js && !wasm && !baremetal

package benchmarks

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
)

// BenchmarkMBTilesArtifactAgainstSQLite compares the complete artifact import
// with a SQLite copy of the same flat MBTiles rows. It is intentionally an
// opt-in benchmark (the work is proportional to b.N) and reports import time,
// artifact/database size and validated point-open time. Access p50/p95/p99
// and parallel reader measurements live in BenchmarkPagedIndexMBTilesAccess.
func BenchmarkMBTilesArtifactAgainstSQLite(b *testing.B) {
	source := filepath.Join(b.TempDir(), "source.mbtiles")
	buildValidArtifactBenchmarkSource(b, source, 4096)
	ctx := context.Background()

	b.Run("tinySQL/dataset.tinysql", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			target := filepath.Join(b.TempDir(), fmt.Sprintf("tinysql-%d.tinysql", i))
			start := time.Now()
			_, err := importer.ImportMBTilesArtifact(ctx, source, target, &importer.MBTilesArtifactOptions{Schema: importer.MBTilesSchemaFlat, BatchSize: 512, MaxMemoryBytes: 64 << 20})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(time.Since(start).Microseconds()), "import_us")
			b.ReportMetric(float64(directoryBytes(b, target)), "artifact_B")
			openStart := time.Now()
			reader, err := importer.OpenMBTilesReader(ctx, target, 16<<20)
			if err != nil {
				b.Fatal(err)
			}
			_, found, err := reader.LookupTile(ctx, 8, 7, 7)
			if err != nil || !found {
				b.Fatalf("point parity lookup: found=%v err=%v", found, err)
			}
			_ = reader.Close()
			b.ReportMetric(float64(time.Since(openStart).Microseconds()), "open_point_us")
		}
	})

	b.Run("SQLite/mbtiles-copy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			target := filepath.Join(b.TempDir(), fmt.Sprintf("sqlite-%d.mbtiles", i))
			start := time.Now()
			copyFlatSQLiteBenchmark(b, source, target)
			b.ReportMetric(float64(time.Since(start).Microseconds()), "import_us")
			b.ReportMetric(float64(directoryBytes(b, target)), "database_B")
		}
	})
}

// BenchmarkMBTilesArtifactDesktopFixtureAccess runs the serving comparison on
// an explicitly supplied real fixture (for example full DACH). It is opt-in:
// the artifact must already have passed the importer tests, so the benchmark
// never creates or mutates a multi-gigabyte dataset.
//
// TINySQL_MBtiles_SOURCE=/private/tmp/dach-optimized.mbtiles \
// TINySQL_MBtiles_ARTIFACT=/private/tmp/dach.dataset.tinysql \
//
//	go test -tags=sqliteimport ./benchmarks -run '^$' \
//	  -bench '^BenchmarkMBTilesArtifactDesktopFixtureAccess$' -benchmem -count=5
func BenchmarkMBTilesArtifactDesktopFixtureAccess(b *testing.B) {
	source := os.Getenv("TINySQL_MBtiles_SOURCE")
	artifact := os.Getenv("TINySQL_MBtiles_ARTIFACT")
	if source == "" || artifact == "" {
		b.Skip("set TINySQL_MBtiles_SOURCE and TINySQL_MBtiles_ARTIFACT to benchmark an external MBTiles fixture")
	}
	if _, err := os.Stat(artifact); err != nil {
		b.Skipf("artifact is unavailable: %v", err)
	}
	src, err := sql.Open("sqlite", "file:"+source+"?mode=ro&immutable=1")
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close()
	corpus := desktopFixtureBenchmarkCorpus(b, src, 1024)
	rng := rand.New(rand.NewSource(0xDA4C))
	rng.Shuffle(len(corpus), func(i, j int) { corpus[i], corpus[j] = corpus[j], corpus[i] })

	b.Run("tinySQL/point/warm", func(b *testing.B) {
		reader, err := importer.OpenMBTilesReader(context.Background(), artifact, 64<<20)
		if err != nil {
			b.Fatal(err)
		}
		defer reader.Close()
		for _, request := range corpus {
			lookupDesktopFixtureArtifactPoint(b, reader, request)
		}
		benchmarkLatencyCorpus(b, directoryBytes(b, artifact), func(i int) {
			lookupDesktopFixtureArtifactPoint(b, reader, corpus[i%len(corpus)])
		})
	})

	b.Run("SQLite/point/warm", func(b *testing.B) {
		stmt, err := src.Prepare(`SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?`)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		for _, request := range corpus {
			lookupDesktopFixtureSQLitePoint(b, stmt, request)
		}
		benchmarkLatencyCorpus(b, artifactSize(b, source), func(i int) {
			lookupDesktopFixtureSQLitePoint(b, stmt, corpus[i%len(corpus)])
		})
	})

	b.Run("tinySQL/spatial/warm", func(b *testing.B) {
		reader, err := importer.OpenMBTilesReader(context.Background(), artifact, 64<<20)
		if err != nil {
			b.Fatal(err)
		}
		defer reader.Close()
		benchmarkLatencyCorpus(b, directoryBytes(b, artifact), func(i int) {
			lookupDesktopFixtureArtifactRange(b, reader, corpus[i%len(corpus)])
		})
	})

	b.Run("SQLite/spatial/warm", func(b *testing.B) {
		stmt, err := src.Prepare(`SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles WHERE zoom_level=? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?`)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		benchmarkLatencyCorpus(b, artifactSize(b, source), func(i int) {
			lookupDesktopFixtureSQLiteRange(b, stmt, corpus[i%len(corpus)])
		})
	})
}

type desktopFixtureBenchmarkTile struct {
	z, x, y int
	data    []byte
}

func desktopFixtureBenchmarkCorpus(b *testing.B, db *sql.DB, limit int) []desktopFixtureBenchmarkTile {
	b.Helper()
	levelRows, err := db.Query(`SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level`)
	if err != nil {
		b.Fatal(err)
	}
	var levels []int
	for levelRows.Next() {
		var z int
		if err := levelRows.Scan(&z); err != nil {
			_ = levelRows.Close()
			b.Fatal(err)
		}
		levels = append(levels, z)
	}
	if err := levelRows.Err(); err != nil {
		_ = levelRows.Close()
		b.Fatal(err)
	}
	if err := levelRows.Close(); err != nil {
		b.Fatal(err)
	}
	if len(levels) == 0 {
		b.Fatal("MBTiles fixture has no tile zoom levels")
	}
	perLevel := (limit + len(levels) - 1) / len(levels)
	corpus := make([]desktopFixtureBenchmarkTile, 0, perLevel*len(levels))
	for _, z := range levels {
		rows, err := db.Query(`SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles WHERE zoom_level=? ORDER BY tile_column,tile_row LIMIT ?`, z, perLevel)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var request desktopFixtureBenchmarkTile
			if err := rows.Scan(&request.z, &request.x, &request.y, &request.data); err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
			corpus = append(corpus, request)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
	if len(corpus) == 0 {
		b.Fatal("MBTiles fixture has no tiles")
	}
	return corpus
}

func lookupDesktopFixtureArtifactPoint(b *testing.B, reader *importer.MBTilesReader, want desktopFixtureBenchmarkTile) {
	b.Helper()
	got, found, err := reader.LookupTile(context.Background(), want.z, want.x, want.y)
	if err != nil || !found || !bytes.Equal(got, want.data) {
		b.Fatalf("tinySQL point z/x/y=%d/%d/%d found=%v err=%v", want.z, want.x, want.y, found, err)
	}
}

func lookupDesktopFixtureSQLitePoint(b *testing.B, stmt *sql.Stmt, want desktopFixtureBenchmarkTile) {
	b.Helper()
	var got []byte
	if err := stmt.QueryRow(want.z, want.x, want.y).Scan(&got); err != nil || !bytes.Equal(got, want.data) {
		b.Fatalf("SQLite point z/x/y=%d/%d/%d err=%v", want.z, want.x, want.y, err)
	}
}

func lookupDesktopFixtureArtifactRange(b *testing.B, reader *importer.MBTilesReader, want desktopFixtureBenchmarkTile) {
	b.Helper()
	xMax, yMax := desktopFixtureBenchmarkRange(want)
	matched := false
	err := reader.ScanTileRange(context.Background(), want.z, want.x, xMax, want.y, yMax, func(z, x, y int, data []byte) bool {
		if z == want.z && x == want.x && y == want.y && bytes.Equal(data, want.data) {
			matched = true
		}
		return true
	})
	if err != nil || !matched {
		b.Fatalf("tinySQL spatial z/x/y=%d/%d/%d matched=%v err=%v", want.z, want.x, want.y, matched, err)
	}
}

func lookupDesktopFixtureSQLiteRange(b *testing.B, stmt *sql.Stmt, want desktopFixtureBenchmarkTile) {
	b.Helper()
	xMax, yMax := desktopFixtureBenchmarkRange(want)
	rows, err := stmt.Query(want.z, want.x, xMax, want.y, yMax)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	matched := false
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			b.Fatal(err)
		}
		if z == want.z && x == want.x && y == want.y && bytes.Equal(data, want.data) {
			matched = true
		}
	}
	if err := rows.Err(); err != nil || !matched {
		b.Fatalf("SQLite spatial z/x/y=%d/%d/%d matched=%v err=%v", want.z, want.x, want.y, matched, err)
	}
}

func desktopFixtureBenchmarkRange(tile desktopFixtureBenchmarkTile) (xMax, yMax int) {
	limit := 1 << tile.z
	xMax, yMax = tile.x+1, tile.y+1
	if xMax >= limit {
		xMax = limit - 1
	}
	if yMax >= limit {
		yMax = limit - 1
	}
	return xMax, yMax
}

func buildValidArtifactBenchmarkSource(tb testing.TB, path string, n int) {
	tb.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE metadata(name TEXT,value TEXT); CREATE TABLE tiles(zoom_level INTEGER,tile_column INTEGER,tile_row INTEGER,tile_data BLOB); INSERT INTO metadata VALUES('name','benchmark');`); err != nil {
		tb.Fatal(err)
	}
	stmt, err := db.Prepare(`INSERT INTO tiles VALUES(?,?,?,?)`)
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < n; i++ {
		z := 8
		x, y := i%256, i/256
		if _, err := stmt.Exec(z, x, y, mbtilesBenchmarkPayload(i)); err != nil {
			tb.Fatal(err)
		}
	}
}

func copyFlatSQLiteBenchmark(tb testing.TB, source, target string) {
	tb.Helper()
	src, err := sql.Open("sqlite", source)
	if err != nil {
		tb.Fatal(err)
	}
	defer src.Close()
	dst, err := sql.Open("sqlite", target)
	if err != nil {
		tb.Fatal(err)
	}
	defer dst.Close()
	if _, err := dst.Exec(`CREATE TABLE tiles(z INTEGER,x INTEGER,y INTEGER,tile_data BLOB); CREATE UNIQUE INDEX tiles_zxy ON tiles(z,x,y);`); err != nil {
		tb.Fatal(err)
	}
	tx, err := dst.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO tiles VALUES(?,?,?,?)`)
	if err != nil {
		tb.Fatal(err)
	}
	rows, err := src.Query(`SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles ORDER BY zoom_level,tile_column,tile_row`)
	if err != nil {
		tb.Fatal(err)
	}
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			tb.Fatal(err)
		}
		if _, err := stmt.Exec(z, x, y, data); err != nil {
			tb.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	_ = rows.Close()
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func directoryBytes(b *testing.B, root string) int64 {
	b.Helper()
	var total int64
	if err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return total
}
