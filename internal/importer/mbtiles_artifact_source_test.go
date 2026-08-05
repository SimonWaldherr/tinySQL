//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// TestMBTilesArtifactDesktopFixture is opt-in so ordinary CI never depends on
// a developer workstation. It runs the production-shaped importer against the
// regional fixture found on the Desktop and leaves the DACH PBF untouched.
//
// TINySQL_MBtiles_SOURCE=/path/to/bayern-shortbread-1.0.mbtiles \
// TINySQL_MBtiles_ARTIFACT=/private/tmp/bayern.dataset.tinysql \
// go test -tags=sqliteimport ./internal/importer -run TestMBTilesArtifactDesktopFixture -count=1 -v
func TestMBTilesArtifactDesktopFixture(t *testing.T) {
	source := os.Getenv("TINySQL_MBtiles_SOURCE")
	target := os.Getenv("TINySQL_MBtiles_ARTIFACT")
	if source == "" || target == "" {
		t.Skip("set TINySQL_MBtiles_SOURCE and TINySQL_MBtiles_ARTIFACT to run the desktop fixture")
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	result, err := ImportMBTilesArtifact(context.Background(), source, target, &MBTilesArtifactOptions{
		Schema:          MBTilesSchemaFlat,
		BatchSize:       64,
		MaxMemoryBytes:  256 << 20,
		MinFreeBytes:    1 << 30,
		ProgressEvery:   5 * time.Second,
		ReplaceExisting: true,
		Progress: func(p MBTilesProgress) {
			if p.Phase == "preflight" && p.Estimate != nil {
				t.Logf("preflight: source=%d tiles=%d estimated_memory=%d estimated_disk=%d available_disk=%d", p.Estimate.SourceBytes, p.Estimate.TileCount, p.Estimate.EstimatedMemory, p.Estimate.EstimatedDisk, p.Estimate.AvailableDisk)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("published %s: %#v", result.ArtifactPath, result.Manifest.Resources)
	if _, err := ValidateMBTilesArtifact(context.Background(), target); err != nil {
		t.Fatal(err)
	}
}

// TestMBTilesArtifactDesktopFixtureParity compares every source tile byte for
// byte with the published artifact. It assumes the import test has already
// produced TINySQL_MBtiles_ARTIFACT and is intentionally opt-in because it
// performs one indexed lookup per source tile.
func TestMBTilesArtifactDesktopFixtureParity(t *testing.T) {
	source := os.Getenv("TINySQL_MBtiles_SOURCE")
	target := os.Getenv("TINySQL_MBtiles_ARTIFACT")
	if source == "" || target == "" {
		t.Skip("set TINySQL_MBtiles_SOURCE and TINySQL_MBtiles_ARTIFACT to run the desktop fixture")
	}
	src, err := sql.Open("sqlite", "file:"+source+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	reader, err := OpenMBTilesReader(context.Background(), target, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := src.QueryContext(context.Background(), `SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles ORDER BY zoom_level, tile_column, tile_row`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var checked int64
	for rows.Next() {
		var z, x, y int
		var want []byte
		if err := rows.Scan(&z, &x, &y, &want); err != nil {
			t.Fatal(err)
		}
		got, found, err := reader.LookupTile(context.Background(), z, x, y)
		if err != nil {
			t.Fatalf("lookup z/x/y=%d/%d/%d: %v", z, x, y, err)
		}
		if !found || !bytes.Equal(got, want) {
			t.Fatalf("tile parity failed z/x/y=%d/%d/%d found=%v got=%d want=%d", z, x, y, found, len(got), len(want))
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	metadata, err := src.QueryContext(context.Background(), `SELECT name,value FROM metadata ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	var metadataChecked int64
	for metadata.Next() {
		var name, want string
		if err := metadata.Scan(&name, &want); err != nil {
			t.Fatal(err)
		}
		got, found, err := reader.LookupMetadata(context.Background(), name)
		if err != nil || !found || got != want {
			t.Fatalf("metadata parity failed name=%q found=%v got=%q want=%q err=%v", name, found, got, want, err)
		}
		metadataChecked++
	}
	if err := metadata.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("verified byte parity for %d source tiles and %d metadata rows", checked, metadataChecked)
}

// TestMBTilesArtifactDesktopFixtureReaders exercises the same lifecycle as a
// tile server without coupling the artifact to one. The corpus is deliberately
// bounded: DACH parity is covered by TestMBTilesArtifactDesktopFixtureParity,
// while this test concentrates on concurrent opens, exact TMS point lookups,
// and contiguous ranges.
func TestMBTilesArtifactDesktopFixtureReaders(t *testing.T) {
	source, target := desktopFixturePaths(t)
	src, err := sql.Open("sqlite", "file:"+source+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	corpus := desktopFixtureCorpus(t, src, 256)
	for restart := 0; restart < 3; restart++ {
		for _, readers := range []int{1, 4, 8} {
			errs := make(chan error, readers)
			var wg sync.WaitGroup
			for worker := 0; worker < readers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					reader, err := OpenMBTilesReader(context.Background(), target, 64<<20)
					if err != nil {
						errs <- err
						return
					}
					defer reader.Close()
					rng := rand.New(rand.NewSource(int64(restart*97 + readers*13 + worker)))
					for i := 0; i < 64; i++ {
						want := corpus[rng.Intn(len(corpus))]
						got, found, err := reader.LookupTile(context.Background(), want.z, want.x, want.y)
						if err != nil || !found || !bytes.Equal(got, want.data) {
							errs <- fmt.Errorf("worker %d point z/x/y=%d/%d/%d found=%v err=%v", worker, want.z, want.x, want.y, found, err)
							return
						}
					}
					for i := 0; i < 8; i++ {
						want := corpus[rng.Intn(len(corpus))]
						xMax, yMax := desktopFixtureRange(want)
						matched := false
						err := reader.ScanTileRange(context.Background(), want.z, want.x, xMax, want.y, yMax, func(z, x, y int, data []byte) bool {
							if z == want.z && x == want.x && y == want.y && bytes.Equal(data, want.data) {
								matched = true
							}
							return true
						})
						if err != nil || !matched {
							errs <- fmt.Errorf("worker %d spatial z/x/y=%d/%d/%d matched=%v err=%v", worker, want.z, want.x, want.y, matched, err)
							return
						}
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

// TestMBTilesArtifactDesktopFixtureP95Gate is an opt-in, reproducible warm
// point-lookup gate. It compares the same randomized TMS corpus against the
// source SQLite database and the already validated artifact.
func TestMBTilesArtifactDesktopFixtureP95Gate(t *testing.T) {
	source, target := desktopFixturePaths(t)
	src, err := sql.Open("sqlite", "file:"+source+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	corpus := desktopFixtureCorpus(t, src, 512)
	rng := rand.New(rand.NewSource(0xDA4C))
	rng.Shuffle(len(corpus), func(i, j int) { corpus[i], corpus[j] = corpus[j], corpus[i] })

	sqliteStmt, err := src.Prepare(`SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?`)
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteStmt.Close()
	for _, want := range corpus {
		assertSQLiteFixtureTile(t, sqliteStmt, want)
	}
	sqliteP50, sqliteP95, sqliteP99 := desktopFixtureLatency(corpus, func(want desktopFixtureTile) {
		assertSQLiteFixtureTile(t, sqliteStmt, want)
	})

	reader, err := OpenMBTilesReader(context.Background(), target, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, want := range corpus {
		assertArtifactFixtureTile(t, reader, want)
	}
	pagedP50, pagedP95, pagedP99 := desktopFixtureLatency(corpus, func(want desktopFixtureTile) {
		assertArtifactFixtureTile(t, reader, want)
	})
	t.Logf("warm point lookup: SQLite p50/p95/p99=%s/%s/%s tinySQL=%s/%s/%s", sqliteP50, sqliteP95, sqliteP99, pagedP50, pagedP95, pagedP99)
	if pagedP95 > 2*sqliteP95 {
		t.Fatalf("paged-index p95 %s exceeds 2x SQLite p95 %s", pagedP95, sqliteP95)
	}
}

type desktopFixtureTile struct {
	z, x, y int
	data    []byte
}

func desktopFixturePaths(t *testing.T) (string, string) {
	t.Helper()
	source := os.Getenv("TINySQL_MBtiles_SOURCE")
	target := os.Getenv("TINySQL_MBtiles_ARTIFACT")
	if source == "" || target == "" {
		t.Skip("set TINySQL_MBtiles_SOURCE and TINySQL_MBtiles_ARTIFACT to run the desktop fixture")
	}
	return source, target
}

func desktopFixtureCorpus(t *testing.T, src *sql.DB, limit int) []desktopFixtureTile {
	t.Helper()
	levelRows, err := src.Query(`SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level`)
	if err != nil {
		t.Fatal(err)
	}
	var levels []int
	for levelRows.Next() {
		var z int
		if err := levelRows.Scan(&z); err != nil {
			_ = levelRows.Close()
			t.Fatal(err)
		}
		levels = append(levels, z)
	}
	if err := levelRows.Err(); err != nil {
		_ = levelRows.Close()
		t.Fatal(err)
	}
	if err := levelRows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(levels) == 0 {
		t.Fatal("fixture has no tile zoom levels")
	}
	perLevel := (limit + len(levels) - 1) / len(levels)
	var corpus []desktopFixtureTile
	for _, z := range levels {
		rows, err := src.Query(`SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles WHERE zoom_level=? ORDER BY tile_column,tile_row LIMIT ?`, z, perLevel)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var tile desktopFixtureTile
			if err := rows.Scan(&tile.z, &tile.x, &tile.y, &tile.data); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			corpus = append(corpus, tile)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(corpus) == 0 {
		t.Fatal("fixture has no tiles")
	}
	return corpus
}

func desktopFixtureRange(tile desktopFixtureTile) (xMax, yMax int) {
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

func assertSQLiteFixtureTile(t *testing.T, stmt *sql.Stmt, want desktopFixtureTile) {
	t.Helper()
	var got []byte
	if err := stmt.QueryRow(want.z, want.x, want.y).Scan(&got); err != nil || !bytes.Equal(got, want.data) {
		t.Fatalf("SQLite point z/x/y=%d/%d/%d err=%v", want.z, want.x, want.y, err)
	}
}

func assertArtifactFixtureTile(t *testing.T, reader *MBTilesReader, want desktopFixtureTile) {
	t.Helper()
	got, found, err := reader.LookupTile(context.Background(), want.z, want.x, want.y)
	if err != nil || !found || !bytes.Equal(got, want.data) {
		t.Fatalf("artifact point z/x/y=%d/%d/%d found=%v err=%v", want.z, want.x, want.y, found, err)
	}
}

func desktopFixtureLatency(corpus []desktopFixtureTile, lookup func(desktopFixtureTile)) (time.Duration, time.Duration, time.Duration) {
	latencies := make([]time.Duration, 0, len(corpus))
	for _, tile := range corpus {
		start := time.Now()
		lookup(tile)
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return desktopFixturePercentile(latencies, 50), desktopFixturePercentile(latencies, 95), desktopFixturePercentile(latencies, 99)
}

func desktopFixturePercentile(latencies []time.Duration, p int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	return latencies[(len(latencies)-1)*p/100]
}
