package benchmarks

// Tile-serving head-to-head: tinySQL vs SQLite on the one query a tile server
// actually runs.
//
//	SELECT tile_data FROM tiles
//	WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?
//
// That is the whole workload. A tile server does nothing else per request, so
// this single point lookup decides whether tinySQL can replace SQLite for
// MBTiles serving — the substitution docs/storage-guide.md currently tells users
// not to make.
//
// Both engines go through database/sql with bound parameters and a single
// connection, so the comparison includes the driver, parameter binding and row
// scanning that a real server pays. Tiles are random per iteration, defeating
// any single-tile cache and matching how a map client pans across a viewport.
//
// "SQLite" here is modernc.org/sqlite, the pure-Go port -- not the C
// implementation. See the note at the top of BENCHMARKS.md.

import (
	"database/sql"
	"math/rand"
	"path/filepath"
	"testing"

	_ "github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

// tileBenchZoom is the zoom whose full grid is loaded. Zoom 8 is 65,536 tiles,
// enough that a linear scan and an index seek are unmistakably different, and
// small enough to build quickly.
const tileBenchZoom = 8

// tileBenchPayload is a plausible compressed vector-tile size. Payload size
// matters: it is what a scan has to move through memory even when it will not
// return the row.
const tileBenchPayload = 800

type tileEngine struct {
	name      string
	open      func(b *testing.B) *sql.DB
	createDDL string
	createIdx string
}

func tileEngines() []tileEngine {
	return []tileEngine{
		{
			name:      "tinySQL/mem",
			open:      func(b *testing.B) *sql.DB { return openParity(b, "tinysql", "mem://?tenant=default") },
			createDDL: `CREATE TABLE tiles (zoom_level INT, tile_column INT, tile_row INT, tile_data BLOB)`,
			createIdx: `CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)`,
		},
		{
			name:      "SQLite/mem",
			open:      func(b *testing.B) *sql.DB { return openParity(b, "sqlite", ":memory:") },
			createDDL: `CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)`,
			createIdx: `CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)`,
		},
		{
			name: "SQLite/file",
			open: func(b *testing.B) *sql.DB {
				path := filepath.Join(tmpDir(b), "tiles.mbtiles")
				return openParity(b, "sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
			},
			createDDL: `CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)`,
			createIdx: `CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)`,
		},
	}
}

// loadTileset fills a tiles table with every tile of one zoom level, up to
// maxTiles. Returns the grid side length actually used.
func loadTileset(b *testing.B, db *sql.DB, eng tileEngine, withIndex bool, maxTiles int) int {
	b.Helper()
	if _, err := db.Exec(eng.createDDL); err != nil {
		b.Fatalf("%s: create tiles: %v", eng.name, err)
	}

	side := 1 << uint(tileBenchZoom)
	if side*side > maxTiles {
		// Keep a square grid so coordinates stay uniformly distributed.
		side = 1
		for side*side*4 <= maxTiles {
			side *= 2
		}
	}

	payload := make([]byte, tileBenchPayload)
	for i := range payload {
		payload[i] = byte(i)
	}

	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("%s: begin: %v", eng.name, err)
	}
	stmt, err := tx.Prepare(`INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		b.Fatalf("%s: prepare insert: %v", eng.name, err)
	}
	for col := 0; col < side; col++ {
		for row := 0; row < side; row++ {
			if _, err := stmt.Exec(tileBenchZoom, col, row, payload); err != nil {
				b.Fatalf("%s: insert tile: %v", eng.name, err)
			}
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("%s: commit: %v", eng.name, err)
	}

	// The index is created after loading, which is both faster and how a tileset
	// is actually produced.
	if withIndex && eng.createIdx != "" {
		if _, err := db.Exec(eng.createIdx); err != nil {
			b.Fatalf("%s: create index: %v", eng.name, err)
		}
	}
	return side
}

// benchmarkTileLookup measures the per-request tile fetch.
func benchmarkTileLookup(b *testing.B, eng tileEngine, withIndex bool, maxTiles int) {
	db := eng.open(b)
	defer db.Close()
	side := loadTileset(b, db, eng, withIndex, maxTiles)

	stmt, err := db.Prepare(`SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`)
	if err != nil {
		b.Fatalf("%s: prepare select: %v", eng.name, err)
	}
	defer stmt.Close()

	// A fixed seed keeps the access pattern identical across engines.
	rng := rand.New(rand.NewSource(0x7113))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := rng.Intn(side)
		row := rng.Intn(side)
		var data []byte
		if err := stmt.QueryRow(tileBenchZoom, col, row).Scan(&data); err != nil {
			b.Fatalf("%s: tile %d/%d/%d: %v", eng.name, tileBenchZoom, col, row, err)
		}
		if len(data) != tileBenchPayload {
			b.Fatalf("%s: tile %d/%d/%d returned %d bytes, want %d",
				eng.name, tileBenchZoom, col, row, len(data), tileBenchPayload)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(side*side), "tiles")
}

// BenchmarkTileLookup4k compares a small tileset (4,096 tiles), where a linear
// scan is still cheap enough to hide the absence of an index.
func BenchmarkTileLookup4k(b *testing.B) {
	for _, eng := range tileEngines() {
		b.Run(eng.name, func(b *testing.B) { benchmarkTileLookup(b, eng, true, 4096) })
	}
}

// BenchmarkTileLookup64k compares a full zoom-8 grid (65,536 tiles). This is the
// size at which an index seek and a table scan diverge sharply, and it is small
// for a real tileset — a z0-14 city extract runs to millions of tiles.
func BenchmarkTileLookup64k(b *testing.B) {
	for _, eng := range tileEngines() {
		b.Run(eng.name, func(b *testing.B) { benchmarkTileLookup(b, eng, true, 1<<20) })
	}
}

// BenchmarkTileLookupNoIndex isolates what the index contributes, by running the
// same 64k-tile workload without creating one. The gap against the indexed run is
// the cost a tile server pays if index selection does not fire.
func BenchmarkTileLookupNoIndex(b *testing.B) {
	for _, eng := range tileEngines() {
		b.Run(eng.name, func(b *testing.B) { benchmarkTileLookup(b, eng, false, 1<<20) })
	}
}

// BenchmarkTilesetLoad measures building a tileset, which is what an import or a
// tile-generation pipeline pays once.
func BenchmarkTilesetLoad(b *testing.B) {
	for _, eng := range tileEngines() {
		b.Run(eng.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				func() {
					db := eng.open(b)
					defer db.Close()
					loadTileset(b, db, eng, true, 4096)
				}()
			}
		})
	}
}

// ─────────────────── disk-backed tile serving: the honest case ───────────────
//
// The comparisons above put an in-memory tinySQL against an in-memory SQLite.
// That is not how a tileset is deployed: a navigation device or a tile server
// holds the tiles on disk. tinySQL's answer for that is ModePagedIndex, an
// immutable page store whose complete-composite-equality lookup resolves a
// B+Tree and materializes only the located row — it never decodes the whole
// table, which the legacy ModeIndex/ModeHybrid codec does.
//
// This benchmark measures that path against a SQLite file on the *same* fixture
// and the *same* single-lookup query as BenchmarkTileLookup64k, so the numbers
// are comparable to each other rather than to a differently shaped test.

// buildPagedTileset writes an immutable paged-index artifact holding a tileset,
// then returns the directory. The artifact is produced once, mutably, and served
// read-only, which is how a published tileset is meant to be used.
func buildPagedTileset(b *testing.B, side int) string {
	b.Helper()
	dir := filepath.Join(tmpDir(b), "paged-tiles")
	db, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 256 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	payload := make([]byte, tileBenchPayload)
	for i := range payload {
		payload[i] = byte(i)
	}
	for col := 0; col < side; col++ {
		for row := 0; row < side; row++ {
			// Each tile gets its own slice: the page codec stores what it is given,
			// and sharing one backing array across rows would not represent a real
			// tileset's payloads.
			tile := append([]byte(nil), payload...)
			tile[0] = byte(col)
			tile[1] = byte(row)
			tiles.Rows = append(tiles.Rows, []any{tileBenchZoom, col, row, tile})
		}
	}
	if err := tiles.CreateSecondaryIndex("tile_index",
		[]string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		b.Fatal(err)
	}
	if err := db.Put("default", tiles); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

// BenchmarkTileLookupOnDisk compares disk-backed tile serving: tinySQL's
// immutable page store against a SQLite file.
func BenchmarkTileLookupOnDisk(b *testing.B) {
	const side = 256 // 65,536 tiles, matching BenchmarkTileLookup64k

	b.Run("tinySQL/paged_index", func(b *testing.B) {
		dir := buildPagedTileset(b, side)
		db, err := sql.Open("tinysql",
			"file:"+dir+"?mode=paged_index&read_only=1&max_memory_bytes=32MiB&pool_readers=8")
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			b.Fatal(err)
		}
		stmt, err := db.Prepare(
			`SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		// One pass to fill the bounded page cache, so the measurement is serving
		// latency rather than first-traversal cost.
		var warm []byte
		if err := stmt.QueryRow(tileBenchZoom, 0, 0).Scan(&warm); err != nil {
			b.Fatalf("warm-up: %v", err)
		}

		rng := rand.New(rand.NewSource(0x7113))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var data []byte
			col, row := rng.Intn(side), rng.Intn(side)
			if err := stmt.QueryRow(tileBenchZoom, col, row).Scan(&data); err != nil {
				b.Fatalf("tile %d/%d/%d: %v", tileBenchZoom, col, row, err)
			}
			if len(data) != tileBenchPayload {
				b.Fatalf("tile %d/%d/%d returned %d bytes", tileBenchZoom, col, row, len(data))
			}
		}
	})

	b.Run("SQLite/file", func(b *testing.B) {
		eng := tileEngines()[2] // SQLite/file, WAL + synchronous=NORMAL
		benchmarkTileLookup(b, eng, true, 1<<20)
	})
}
