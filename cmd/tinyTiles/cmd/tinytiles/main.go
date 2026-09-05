//go:build sqliteimport && !js && !wasm && !baremetal

// tinytiles is the command-line entry point for the standalone tinyTiles
// project. It deliberately uses only tinySQL's public artifact API so this
// directory can move to its own repository without importing internal code.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	tinytiles "github.com/Karte-Bayern/tinyTiles"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
	_ "modernc.org/sqlite"
)

func commandImport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(stderr)
	batch := fs.Int("batch", 1_000, "rows per bounded import batch")
	memory := fs.Int64("max-memory", 64<<20, "maximum tinySQL cache budget in bytes")
	reserve := fs.Int64("min-free", 1<<30, "required free disk reserve in bytes")
	schema := fs.String("schema", "auto", "artifact schema: auto, flat, normalized")
	replace := fs.Bool("replace", false, "atomically replace an existing artifact")
	if fs.Parse(args) != nil || fs.NArg() != 2 {
		fmt.Fprintln(stderr, "usage: tinytiles import [flags] source.mbtiles dataset.ttiles/")
		return 2
	}
	artifactSchema, err := parseSchema(*schema)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	result, err := importArtifact(context.Background(), fs.Arg(0), fs.Arg(1), artifactSchema, *batch, *memory, *reserve, *replace, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles import: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "artifact=%s schema=%s\n", result.ArtifactPath, result.Info.Schema)
	return 0
}

func importArtifact(ctx context.Context, source, artifact string, schema tiles.Schema, batch int, memory, reserve int64, replace bool, stdout io.Writer) (*tiles.ImportResult, error) {
	return importArtifactWithProvenance(ctx, source, artifact, schema, batch, memory, reserve, replace, nil, stdout)
}

func importArtifactWithProvenance(ctx context.Context, source, artifact string, schema tiles.Schema, batch int, memory, reserve int64, replace bool, provenance map[string]any, stdout io.Writer) (*tiles.ImportResult, error) {
	start := time.Now()
	result, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{
		Schema: schema, BatchSize: batch, MaxMemoryBytes: memory, MinFreeBytes: reserve, Provenance: provenance, ProgressEvery: time.Second, ReplaceExisting: replace,
		Progress: func(progress tiles.Progress) {
			if progress.Phase == "preflight" && progress.Estimate != nil {
				e := progress.Estimate
				fmt.Fprintf(stdout, "preflight tiles=%d source=%dB estimated-disk=%dB working-set=%dB available-disk=%dB batch=%d\n", e.TileCount, e.SourceBytes, e.EstimatedDisk, e.EstimatedMemory, e.AvailableDisk, e.BatchSize)
			} else if progress.Phase == "published" {
				fmt.Fprintf(stdout, "published tiles=%d batches-of=%d\n", progress.RowsWritten, progress.BatchSize)
			}
		},
	})
	if err == nil {
		fmt.Fprintf(stdout, "import elapsed=%s\n", time.Since(start).Round(time.Millisecond))
	}
	return result, err
}

func commandBenchmark(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.SetOutput(stderr)
	source := fs.String("source", "", "SQLite MBTiles reference")
	artifact := fs.String("artifact", "", "published tinyTiles artifact")
	requests := fs.Int("requests", 512, "number of deterministic warm lookups")
	memory := fs.Int64("max-memory", tinytiles.DefaultReaderMemoryBytes, "per-reader cache budget in bytes")
	readers := fs.Int("readers", 8, "independent readers used for parallel measurements")
	if fs.Parse(args) != nil || *source == "" || *artifact == "" || *requests < 10 || *readers < 1 {
		fmt.Fprintln(stderr, "usage: tinytiles benchmark --source source.mbtiles --artifact dataset.ttiles/ [--requests 512]")
		return 2
	}
	sqliteOpenStart := time.Now()
	db, err := sql.Open("sqlite", "file:"+filepath.Clean(*source)+"?mode=ro&immutable=1")
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: open SQLite: %v\n", err)
		return 1
	}
	defer db.Close()
	db.SetMaxOpenConns(*readers)
	db.SetMaxIdleConns(*readers)
	if err := db.PingContext(context.Background()); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: ping SQLite: %v\n", err)
		return 1
	}
	sqliteOpen := time.Since(sqliteOpenStart)
	benchmarkSource, err := detectBenchmarkSource(db)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: %v\n", err)
		return 1
	}
	corpus, err := benchmarkCorpus(db, benchmarkSource, *requests)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: %v\n", err)
		return 1
	}
	rng := rand.New(rand.NewSource(0x71A5))
	rng.Shuffle(len(corpus), func(i, j int) { corpus[i], corpus[j] = corpus[j], corpus[i] })
	tinyOpenStart := time.Now()
	dataset, err := tinytiles.Open(context.Background(), *artifact, tinytiles.OpenOptions{Readers: *readers, MaxMemoryBytes: *memory})
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: open artifact: %v\n", err)
		return 1
	}
	defer dataset.Close()
	tinyOpen := time.Since(tinyOpenStart)
	stmt, err := db.Prepare(benchmarkSource.lookupSQL)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: prepare SQLite: %v\n", err)
		return 1
	}
	defer stmt.Close()
	sqliteLookup := func(tile benchmarkTile) error {
		var data []byte
		if err := stmt.QueryRow(tile.z, tile.x, tile.y).Scan(&data); err != nil {
			return err
		}
		if len(data) != tile.size {
			return errors.New("SQLite tile length differs from corpus")
		}
		return nil
	}
	tinyLookup := func(tile benchmarkTile) error {
		value, found, err := dataset.LookupTMS(context.Background(), tiles.Key{Z: tile.z, X: tile.x, Y: tile.y})
		if err != nil {
			return err
		}
		if !found || len(value.Data) != tile.size {
			return errors.New("tinyTiles tile length differs from corpus")
		}
		return nil
	}
	if err := verifyBenchmarkParity(corpus, stmt, dataset); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: parity: %v\n", err)
		return 1
	}
	fullTiles, fullMetadata, err := verifyFullBenchmarkParity(db, benchmarkSource, dataset.Info())
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: full parity: %v\n", err)
		return 1
	}
	if err := warm(corpus, sqliteLookup); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: warm SQLite: %v\n", err)
		return 1
	}
	if err := warm(corpus, tinyLookup); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: warm artifact: %v\n", err)
		return 1
	}
	sqliteStats, err := measure(corpus, sqliteLookup)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: SQLite: %v\n", err)
		return 1
	}
	tinyStats, err := measure(corpus, tinyLookup)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: artifact: %v\n", err)
		return 1
	}
	sourceBytes, artifactBytes, sizeErr := benchmarkSizes(*source, *artifact)
	if sizeErr != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: sizes: %v\n", sizeErr)
		return 1
	}
	fmt.Fprintln(stdout, "resource\tSQLite\ttinyTiles")
	fmt.Fprintf(stdout, "open\t%s\t%s\n", sqliteOpen, tinyOpen)
	fmt.Fprintf(stdout, "bytes\t%d\t%d\n", sourceBytes, artifactBytes)
	fmt.Fprintf(stdout, "size-ratio\t1.000\t%.3f\n", float64(artifactBytes)/float64(sourceBytes))
	fmt.Fprintf(stdout, "sample-parity\t%d/%d\tPASS\n", len(corpus), len(corpus))
	fmt.Fprintf(stdout, "full-parity\ttiles=%d metadata=%d\tPASS\n", fullTiles, fullMetadata)
	fmt.Fprintln(stdout, "workload\tbackend\treaders\tp50\tp95\tp99")
	printLatency(stdout, "point", "SQLite", 1, sqliteStats)
	printLatency(stdout, "point", "tinyTiles", 1, tinyStats)
	for _, parallelism := range []int{4, 8} {
		if parallelism > *readers {
			continue
		}
		sqliteParallel, err := measureParallel(corpus, parallelism, sqliteLookup)
		if err != nil {
			fmt.Fprintf(stderr, "tinytiles benchmark: SQLite parallel=%d: %v\n", parallelism, err)
			return 1
		}
		tinyParallel, err := measureParallel(corpus, parallelism, tinyLookup)
		if err != nil {
			fmt.Fprintf(stderr, "tinytiles benchmark: tinyTiles parallel=%d: %v\n", parallelism, err)
			return 1
		}
		printLatency(stdout, "point", "SQLite", parallelism, sqliteParallel)
		printLatency(stdout, "point", "tinyTiles", parallelism, tinyParallel)
	}
	rangeStmt, err := db.Prepare(benchmarkSource.rangeSQL)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: prepare SQLite range: %v\n", err)
		return 1
	}
	defer rangeStmt.Close()
	sqliteRange := sqliteRangeLookup(rangeStmt)
	tinyRange := tinyRangeLookup(dataset)
	if err := warm(corpus, sqliteRange); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: warm SQLite range: %v\n", err)
		return 1
	}
	if err := warm(corpus, tinyRange); err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: warm artifact range: %v\n", err)
		return 1
	}
	sqliteRangeStats, err := measure(corpus, sqliteRange)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: SQLite range: %v\n", err)
		return 1
	}
	tinyRangeStats, err := measure(corpus, tinyRange)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles benchmark: tinyTiles range: %v\n", err)
		return 1
	}
	printLatency(stdout, "spatial-2x2", "SQLite", 1, sqliteRangeStats)
	printLatency(stdout, "spatial-2x2", "tinyTiles", 1, tinyRangeStats)
	if tinyStats.p95 > 2*sqliteStats.p95 {
		fmt.Fprintf(stderr, "tinytiles benchmark: FAIL p95 %s exceeds 2x SQLite %s\n", tinyStats.p95, sqliteStats.p95)
		return 1
	}
	fmt.Fprintln(stdout, "gate\tp95 <= 2x SQLite\tPASS")
	return 0
}

func parseSchema(value string) (tiles.Schema, error) {
	switch strings.ToLower(value) {
	case "auto":
		return tiles.SchemaAuto, nil
	case "flat":
		return tiles.SchemaFlat, nil
	case "normalized":
		return tiles.SchemaNormalized, nil
	default:
		return "", fmt.Errorf("unsupported schema %q (want auto, flat or normalized)", value)
	}
}

type benchmarkTile struct {
	z, x, y int
	size    int
}
type latencyStats struct{ p50, p95, p99 time.Duration }

type sqliteBenchmarkSource struct {
	zoomLevelsSQL  string
	tilesAtZoomSQL string
	lookupSQL      string
	rangeSQL       string
	fullTilesSQL   string
}

func detectBenchmarkSource(db *sql.DB) (sqliteBenchmarkSource, error) {
	flat, err := sqliteTableExists(db, "tiles")
	if err != nil {
		return sqliteBenchmarkSource{}, err
	}
	if flat {
		return sqliteBenchmarkSource{
			zoomLevelsSQL:  `SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level`,
			tilesAtZoomSQL: `SELECT zoom_level,tile_column,tile_row,length(tile_data) FROM tiles WHERE zoom_level=? ORDER BY tile_column,tile_row LIMIT ?`,
			lookupSQL:      `SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?`,
			rangeSQL:       `SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles WHERE zoom_level=? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?`,
			fullTilesSQL:   `SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles ORDER BY zoom_level,tile_column,tile_row`,
		}, nil
	}
	mapTable, err := sqliteTableExists(db, "map")
	if err != nil {
		return sqliteBenchmarkSource{}, err
	}
	images, err := sqliteTableExists(db, "images")
	if err != nil {
		return sqliteBenchmarkSource{}, err
	}
	if mapTable && images {
		return sqliteBenchmarkSource{
			zoomLevelsSQL:  `SELECT DISTINCT zoom_level FROM map ORDER BY zoom_level`,
			tilesAtZoomSQL: `SELECT m.zoom_level,m.tile_column,m.tile_row,length(i.tile_data) FROM map AS m JOIN images AS i ON i.tile_id=m.tile_id WHERE m.zoom_level=? ORDER BY m.tile_column,m.tile_row LIMIT ?`,
			lookupSQL:      `SELECT i.tile_data FROM map AS m JOIN images AS i ON i.tile_id=m.tile_id WHERE m.zoom_level=? AND m.tile_column=? AND m.tile_row=?`,
			rangeSQL:       `SELECT m.zoom_level,m.tile_column,m.tile_row,i.tile_data FROM map AS m JOIN images AS i ON i.tile_id=m.tile_id WHERE m.zoom_level=? AND m.tile_column BETWEEN ? AND ? AND m.tile_row BETWEEN ? AND ?`,
			fullTilesSQL:   `SELECT m.zoom_level,m.tile_column,m.tile_row,i.tile_data FROM map AS m JOIN images AS i ON i.tile_id=m.tile_id ORDER BY m.zoom_level,m.tile_column,m.tile_row`,
		}, nil
	}
	return sqliteBenchmarkSource{}, errors.New("source has neither flat tiles nor normalized map/images schema")
}

func sqliteTableExists(db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect source schema: %w", err)
	}
	return true, nil
}

func benchmarkCorpus(db *sql.DB, source sqliteBenchmarkSource, count int) ([]benchmarkTile, error) {
	levels, err := db.Query(source.zoomLevelsSQL)
	if err != nil {
		return nil, err
	}
	defer levels.Close()
	var zooms []int
	for levels.Next() {
		var z int
		if err := levels.Scan(&z); err != nil {
			return nil, err
		}
		zooms = append(zooms, z)
	}
	if err := levels.Err(); err != nil {
		return nil, err
	}
	if len(zooms) == 0 {
		return nil, errors.New("source has no tiles")
	}
	perZoom := (count + len(zooms) - 1) / len(zooms)
	corpus := make([]benchmarkTile, 0, perZoom*len(zooms))
	for _, z := range zooms {
		rows, err := db.Query(source.tilesAtZoomSQL, z, perZoom)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tile benchmarkTile
			if err := rows.Scan(&tile.z, &tile.x, &tile.y, &tile.size); err != nil {
				_ = rows.Close()
				return nil, err
			}
			corpus = append(corpus, tile)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	if len(corpus) == 0 {
		return nil, errors.New("source has no readable tiles")
	}
	return corpus, nil
}

func warm(corpus []benchmarkTile, lookup func(benchmarkTile) error) error {
	for _, tile := range corpus {
		if err := lookup(tile); err != nil {
			return err
		}
	}
	return nil
}
func measure(corpus []benchmarkTile, lookup func(benchmarkTile) error) (latencyStats, error) {
	latencies := make([]time.Duration, 0, len(corpus))
	for _, tile := range corpus {
		start := time.Now()
		if err := lookup(tile); err != nil {
			return latencyStats{}, err
		}
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencyStats{latencies[(len(latencies)-1)*50/100], latencies[(len(latencies)-1)*95/100], latencies[(len(latencies)-1)*99/100]}, nil
}

func measureParallel(corpus []benchmarkTile, workers int, lookup func(benchmarkTile) error) (latencyStats, error) {
	if workers < 1 {
		return latencyStats{}, errors.New("parallel worker count must be positive")
	}
	type job struct {
		index int
		tile  benchmarkTile
	}
	jobs := make(chan job, workers*2)
	latencies := make([]time.Duration, len(corpus))
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for current := range jobs {
				start := time.Now()
				if err := lookup(current.tile); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				latencies[current.index] = time.Since(start)
			}
		}()
	}
	for index, tile := range corpus {
		jobs <- job{index: index, tile: tile}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return latencyStats{}, firstErr
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencyStats{latencies[(len(latencies)-1)*50/100], latencies[(len(latencies)-1)*95/100], latencies[(len(latencies)-1)*99/100]}, nil
}

func verifyBenchmarkParity(corpus []benchmarkTile, stmt *sql.Stmt, dataset *tinytiles.Dataset) error {
	ctx := context.Background()
	for _, request := range corpus {
		var sqliteData []byte
		if err := stmt.QueryRowContext(ctx, request.z, request.x, request.y).Scan(&sqliteData); err != nil {
			return err
		}
		tile, found, err := dataset.LookupTMS(ctx, tiles.Key{Z: request.z, X: request.x, Y: request.y})
		if err != nil {
			return err
		}
		if !found || !bytes.Equal(sqliteData, tile.Data) {
			return fmt.Errorf("tile bytes differ at %d/%d/%d", request.z, request.x, request.y)
		}
	}
	return nil
}

func verifyFullBenchmarkParity(db *sql.DB, source sqliteBenchmarkSource, info tiles.ArtifactInfo) (int64, int64, error) {
	rows, err := db.Query(source.fullTilesSQL)
	if err != nil {
		return 0, 0, err
	}
	tileHash := sha256.New()
	var tileCount int64
	var previous [3]int
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		key := [3]int{z, x, y}
		if tileCount > 0 && key == previous {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("duplicate source tile key %d/%d/%d", z, x, y)
		}
		previous = key
		hashBenchmarkTile(tileHash, z, x, y, data)
		tileCount++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if got := hex.EncodeToString(tileHash.Sum(nil)); got != info.TileDigestSHA256 {
		return 0, 0, fmt.Errorf("complete source tile digest %s differs from artifact %s", got, info.TileDigestSHA256)
	}
	artifactTileRows := artifactRows(info, "tiles")
	if info.Schema == tiles.SchemaNormalized {
		artifactTileRows = artifactRows(info, "map")
	}
	if artifactTileRows != tileCount {
		return 0, 0, fmt.Errorf("source has %d tile keys, artifact declares %d", tileCount, artifactTileRows)
	}

	metadataHash := sha256.New()
	var metadataCount int64
	metadataExists, err := sqliteTableExists(db, "metadata")
	if err != nil {
		return 0, 0, err
	}
	if metadataExists {
		metadataRows, err := db.Query(`SELECT name,value FROM metadata ORDER BY name`)
		if err != nil {
			return 0, 0, err
		}
		var previousName string
		for metadataRows.Next() {
			var name, value sql.NullString
			if err := metadataRows.Scan(&name, &value); err != nil {
				_ = metadataRows.Close()
				return 0, 0, err
			}
			if !name.Valid || !value.Valid {
				_ = metadataRows.Close()
				return 0, 0, errors.New("source metadata contains NULL name/value")
			}
			if metadataCount > 0 && name.String == previousName {
				_ = metadataRows.Close()
				return 0, 0, fmt.Errorf("duplicate source metadata name %q", name.String)
			}
			previousName = name.String
			hashBenchmarkMetadata(metadataHash, name.String, value.String)
			metadataCount++
		}
		if err := metadataRows.Err(); err != nil {
			_ = metadataRows.Close()
			return 0, 0, err
		}
		if err := metadataRows.Close(); err != nil {
			return 0, 0, err
		}
	}
	if got := hex.EncodeToString(metadataHash.Sum(nil)); got != info.MetadataDigestSHA256 {
		return 0, 0, fmt.Errorf("complete source metadata digest %s differs from artifact %s", got, info.MetadataDigestSHA256)
	}
	if artifactRows(info, "metadata") != metadataCount {
		return 0, 0, fmt.Errorf("source has %d metadata rows, artifact declares %d", metadataCount, artifactRows(info, "metadata"))
	}
	return tileCount, metadataCount, nil
}

func artifactRows(info tiles.ArtifactInfo, name string) int64 {
	for _, table := range info.Tables {
		if table.Name == name {
			return table.Rows
		}
	}
	return -1
}

func hashBenchmarkTile(dst io.Writer, z, x, y int, data []byte) {
	var buffer [8]byte
	for _, value := range []int{z, x, y} {
		binary.BigEndian.PutUint64(buffer[:], uint64(int64(value)))
		_, _ = dst.Write(buffer[:])
	}
	binary.BigEndian.PutUint64(buffer[:], uint64(len(data)))
	_, _ = dst.Write(buffer[:])
	_, _ = dst.Write(data)
}

func hashBenchmarkMetadata(dst io.Writer, name, value string) {
	var buffer [4]byte
	binary.BigEndian.PutUint32(buffer[:], uint32(len(name)))
	_, _ = dst.Write(buffer[:])
	_, _ = io.WriteString(dst, name)
	binary.BigEndian.PutUint32(buffer[:], uint32(len(value)))
	_, _ = dst.Write(buffer[:])
	_, _ = io.WriteString(dst, value)
}

func sqliteRangeLookup(stmt *sql.Stmt) func(benchmarkTile) error {
	return func(request benchmarkTile) error {
		xMax, yMax := benchmarkRange(request)
		rows, err := stmt.Query(request.z, request.x, xMax, request.y, yMax)
		if err != nil {
			return err
		}
		defer rows.Close()
		matched := false
		for rows.Next() {
			var z, x, y int
			var data []byte
			if err := rows.Scan(&z, &x, &y, &data); err != nil {
				return err
			}
			if z == request.z && x == request.x && y == request.y && len(data) == request.size {
				matched = true
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !matched {
			return errors.New("SQLite spatial lookup omitted requested tile")
		}
		return nil
	}
}

func tinyRangeLookup(dataset *tinytiles.Dataset) func(benchmarkTile) error {
	return func(request benchmarkTile) error {
		xMax, yMax := benchmarkRange(request)
		matched := false
		err := dataset.ScanTMS(context.Background(), tiles.Range{Z: request.z, XMin: request.x, XMax: xMax, YMin: request.y, YMax: yMax}, func(tile tiles.Tile) error {
			if tile.Key.Z == request.z && tile.Key.X == request.x && tile.Key.Y == request.y && len(tile.Data) == request.size {
				matched = true
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !matched {
			return errors.New("tinyTiles spatial lookup omitted requested tile")
		}
		return nil
	}
}

func benchmarkRange(tile benchmarkTile) (xMax, yMax int) {
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

func benchmarkSizes(source, artifact string) (int64, int64, error) {
	info, err := os.Stat(source)
	if err != nil {
		return 0, 0, err
	}
	var artifactBytes int64
	err = filepath.WalkDir(artifact, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			fileInfo, err := entry.Info()
			if err != nil {
				return err
			}
			artifactBytes += fileInfo.Size()
		}
		return nil
	})
	return info.Size(), artifactBytes, err
}

func printLatency(output io.Writer, workload, backend string, readers int, stats latencyStats) {
	fmt.Fprintf(output, "%s\t%s\t%d\t%s\t%s\t%s\n", workload, backend, readers, stats.p50, stats.p95, stats.p99)
}
