//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const mbtilesArtifactFormatVersion = 1

type artifactSourceSchema string

const (
	artifactSourceFlat       artifactSourceSchema = "flat"
	artifactSourceNormalized artifactSourceSchema = "normalized"
)

// ImportMBTilesArtifact builds a bounded paged-index database in a temporary
// sibling directory and publishes it only after checksums and logical index
// validation succeed. The source is opened read-only and is never modified.
func ImportMBTilesArtifact(ctx context.Context, sourcePath, artifactPath string, opts *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	if opts == nil {
		opts = &MBTilesArtifactOptions{}
	}
	opts = cloneArtifactOptions(opts)
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.MaxMemoryBytes <= 0 {
		opts.MaxMemoryBytes = 64 << 20
	}
	if opts.ProgressEvery <= 0 {
		opts.ProgressEvery = 250 * time.Millisecond
	}
	if opts.Schema == "" {
		opts.Schema = MBTilesSchemaAuto
	}
	if opts.Schema != MBTilesSchemaAuto && opts.Schema != MBTilesSchemaFlat && opts.Schema != MBTilesSchemaNormalized {
		return nil, fmt.Errorf("unsupported MBTiles artifact schema %q", opts.Schema)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	parent := filepath.Dir(filepath.Clean(artifactPath))
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact parent: %w", err)
	}

	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("stat MBTiles source: %w", err)
	}
	src, err := sql.Open("sqlite", "file:"+sourcePath+"?mode=ro&immutable=1")
	if err != nil {
		return nil, fmt.Errorf("open MBTiles source: %w", err)
	}
	defer src.Close()
	if err := src.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping MBTiles source: %w", err)
	}

	sourceSchema, err := detectArtifactSourceSchema(ctx, src)
	if err != nil {
		return nil, err
	}
	estimate, err := estimateArtifactResources(ctx, src, sourceSchema, info.Size(), filepath.Dir(artifactPath), opts)
	if err != nil {
		return nil, err
	}
	if opts.Progress != nil {
		opts.Progress(MBTilesProgress{Phase: "preflight", TotalRows: estimate.TileCount, BatchSize: estimate.BatchSize, Estimate: &estimate})
	}
	if estimate.EstimatedMemory > opts.MaxMemoryBytes {
		return nil, fmt.Errorf("insufficient memory for MBTiles import: need %d bytes, limit %d bytes", estimate.EstimatedMemory, opts.MaxMemoryBytes)
	}
	if estimate.AvailableDisk >= 0 && estimate.AvailableDisk < estimate.EstimatedDisk+opts.MinFreeBytes {
		return nil, fmt.Errorf("insufficient disk space for MBTiles import: need %d bytes plus reserve %d, available %d", estimate.EstimatedDisk, opts.MinFreeBytes, estimate.AvailableDisk)
	}

	if _, err := os.Stat(artifactPath); err == nil && !opts.ReplaceExisting {
		return nil, fmt.Errorf("artifact already exists: %s", artifactPath)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect artifact target: %w", err)
	}
	tmp, err := os.MkdirTemp(parent, ".tinysql-artifact-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary artifact: %w", err)
	}
	defer os.RemoveAll(tmp)
	for _, dir := range []string{"database", "indexes"} {
		if err := os.Mkdir(filepath.Join(tmp, dir), 0o755); err != nil {
			return nil, fmt.Errorf("create artifact directory %s: %w", dir, err)
		}
	}

	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: filepath.Join(tmp, "database"), MaxMemoryBytes: opts.MaxMemoryBytes})
	if err != nil {
		return nil, fmt.Errorf("create paged-index database: %w", err)
	}
	closeDB := true
	defer func() {
		if closeDB {
			_ = db.Close()
		}
	}()

	metadataRows, metadataDigest, err := readArtifactMetadata(ctx, src)
	if err != nil {
		return nil, err
	}
	destinationSchema := opts.Schema
	if destinationSchema == MBTilesSchemaAuto {
		if sourceSchema == artifactSourceFlat {
			destinationSchema = MBTilesSchemaFlat
		} else {
			destinationSchema = MBTilesSchemaNormalized
		}
	}
	if destinationSchema == MBTilesSchemaFlat {
		if err := createArtifactTables(db, MBTilesSchemaFlat); err != nil {
			return nil, err
		}
	} else if err := createArtifactTables(db, MBTilesSchemaNormalized); err != nil {
		return nil, err
	}
	if err := appendArtifactRows(ctx, db, "default", "metadata", metadataRows, opts.BatchSize, nil, opts); err != nil {
		return nil, err
	}

	var tileDigest = sha256.New()
	progress := &artifactProgress{opts: opts, total: estimate.TileCount, batchSize: opts.BatchSize}
	switch {
	case sourceSchema == artifactSourceFlat && destinationSchema == MBTilesSchemaFlat:
		err = importFlatToFlat(ctx, src, db, progress, tileDigest)
	case sourceSchema == artifactSourceFlat && destinationSchema == MBTilesSchemaNormalized:
		err = importFlatToNormalized(ctx, src, db, progress, tileDigest)
	case sourceSchema == artifactSourceNormalized && destinationSchema == MBTilesSchemaNormalized:
		err = importNormalizedToNormalized(ctx, src, db, progress, tileDigest)
	case sourceSchema == artifactSourceNormalized && destinationSchema == MBTilesSchemaFlat:
		err = importNormalizedToFlat(ctx, src, db, progress, tileDigest)
	}
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close imported database: %w", err)
	}
	closeDB = false

	indexConfig := artifactIndexConfig(destinationSchema)
	if err := writeJSONFile(filepath.Join(tmp, "indexes", "config.json"), indexConfig); err != nil {
		return nil, err
	}
	manifest := MBTilesArtifactManifest{
		FormatVersion:  mbtilesArtifactFormatVersion,
		TinySQLVersion: "v0.27",
		Schema:         destinationSchema,
		CreatedAt:      time.Now().UTC(),
		Source:         filepath.Base(sourcePath),
		SourceBytes:    info.Size(),
		Resources:      estimate,
		Tables:         artifactTableManifest(destinationSchema, estimate.TileCount, estimate.ImageRows, int64(len(metadataRows))),
		IndexConfig:    indexConfig,
		Checksums:      map[string]string{},
	}
	manifest.Checksums, err = checksumFiles(tmp, artifactDataFileNames(tmp))
	if err != nil {
		return nil, err
	}
	manifest.IndexConfig["tile_digest_sha256"] = hex.EncodeToString(tileDigest.Sum(nil))
	manifest.IndexConfig["metadata_digest_sha256"] = metadataDigest
	if err := writeJSONFile(filepath.Join(tmp, "manifest.json"), manifest); err != nil {
		return nil, err
	}
	if err := writeChecksumsFile(tmp); err != nil {
		return nil, err
	}
	if _, err := validateArtifact(ctx, tmp, false); err != nil {
		return nil, fmt.Errorf("validate temporary artifact: %w", err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "COMPLETE"), []byte(""), 0o644); err != nil {
		return nil, fmt.Errorf("write completion marker: %w", err)
	}
	if _, err := validateArtifact(ctx, tmp, true); err != nil {
		return nil, fmt.Errorf("validate completed artifact: %w", err)
	}
	if err := syncArtifactTree(tmp); err != nil {
		return nil, err
	}
	if err := publishArtifact(tmp, artifactPath, opts.ReplaceExisting); err != nil {
		return nil, err
	}
	if opts.Progress != nil {
		opts.Progress(MBTilesProgress{Phase: "published", RowsRead: estimate.TileCount, RowsWritten: estimate.TileCount, TotalRows: estimate.TileCount, BatchSize: opts.BatchSize})
	}
	return &MBTilesArtifactResult{ArtifactPath: artifactPath, Manifest: manifest, Estimate: estimate}, nil
}

func cloneArtifactOptions(in *MBTilesArtifactOptions) *MBTilesArtifactOptions {
	out := *in
	return &out
}

func detectArtifactSourceSchema(ctx context.Context, db *sql.DB) (artifactSourceSchema, error) {
	flat, err := artifactTableExists(ctx, db, "tiles")
	if err != nil {
		return "", err
	}
	normMap, err := artifactTableExists(ctx, db, "map")
	if err != nil {
		return "", err
	}
	normImages, err := artifactTableExists(ctx, db, "images")
	if err != nil {
		return "", err
	}
	if flat {
		return artifactSourceFlat, nil
	}
	if normMap && normImages {
		return artifactSourceNormalized, nil
	}
	return "", errors.New("MBTiles source has neither tiles nor map/images schema")
}

func artifactTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect MBTiles schema: %w", err)
	}
	return true, nil
}

func estimateArtifactResources(ctx context.Context, db *sql.DB, schema artifactSourceSchema, sourceBytes int64, diskPath string, opts *MBTilesArtifactOptions) (MBTilesResourceEstimate, error) {
	var rows, maxTile, totalTileBytes int64
	var query string
	if schema == artifactSourceFlat {
		query = "SELECT COUNT(*), COALESCE(MAX(length(tile_data)),0), COALESCE(SUM(length(tile_data)),0) FROM tiles"
	} else {
		query = "SELECT COUNT(*), COALESCE(MAX(length(tile_data)),0), COALESCE(SUM(length(tile_data)),0) FROM images"
	}
	if err := db.QueryRowContext(ctx, query).Scan(&rows, &maxTile, &totalTileBytes); err != nil {
		return MBTilesResourceEstimate{}, fmt.Errorf("estimate MBTiles resources: %w", err)
	}
	mapRows, imageRows := rows, rows
	if schema == artifactSourceNormalized {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM map").Scan(&mapRows); err != nil {
			return MBTilesResourceEstimate{}, fmt.Errorf("count normalized MBTiles map: %w", err)
		}
	}
	var metadataRows int64
	if exists, err := artifactTableExists(ctx, db, "metadata"); err != nil {
		return MBTilesResourceEstimate{}, err
	} else if exists {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM metadata").Scan(&metadataRows); err != nil {
			return MBTilesResourceEstimate{}, fmt.Errorf("count MBTiles metadata: %w", err)
		}
	}
	batch := opts.BatchSize
	memory := maxTile*int64(batch) + int64(batch)*768 + 2*int64(1<<20)
	disk := sourceBytes + totalTileBytes + rows*320 + int64(8<<20)
	return MBTilesResourceEstimate{
		SourceBytes: sourceBytes, TileCount: mapRows, MapRows: mapRows, ImageRows: imageRows, MetadataRows: metadataRows,
		EstimatedMemory: memory, EstimatedDisk: disk,
		AvailableDisk: availableDiskBytes(diskPath), BatchSize: batch,
	}, nil
}

func createArtifactTables(db *storage.DB, schema MBTilesArtifactSchema) error {
	metadata := storage.NewTable("metadata", []storage.Column{{Name: "name", Type: storage.TextType}, {Name: "value", Type: storage.TextType}}, false)
	if err := metadata.CreateSecondaryIndex("metadata_name", []string{"name"}, true); err != nil {
		return err
	}
	if err := db.Put("default", metadata); err != nil {
		return fmt.Errorf("create metadata table: %w", err)
	}
	if schema == MBTilesSchemaFlat {
		tiles := storage.NewTable("tiles", []storage.Column{{Name: "z", Type: storage.IntType}, {Name: "x", Type: storage.IntType}, {Name: "y", Type: storage.IntType}, {Name: "tile_data", Type: storage.BlobType}}, false)
		if err := tiles.CreateSecondaryIndex("tiles_zxy", []string{"z", "x", "y"}, true); err != nil {
			return err
		}
		return db.Put("default", tiles)
	}
	mapTable := storage.NewTable("map", []storage.Column{{Name: "z", Type: storage.IntType}, {Name: "x", Type: storage.IntType}, {Name: "y", Type: storage.IntType}, {Name: "tile_id", Type: storage.TextType}}, false)
	if err := mapTable.CreateSecondaryIndex("map_zxy", []string{"z", "x", "y"}, true); err != nil {
		return err
	}
	if err := db.Put("default", mapTable); err != nil {
		return fmt.Errorf("create map table: %w", err)
	}
	images := storage.NewTable("images", []storage.Column{{Name: "tile_id", Type: storage.TextType}, {Name: "tile_data", Type: storage.BlobType}}, false)
	if err := images.CreateSecondaryIndex("images_tile_id", []string{"tile_id"}, true); err != nil {
		return err
	}
	if err := db.Put("default", images); err != nil {
		return fmt.Errorf("create images table: %w", err)
	}
	return nil
}

func readArtifactMetadata(ctx context.Context, db *sql.DB) ([][]any, string, error) {
	rows := [][]any{}
	h := sha256.New()
	exists, err := artifactTableExists(ctx, db, "metadata")
	if err != nil || !exists {
		return rows, hex.EncodeToString(h.Sum(nil)), err
	}
	sqlRows, err := db.QueryContext(ctx, "SELECT name,value FROM metadata ORDER BY name")
	if err != nil {
		return nil, "", fmt.Errorf("read MBTiles metadata: %w", err)
	}
	defer sqlRows.Close()
	for sqlRows.Next() {
		var name, value sql.NullString
		if err := sqlRows.Scan(&name, &value); err != nil {
			return nil, "", fmt.Errorf("scan MBTiles metadata: %w", err)
		}
		if !name.Valid || !value.Valid {
			return nil, "", errors.New("MBTiles metadata contains NULL name/value")
		}
		rows = append(rows, []any{name.String, value.String})
		hashMetadata(h, name.String, value.String)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, "", fmt.Errorf("read MBTiles metadata: %w", err)
	}
	return rows, hex.EncodeToString(h.Sum(nil)), nil
}

func hashMetadata(h io.Writer, name, value string) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(name)))
	_, _ = h.Write(b[:])
	_, _ = io.WriteString(h, name)
	binary.BigEndian.PutUint32(b[:], uint32(len(value)))
	_, _ = h.Write(b[:])
	_, _ = io.WriteString(h, value)
}

func hashTile(h io.Writer, z, x, y int, data []byte) {
	var b [8]byte
	for _, v := range []int{z, x, y} {
		binary.BigEndian.PutUint64(b[:], uint64(int64(v)))
		_, _ = h.Write(b[:])
	}
	binary.BigEndian.PutUint64(b[:], uint64(len(data)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(data)
}

func validateTileCoordinate(z, x, y int) error {
	if z < 0 || z > 30 {
		return fmt.Errorf("invalid MBTiles zoom %d", z)
	}
	limit := 1 << z
	if x < 0 || y < 0 || x >= limit || y >= limit {
		return fmt.Errorf("invalid TMS tile coordinate z=%d x=%d y=%d", z, x, y)
	}
	return nil
}

type artifactProgress struct {
	opts                                          *MBTilesArtifactOptions
	total, read, written, bytesRead, bytesWritten int64
	batchSize                                     int
	lastEmit                                      time.Time
}

func (p *artifactProgress) emit(phase string) {
	now := time.Now()
	if phase == "import" && !p.lastEmit.IsZero() && now.Sub(p.lastEmit) < p.opts.ProgressEvery && p.written < p.total {
		return
	}
	if p.opts.Progress != nil {
		p.opts.Progress(MBTilesProgress{Phase: phase, RowsRead: p.read, RowsWritten: p.written, BytesRead: p.bytesRead, BytesWritten: p.bytesWritten, TotalRows: p.total, BatchSize: p.batchSize})
	}
	p.lastEmit = now
}

func appendArtifactRows(ctx context.Context, db *storage.DB, tenant, table string, rows [][]any, batchSize int, progress *artifactProgress, opts *MBTilesArtifactOptions) error {
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		ok, err := db.AppendRowsFast(tenant, table, rows[start:end])
		if err != nil {
			return fmt.Errorf("append %s: %w", table, err)
		}
		if !ok {
			return fmt.Errorf("append %s: paged index fast path unavailable", table)
		}
		if progress != nil {
			progress.written += int64(end - start)
			for _, row := range rows[start:end] {
				if len(row) > 3 {
					if data, ok := row[len(row)-1].([]byte); ok {
						progress.bytesWritten += int64(len(data))
					}
				}
			}
			progress.emit("import")
		}
		// db.Put caches the empty schema in DB.tenants. AppendRowsFast writes the
		// authoritative pager directly; discard that stale compatibility cache so
		// DB.Close cannot flush the empty snapshot over the completed index.
		db.DiscardCachedTable(tenant, table)
	}
	_ = opts
	return nil
}

func importFlatToFlat(ctx context.Context, src *sql.DB, db *storage.DB, p *artifactProgress, digest io.Writer) error {
	rows, err := src.QueryContext(ctx, "SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles ORDER BY zoom_level,tile_column,tile_row")
	if err != nil {
		return fmt.Errorf("query flat MBTiles: %w", err)
	}
	defer rows.Close()
	batch := make([][]any, 0, p.batchSize)
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			return err
		}
		if err := validateTileCoordinate(z, x, y); err != nil {
			return err
		}
		hashTile(digest, z, x, y, data)
		p.read++
		p.bytesRead += int64(len(data))
		batch = append(batch, []any{z, x, y, data})
		if len(batch) == p.batchSize {
			if err := appendArtifactRows(ctx, db, "default", "tiles", batch, p.batchSize, p, nil); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return appendArtifactRows(ctx, db, "default", "tiles", batch, p.batchSize, p, nil)
}

func importFlatToNormalized(ctx context.Context, src *sql.DB, db *storage.DB, p *artifactProgress, digest io.Writer) error {
	rows, err := src.QueryContext(ctx, "SELECT zoom_level,tile_column,tile_row,tile_data FROM tiles ORDER BY zoom_level,tile_column,tile_row")
	if err != nil {
		return fmt.Errorf("query flat MBTiles: %w", err)
	}
	defer rows.Close()
	mapRows := make([][]any, 0, p.batchSize)
	imageRows := make([][]any, 0, p.batchSize)
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			return err
		}
		if err := validateTileCoordinate(z, x, y); err != nil {
			return err
		}
		id := fmt.Sprintf("%d/%d/%d", z, x, y)
		hashTile(digest, z, x, y, data)
		p.read++
		p.bytesRead += int64(len(data))
		mapRows = append(mapRows, []any{z, x, y, id})
		imageRows = append(imageRows, []any{id, data})
		if len(mapRows) == p.batchSize {
			if err := appendArtifactRows(ctx, db, "default", "map", mapRows, p.batchSize, p, nil); err != nil {
				return err
			}
			if err := appendArtifactRows(ctx, db, "default", "images", imageRows, p.batchSize, nil, nil); err != nil {
				return err
			}
			mapRows = mapRows[:0]
			imageRows = imageRows[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := appendArtifactRows(ctx, db, "default", "map", mapRows, p.batchSize, p, nil); err != nil {
		return err
	}
	return appendArtifactRows(ctx, db, "default", "images", imageRows, p.batchSize, nil, nil)
}

func importNormalizedToNormalized(ctx context.Context, src *sql.DB, db *storage.DB, p *artifactProgress, digest io.Writer) error {
	rows, err := src.QueryContext(ctx, "SELECT m.zoom_level,m.tile_column,m.tile_row,m.tile_id,i.tile_data FROM map m JOIN images i ON i.tile_id=m.tile_id ORDER BY m.zoom_level,m.tile_column,m.tile_row")
	if err != nil {
		return fmt.Errorf("query normalized MBTiles: %w", err)
	}
	defer rows.Close()
	batch := make([][]any, 0, p.batchSize)
	for rows.Next() {
		var z, x, y int
		var id string
		var data []byte
		if err := rows.Scan(&z, &x, &y, &id, &data); err != nil {
			return err
		}
		if err := validateTileCoordinate(z, x, y); err != nil {
			return err
		}
		hashTile(digest, z, x, y, data)
		p.read++
		p.bytesRead += int64(len(data))
		batch = append(batch, []any{z, x, y, id})
		if len(batch) == p.batchSize {
			if err := appendArtifactRows(ctx, db, "default", "map", batch, p.batchSize, p, nil); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := appendArtifactRows(ctx, db, "default", "map", batch, p.batchSize, p, nil); err != nil {
		return err
	}
	return importNormalizedImages(ctx, src, db, p)
}

func importNormalizedToFlat(ctx context.Context, src *sql.DB, db *storage.DB, p *artifactProgress, digest io.Writer) error {
	rows, err := src.QueryContext(ctx, "SELECT m.zoom_level,m.tile_column,m.tile_row,i.tile_data FROM map m JOIN images i ON i.tile_id=m.tile_id ORDER BY m.zoom_level,m.tile_column,m.tile_row")
	if err != nil {
		return fmt.Errorf("query normalized MBTiles: %w", err)
	}
	defer rows.Close()
	batch := make([][]any, 0, p.batchSize)
	for rows.Next() {
		var z, x, y int
		var data []byte
		if err := rows.Scan(&z, &x, &y, &data); err != nil {
			return err
		}
		if err := validateTileCoordinate(z, x, y); err != nil {
			return err
		}
		hashTile(digest, z, x, y, data)
		p.read++
		p.bytesRead += int64(len(data))
		batch = append(batch, []any{z, x, y, data})
		if len(batch) == p.batchSize {
			if err := appendArtifactRows(ctx, db, "default", "tiles", batch, p.batchSize, p, nil); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return appendArtifactRows(ctx, db, "default", "tiles", batch, p.batchSize, p, nil)
}

func importNormalizedImages(ctx context.Context, src *sql.DB, db *storage.DB, p *artifactProgress) error {
	rows, err := src.QueryContext(ctx, "SELECT tile_id,tile_data FROM images ORDER BY tile_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	batch := make([][]any, 0, p.batchSize)
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return err
		}
		batch = append(batch, []any{id, data})
		if len(batch) == p.batchSize {
			if err := appendArtifactRows(ctx, db, "default", "images", batch, p.batchSize, nil, nil); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return appendArtifactRows(ctx, db, "default", "images", batch, p.batchSize, nil, nil)
}

func artifactIndexConfig(schema MBTilesArtifactSchema) map[string]any {
	if schema == MBTilesSchemaFlat {
		return map[string]any{"tiles": "tiles_zxy", "coordinate_system": "TMS"}
	}
	return map[string]any{"map": "map_zxy", "images": "images_tile_id", "metadata": "metadata_name", "coordinate_system": "TMS"}
}
func artifactTableManifest(schema MBTilesArtifactSchema, tiles, images, metadata int64) []MBTilesArtifactTable {
	out := []MBTilesArtifactTable{{Name: "metadata", Columns: []string{"name", "value"}, Rows: metadata, Indexes: []MBTilesArtifactIndex{{Name: "metadata_name", Columns: []string{"name"}, Unique: true}}}}
	if schema == MBTilesSchemaFlat {
		return append(out, MBTilesArtifactTable{Name: "tiles", Columns: []string{"z", "x", "y", "tile_data"}, Rows: tiles, Indexes: []MBTilesArtifactIndex{{Name: "tiles_zxy", Columns: []string{"z", "x", "y"}, Unique: true}}})
	}
	return append(out, MBTilesArtifactTable{Name: "map", Columns: []string{"z", "x", "y", "tile_id"}, Rows: tiles, Indexes: []MBTilesArtifactIndex{{Name: "map_zxy", Columns: []string{"z", "x", "y"}, Unique: true}}}, MBTilesArtifactTable{Name: "images", Columns: []string{"tile_id", "tile_data"}, Rows: images, Indexes: []MBTilesArtifactIndex{{Name: "images_tile_id", Columns: []string{"tile_id"}, Unique: true}}})
}

func writeJSONFile(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
func artifactDataFileNames(root string) []string {
	var names []string
	for _, dir := range []string{"database", "indexes"} {
		_ = filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() {
				rel, relErr := filepath.Rel(root, path)
				if relErr == nil {
					names = append(names, filepath.ToSlash(rel))
				}
			}
			return nil
		})
	}
	sort.Strings(names)
	return names
}

func checksumFiles(root string, names []string) (map[string]string, error) {
	out := make(map[string]string, len(names))
	for _, name := range names {
		sum, err := checksumFile(filepath.Join(root, name))
		if err != nil {
			return nil, fmt.Errorf("checksum %s: %w", name, err)
		}
		out[name] = sum
	}
	return out, nil
}

func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func writeChecksumsFile(root string) error {
	names := append([]string{"manifest.json"}, artifactDataFileNames(root)...)
	sort.Strings(names)
	f, err := os.OpenFile(filepath.Join(root, "checksums.sha256"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, name := range names {
		sum, err := checksumFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(f, "%s  %s\n", sum, name); err != nil {
			return err
		}
	}
	return f.Sync()
}
func syncArtifactTree(root string) error {
	names := append([]string{"manifest.json"}, artifactDataFileNames(root)...)
	names = append(names, "checksums.sha256", "COMPLETE")
	for _, name := range names {
		f, err := os.OpenFile(filepath.Join(root, name), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func publishArtifact(tmp, target string, replace bool) error {
	if !replace {
		return os.Rename(tmp, target)
	}
	backup := target + ".rollback"
	_ = os.RemoveAll(backup)
	if _, err := os.Lstat(target); err == nil {
		if err := os.Rename(target, backup); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		if _, backupErr := os.Lstat(backup); backupErr == nil {
			_ = os.Rename(backup, target)
		}
		return err
	}
	if _, err := os.Lstat(backup); err == nil {
		return os.RemoveAll(backup)
	}
	return nil
}
