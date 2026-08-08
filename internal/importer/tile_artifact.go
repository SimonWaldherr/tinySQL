//go:build !js && !wasm && !baremetal

package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportTileArtifact streams a repeatable tile source directly into a
// validated, atomically published artifact. It never creates or opens an
// intermediate SQLite/MBTiles database.
func ImportTileArtifact(ctx context.Context, source TileArtifactSource, artifactPath string, opts *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	if source == nil {
		return nil, errors.New("tile artifact source is nil")
	}
	if opts == nil {
		opts = &MBTilesArtifactOptions{}
	}
	var err error
	opts, err = cloneArtifactOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("copy tile artifact options: %w", err)
	}
	applyArtifactOptionDefaults(opts)
	if opts.Schema == "" || opts.Schema == MBTilesSchemaAuto {
		opts.Schema = MBTilesSchemaFlat
	}
	if opts.Schema != MBTilesSchemaFlat {
		return nil, fmt.Errorf("direct tile stream requires flat artifact schema, got %q", opts.Schema)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	info, err := source.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect tile source: %w", err)
	}
	if info.SourceBytes < 0 || info.TileCount < 0 || info.TileBytes < 0 || info.MaxTileBytes < 0 {
		return nil, errors.New("tile source info contains a negative size or count")
	}
	if info.TileCount == 0 {
		return nil, errors.New("tile source contains no tiles")
	}
	if info.MaxTileBytes > info.TileBytes {
		return nil, errors.New("tile source max tile size exceeds total tile bytes")
	}
	estimatedMemory, estimatedDisk, err := estimateDirectTileResources(info, opts.BatchSize)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(filepath.Clean(artifactPath))
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact parent: %w", err)
	}
	estimate := MBTilesResourceEstimate{
		SourceBytes:     info.SourceBytes,
		TileCount:       info.TileCount,
		MapRows:         info.TileCount,
		ImageRows:       info.TileCount,
		MetadataRows:    int64(len(info.Metadata)),
		EstimatedMemory: estimatedMemory,
		EstimatedDisk:   estimatedDisk,
		AvailableDisk:   availableDiskBytes(parent),
		BatchSize:       opts.BatchSize,
	}
	if opts.Progress != nil {
		opts.Progress(MBTilesProgress{Phase: "preflight", TotalRows: estimate.TileCount, BatchSize: opts.BatchSize, Estimate: &estimate})
	}
	if err := checkArtifactResourceBudget(estimate, opts, "tile stream import"); err != nil {
		return nil, err
	}
	if err := ensureArtifactTargetAvailable(artifactPath, opts.ReplaceExisting); err != nil {
		return nil, err
	}
	tmp, err := newTempArtifactDir(parent)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
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
	if err := createArtifactTables(db, MBTilesSchemaFlat); err != nil {
		return nil, err
	}
	metadataRows, metadataDigest := directMetadataRows(info.Metadata)
	if err := appendArtifactRows(ctx, db, "default", "metadata", metadataRows, opts.BatchSize, nil, opts); err != nil {
		return nil, err
	}
	progress := &artifactProgress{opts: opts, total: info.TileCount, batchSize: opts.BatchSize}
	tileDigest := sha256.New()
	batch := make([][]any, 0, opts.BatchSize)
	var actualBytes, actualMax int64
	flush := func() error {
		if err := appendArtifactRows(ctx, db, "default", "tiles", batch, opts.BatchSize, progress, opts); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	err = source.ScanTiles(ctx, func(tile TileArtifactTile) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := validateTileCoordinate(tile.Z, tile.X, tile.Y); err != nil {
			return err
		}
		if progress.read >= info.TileCount {
			return fmt.Errorf("tile source emitted more than declared %d tiles", info.TileCount)
		}
		data := append([]byte(nil), tile.Data...)
		size := int64(len(data))
		actualBytes += size
		if size > actualMax {
			actualMax = size
		}
		hashTile(tileDigest, tile.Z, tile.X, tile.Y, data)
		progress.read++
		progress.bytesRead += size
		batch = append(batch, []any{tile.Z, tile.X, tile.Y, data})
		if len(batch) == opts.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan tile source: %w", err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if progress.read != info.TileCount || actualBytes != info.TileBytes || actualMax != info.MaxTileBytes {
		return nil, fmt.Errorf("tile source changed during import: tiles=%d/%d bytes=%d/%d max=%d/%d", progress.read, info.TileCount, actualBytes, info.TileBytes, actualMax, info.MaxTileBytes)
	}
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close imported database: %w", err)
	}
	closeDB = false
	debug.FreeOSMemory()
	indexConfig := artifactIndexConfig(MBTilesSchemaFlat)
	if err := writeJSONFile(filepath.Join(tmp, "indexes", "config.json"), indexConfig); err != nil {
		return nil, err
	}
	sourceName := filepath.Base(info.Name)
	if sourceName == "." || sourceName == "" {
		sourceName = "tile-stream"
	}
	manifest := MBTilesArtifactManifest{
		FormatVersion:  mbtilesArtifactFormatVersion,
		TinySQLVersion: "v0.29.0",
		Schema:         MBTilesSchemaFlat,
		CreatedAt:      time.Now().UTC(),
		Source:         sourceName,
		SourceBytes:    info.SourceBytes,
		Resources:      estimate,
		Provenance:     opts.Provenance,
		Tables:         artifactTableManifest(MBTilesSchemaFlat, info.TileCount, info.TileCount, int64(len(metadataRows))),
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
	if err := os.WriteFile(filepath.Join(tmp, "COMPLETE"), nil, 0o644); err != nil {
		return nil, fmt.Errorf("write completion marker: %w", err)
	}
	if err := syncArtifactTree(tmp); err != nil {
		return nil, err
	}
	if err := publishArtifact(tmp, artifactPath, opts.ReplaceExisting); err != nil {
		return nil, err
	}
	if opts.Progress != nil {
		opts.Progress(MBTilesProgress{Phase: "published", RowsRead: info.TileCount, RowsWritten: info.TileCount, TotalRows: info.TileCount, BatchSize: opts.BatchSize})
	}
	return &MBTilesArtifactResult{ArtifactPath: artifactPath, Manifest: manifest, Estimate: estimate}, nil
}

func estimateDirectTileResources(info TileArtifactInfo, batchSize int) (memory, disk int64, err error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	batch := int64(batchSize)
	if batch <= 0 {
		return 0, 0, errors.New("tile stream batch size must be positive")
	}
	if batch > (maxInt64-importBatchFixedBytes)/importBatchRowBytes {
		return 0, 0, errors.New("tile stream memory estimate overflows")
	}
	overhead := batch*importBatchRowBytes + importBatchFixedBytes
	if info.MaxTileBytes > (maxInt64-overhead)/batch {
		return 0, 0, errors.New("tile stream memory estimate overflows")
	}
	memory = info.MaxTileBytes*batch + overhead
	if info.TileCount > (maxInt64-importArtifactFixedBytes-info.TileBytes)/importArtifactRowBytes {
		return 0, 0, errors.New("tile stream disk estimate overflows")
	}
	disk = info.TileBytes + info.TileCount*importArtifactRowBytes + importArtifactFixedBytes
	return memory, disk, nil
}

const (
	importBatchRowBytes      int64 = 768
	importBatchFixedBytes    int64 = 2 << 20
	importArtifactRowBytes   int64 = 320
	importArtifactFixedBytes int64 = 8 << 20
)

func directMetadataRows(metadata map[string]string) ([][]any, string) {
	names := make([]string, 0, len(metadata))
	for name := range metadata {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]any, 0, len(names))
	h := sha256.New()
	for _, name := range names {
		value := metadata[name]
		rows = append(rows, []any{name, value})
		hashMetadata(h, name, value)
	}
	return rows, hex.EncodeToString(h.Sum(nil))
}
