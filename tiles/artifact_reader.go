//go:build !js && !wasm && !baremetal

package tiles

import (
	"context"
	"errors"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
)

// ImportMBTiles streams a flat or normalized MBTiles SQLite source into a
// validated, immutable tile artifact. It never exposes the source database or
// tinySQL's internal pager to the caller.
func ImportMBTiles(ctx context.Context, sourcePath, artifactPath string, opts *ImportOptions) (*ImportResult, error) {
	internalOptions, err := toInternalImportOptions(opts)
	if err != nil {
		return nil, err
	}
	result, err := importer.ImportMBTilesArtifact(ctx, sourcePath, artifactPath, internalOptions)
	if err != nil {
		if errors.Is(err, importer.ErrMBTilesArtifactSQLiteUnavailable) {
			return nil, ErrSQLiteImportUnavailable
		}
		return nil, err
	}
	return &ImportResult{
		ArtifactPath: result.ArtifactPath,
		Info:         artifactInfo(&result.Manifest),
		Estimate:     resourceEstimate(result.Estimate),
	}, nil
}

// ValidateArtifact runs the complete checksum, schema, unique-key, index and
// logical digest audit. It is safe to call before changing a publication
// pointer or deploying an artifact to a reader fleet.
func ValidateArtifact(ctx context.Context, artifactPath string) (ArtifactInfo, error) {
	manifest, err := importer.ValidateMBTilesArtifact(ctx, artifactPath)
	if err != nil {
		return ArtifactInfo{}, err
	}
	return artifactInfo(manifest), nil
}

// OpenArtifact validates artifactPath before returning a read-only Reader.
// The result has no database/sql dependency in its serving API.
func OpenArtifact(ctx context.Context, artifactPath string, opts OpenOptions) (MetadataScanner, error) {
	reader, err := importer.OpenMBTilesReader(ctx, artifactPath, opts.MaxMemoryBytes)
	if err != nil {
		return nil, err
	}
	manifest := reader.Manifest()
	if manifest == nil {
		_ = reader.Close()
		return nil, errors.New("tiles: opened artifact has no manifest")
	}
	return &artifactReader{reader: reader, info: artifactInfo(manifest)}, nil
}

type artifactReader struct {
	reader *importer.MBTilesReader
	info   ArtifactInfo
}

var _ Reader = (*artifactReader)(nil)
var _ MetadataScanner = (*artifactReader)(nil)

func (r *artifactReader) Info() ArtifactInfo {
	if r == nil {
		return ArtifactInfo{}
	}
	return r.info.Clone()
}

func (r *artifactReader) Metadata(ctx context.Context, name string) (string, bool, error) {
	if r == nil || r.reader == nil {
		return "", false, errors.New("tiles: reader is closed")
	}
	return r.reader.LookupMetadata(ctx, name)
}

func (r *artifactReader) ScanMetadata(ctx context.Context, fn func(name, value string) error) error {
	if r == nil || r.reader == nil {
		return errors.New("tiles: reader is closed")
	}
	if fn == nil {
		return errors.New("tiles: metadata callback is nil")
	}
	return r.reader.ScanMetadata(ctx, fn)
}

func (r *artifactReader) Lookup(ctx context.Context, key Key) (Tile, bool, error) {
	if r == nil || r.reader == nil {
		return Tile{}, false, errors.New("tiles: reader is closed")
	}
	if err := key.Validate(); err != nil {
		return Tile{}, false, err
	}
	data, found, err := r.reader.LookupTile(ctx, key.Z, key.X, key.Y)
	if err != nil || !found {
		return Tile{}, found, err
	}
	return Tile{Key: key, Data: data}, true, nil
}

func (r *artifactReader) LookupFunc(ctx context.Context, key Key, fn func(Tile) error) (bool, error) {
	if r == nil || r.reader == nil {
		return false, errors.New("tiles: reader is closed")
	}
	if err := key.Validate(); err != nil {
		return false, err
	}
	if fn == nil {
		return false, errors.New("tiles: tile callback is nil")
	}
	return r.reader.LookupTileFunc(ctx, key.Z, key.X, key.Y, func(data []byte) error {
		return fn(Tile{Key: key, Data: data})
	})
}

func (r *artifactReader) Scan(ctx context.Context, tileRange Range, fn func(Tile) error) error {
	if r == nil || r.reader == nil {
		return errors.New("tiles: reader is closed")
	}
	if err := tileRange.Validate(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("tiles: range callback is nil")
	}
	var callbackErr error
	err := r.reader.ScanTileRange(ctx, tileRange.Z, tileRange.XMin, tileRange.XMax, tileRange.YMin, tileRange.YMax, func(z, x, y int, data []byte) bool {
		callbackErr = fn(Tile{Key: Key{Z: z, X: x, Y: y}, Data: data})
		return callbackErr == nil
	})
	if err != nil {
		return err
	}
	return callbackErr
}

func (r *artifactReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}

func toInternalImportOptions(options *ImportOptions) (*importer.MBTilesArtifactOptions, error) {
	if options == nil {
		return nil, nil
	}
	provenance, err := cloneJSONMapChecked(options.Provenance)
	if err != nil {
		return nil, fmt.Errorf("tiles: copy provenance: %w", err)
	}
	out := &importer.MBTilesArtifactOptions{
		Schema:          importer.MBTilesArtifactSchema(options.Schema),
		BatchSize:       options.BatchSize,
		MaxMemoryBytes:  options.MaxMemoryBytes,
		MinFreeBytes:    options.MinFreeBytes,
		Provenance:      provenance,
		ProgressEvery:   options.ProgressEvery,
		ReplaceExisting: options.ReplaceExisting,
	}
	if options.Progress != nil {
		out.Progress = func(progress importer.MBTilesProgress) {
			options.Progress(publicProgress(progress))
		}
	}
	return out, nil
}

func publicProgress(progress importer.MBTilesProgress) Progress {
	out := Progress{
		Phase:        progress.Phase,
		RowsRead:     progress.RowsRead,
		RowsWritten:  progress.RowsWritten,
		BytesRead:    progress.BytesRead,
		BytesWritten: progress.BytesWritten,
		TotalRows:    progress.TotalRows,
		BatchSize:    progress.BatchSize,
	}
	if progress.Estimate != nil {
		estimate := resourceEstimate(*progress.Estimate)
		out.Estimate = &estimate
	}
	return out
}

func artifactInfo(manifest *importer.MBTilesArtifactManifest) ArtifactInfo {
	if manifest == nil {
		return ArtifactInfo{}
	}
	info := ArtifactInfo{
		APIVersion:       APIVersion,
		FormatVersion:    manifest.FormatVersion,
		TinySQLVersion:   manifest.TinySQLVersion,
		Schema:           Schema(manifest.Schema),
		CoordinateSystem: CoordinateTMS,
		CreatedAt:        manifest.CreatedAt,
		Source:           manifest.Source,
		SourceBytes:      manifest.SourceBytes,
		Resources:        resourceEstimate(manifest.Resources),
		Provenance:       cloneJSONMap(manifest.Provenance),
		Checksums:        cloneStringMap(manifest.Checksums),
	}
	if coordinate, ok := manifest.IndexConfig["coordinate_system"].(string); ok && coordinate != "" {
		info.CoordinateSystem = CoordinateSystem(coordinate)
	}
	if digest, ok := manifest.IndexConfig["tile_digest_sha256"].(string); ok {
		info.TileDigestSHA256 = digest
	}
	if digest, ok := manifest.IndexConfig["metadata_digest_sha256"].(string); ok {
		info.MetadataDigestSHA256 = digest
	}
	for table, index := range manifest.IndexConfig {
		name, ok := index.(string)
		if !ok || table == "coordinate_system" || table == "tile_digest_sha256" || table == "metadata_digest_sha256" {
			continue
		}
		if info.PhysicalIndexes == nil {
			info.PhysicalIndexes = make(map[string]string)
		}
		info.PhysicalIndexes[table] = name
	}
	if manifest.Tables != nil {
		info.Tables = make([]Table, len(manifest.Tables))
		for i, table := range manifest.Tables {
			info.Tables[i] = Table{Name: table.Name, Columns: append([]string(nil), table.Columns...), Rows: table.Rows}
			if table.Indexes != nil {
				info.Tables[i].Indexes = make([]Index, len(table.Indexes))
				for j, index := range table.Indexes {
					info.Tables[i].Indexes[j] = Index{Name: index.Name, Columns: append([]string(nil), index.Columns...), Unique: index.Unique}
				}
			}
		}
	}
	return info
}

func resourceEstimate(value importer.MBTilesResourceEstimate) ResourceEstimate {
	return ResourceEstimate{
		SourceBytes:     value.SourceBytes,
		TileCount:       value.TileCount,
		MapRows:         value.MapRows,
		ImageRows:       value.ImageRows,
		MetadataRows:    value.MetadataRows,
		EstimatedMemory: value.EstimatedMemory,
		EstimatedDisk:   value.EstimatedDisk,
		AvailableDisk:   value.AvailableDisk,
		BatchSize:       value.BatchSize,
	}
}
