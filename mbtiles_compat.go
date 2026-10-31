package tinysql

import (
	"context"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
)

// MBTiles artifact compatibility API. Artifacts are portable, read-only,
// atomically published paged-index directories intended for large MBTiles
// datasets. New integrations should use github.com/SimonWaldherr/tinySQL/tiles:
// it exposes typed TMS keys and an interface-based reader without leaking a
// concrete internal implementation.
//
// Deprecated: use package tiles for new artifact integrations.
type MBTilesArtifactSchema = importer.MBTilesArtifactSchema

type MBTilesArtifactOptions = importer.MBTilesArtifactOptions

type MBTilesProgress = importer.MBTilesProgress

type MBTilesResourceEstimate = importer.MBTilesResourceEstimate

type MBTilesArtifactTable = importer.MBTilesArtifactTable

type MBTilesArtifactIndex = importer.MBTilesArtifactIndex

type MBTilesArtifactManifest = importer.MBTilesArtifactManifest

type MBTilesArtifactResult = importer.MBTilesArtifactResult

type MBTilesReader = importer.MBTilesReader

const (
	MBTilesSchemaAuto       = importer.MBTilesSchemaAuto
	MBTilesSchemaFlat       = importer.MBTilesSchemaFlat
	MBTilesSchemaNormalized = importer.MBTilesSchemaNormalized
)

// Deprecated: use tiles.ImportMBTiles.
func ImportMBTilesArtifact(ctx context.Context, sourcePath, artifactPath string, opts *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	return importer.ImportMBTilesArtifact(ctx, sourcePath, artifactPath, opts)
}

// Deprecated: use tiles.ValidateArtifact.
func ValidateMBTilesArtifact(ctx context.Context, artifactPath string) (*MBTilesArtifactManifest, error) {
	return importer.ValidateMBTilesArtifact(ctx, artifactPath)
}

// Deprecated: use tiles.OpenArtifact. This function exposes a generic DB for
// compatibility and should not be used as a tile-serving hot path.
func OpenMBTilesArtifact(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*DB, *MBTilesArtifactManifest, error) {
	return importer.OpenMBTilesArtifact(ctx, artifactPath, maxMemoryBytes)
}

// Deprecated: use tiles.OpenArtifact.
func OpenMBTilesReader(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*MBTilesReader, error) {
	return importer.OpenMBTilesReader(ctx, artifactPath, maxMemoryBytes)
}
