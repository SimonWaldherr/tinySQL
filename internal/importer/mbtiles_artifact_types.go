package importer

import (
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// MBTilesArtifactSchema selects the physical representation used in a
// dataset.tinysql artifact.
type MBTilesArtifactSchema string

const (
	MBTilesSchemaAuto       MBTilesArtifactSchema = "auto"
	MBTilesSchemaFlat       MBTilesArtifactSchema = "flat"
	MBTilesSchemaNormalized MBTilesArtifactSchema = "normalized"
)

// MBTilesArtifactOptions controls a bounded, publish-on-success import.
type MBTilesArtifactOptions struct {
	Schema         MBTilesArtifactSchema
	BatchSize      int
	MaxMemoryBytes int64
	MinFreeBytes   int64
	// Provenance is optional caller-supplied, JSON-compatible source context.
	// tinySQL preserves it in the checksummed manifest but does not interpret
	// application-specific fields such as an OSM generator configuration.
	Provenance      map[string]any
	Progress        func(MBTilesProgress)
	ProgressEvery   time.Duration
	ReplaceExisting bool
}

// MBTilesProgress is emitted after resource planning and after each flushed
// batch. The callback is never called while a source row is being retained.
type MBTilesProgress struct {
	Phase        string
	RowsRead     int64
	RowsWritten  int64
	BytesRead    int64
	BytesWritten int64
	TotalRows    int64
	BatchSize    int
	Estimate     *MBTilesResourceEstimate
}

// MBTilesResourceEstimate is the preflight result printed/reported before
// the first destination file is created.
type MBTilesResourceEstimate struct {
	SourceBytes     int64 `json:"source_bytes"`
	TileCount       int64 `json:"tile_count"`
	MapRows         int64 `json:"map_rows,omitempty"`
	ImageRows       int64 `json:"image_rows,omitempty"`
	MetadataRows    int64 `json:"metadata_rows"`
	EstimatedMemory int64 `json:"estimated_memory"`
	EstimatedDisk   int64 `json:"estimated_disk"`
	AvailableDisk   int64 `json:"available_disk"`
	BatchSize       int   `json:"batch_size"`
}

// MBTilesArtifactTable describes one persisted table in the manifest.
type MBTilesArtifactTable struct {
	Name    string                 `json:"name"`
	Columns []string               `json:"columns"`
	Rows    int64                  `json:"rows"`
	Indexes []MBTilesArtifactIndex `json:"indexes,omitempty"`
}

type MBTilesArtifactIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// MBTilesArtifactManifest is the signed-by-checksum description of a
// published artifact. The JSON itself is included in checksums.sha256.
type MBTilesArtifactManifest struct {
	FormatVersion  int                     `json:"format_version"`
	TinySQLVersion string                  `json:"tinysql_version"`
	Schema         MBTilesArtifactSchema   `json:"schema"`
	CreatedAt      time.Time               `json:"created_at"`
	Source         string                  `json:"source"`
	SourceBytes    int64                   `json:"source_bytes"`
	Resources      MBTilesResourceEstimate `json:"resources"`
	Provenance     map[string]any          `json:"provenance,omitempty"`
	Tables         []MBTilesArtifactTable  `json:"tables"`
	IndexConfig    map[string]any          `json:"index_config"`
	Checksums      map[string]string       `json:"checksums"`
}

// MBTilesArtifactResult is returned only after validation and atomic publish.
type MBTilesArtifactResult struct {
	ArtifactPath string
	Manifest     MBTilesArtifactManifest
	Estimate     MBTilesResourceEstimate
}

// MBTilesReader is a validated, read-only tile reader. Each reader owns one
// pager handle; separate readers are safe to use concurrently.
type MBTilesReader struct {
	db            *storage.DB
	manifest      *MBTilesArtifactManifest
	metadataIndex *storage.PagedIndexLocator
	tileIndex     *storage.PagedIndexLocator
	imageIndex    *storage.PagedIndexLocator
}
