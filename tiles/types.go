// Package tiles defines tinySQL's stable, typed API for immutable tile
// artifacts. It is intended for serving applications such as tinyTiles, not
// for arbitrary SQL access to an artifact's implementation tables.
//
// Tile coordinates are always TMS coordinates. Callers that receive XYZ
// requests must convert the row before constructing a Key.
package tiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// APIVersion changes only for a deliberately incompatible change to this
// package's public contract. Additive fields and methods are introduced under
// the same major API version.
const APIVersion = 1

// ErrSQLiteImportUnavailable is returned by ImportMBTiles in builds that omit
// tinySQL's optional sqliteimport build tag. Opening and serving an already
// published artifact does not link SQLite on supported native targets.
var ErrSQLiteImportUnavailable = errors.New("tiles: MBTiles import requires a build with -tags=sqliteimport")

// ErrArtifactReaderUnavailable is returned on targets that do not support the
// native paged-artifact reader. Browser/WASM applications use tinyTiles'
// offline cache protocol rather than opening server artifact files directly.
var ErrArtifactReaderUnavailable = errors.New("tiles: artifact readers are unavailable on this target")

// CoordinateSystem identifies the row convention used by tile keys.
type CoordinateSystem string

const (
	// CoordinateTMS is the coordinate system used by MBTiles and every tiles
	// API lookup. y=0 is the southernmost row at a zoom level.
	CoordinateTMS CoordinateSystem = "TMS"
)

// Schema describes the physical representation of a tile artifact.
type Schema string

const (
	// SchemaAuto preserves the source representation during an import.
	SchemaAuto Schema = "auto"
	// SchemaFlat stores tiles(z, x, y, tile_data).
	SchemaFlat Schema = "flat"
	// SchemaNormalized stores map(z, x, y, tile_id) and images(tile_id, tile_data).
	SchemaNormalized Schema = "normalized"
)

// Key identifies one TMS tile.
type Key struct {
	Z int `json:"z"`
	X int `json:"x"`
	Y int `json:"y"`
}

// Validate rejects coordinates outside the MBTiles/TMS zoom domain supported
// by tinySQL artifacts.
func (k Key) Validate() error {
	if k.Z < 0 || k.Z > 30 {
		return fmt.Errorf("tiles: invalid zoom %d", k.Z)
	}
	limit := 1 << k.Z
	if k.X < 0 || k.Y < 0 || k.X >= limit || k.Y >= limit {
		return fmt.Errorf("tiles: invalid TMS coordinate z=%d x=%d y=%d", k.Z, k.X, k.Y)
	}
	return nil
}

func (k Key) String() string { return fmt.Sprintf("%d/%d/%d", k.Z, k.X, k.Y) }

// Range is an inclusive TMS rectangle at one zoom level. Scan visits matching
// tiles in stable z/x/y order without materializing the rectangle.
type Range struct {
	Z    int `json:"z"`
	XMin int `json:"x_min"`
	XMax int `json:"x_max"`
	YMin int `json:"y_min"`
	YMax int `json:"y_max"`
}

// Validate rejects inverted or out-of-domain ranges.
func (r Range) Validate() error {
	if r.XMin > r.XMax || r.YMin > r.YMax {
		return errors.New("tiles: invalid range")
	}
	if err := (Key{Z: r.Z, X: r.XMin, Y: r.YMin}).Validate(); err != nil {
		return err
	}
	return (Key{Z: r.Z, X: r.XMax, Y: r.YMax}).Validate()
}

// Tile is a tile payload returned by Reader. Data is owned by the caller and
// may be retained or modified without affecting a later lookup.
type Tile struct {
	Key  Key    `json:"key"`
	Data []byte `json:"-"`
}

// Reader is the serving-path contract. Implementations are read-only and
// validate the COMPLETE marker, checksums and logical index integrity before
// OpenArtifact returns. One Reader is intended for one concurrent user; use a
// pool of independent readers for parallel HTTP requests.
type Reader interface {
	io.Closer

	// Info returns an independent copy of immutable artifact information.
	Info() ArtifactInfo
	// Metadata finds a standard MBTiles metadata value by name.
	Metadata(ctx context.Context, name string) (value string, found bool, err error)
	// Lookup reads one exact TMS tile.
	Lookup(ctx context.Context, key Key) (tile Tile, found bool, err error)
	// LookupFunc avoids the result wrapper on an exact tile read. fn receives a
	// caller-owned tile payload and may retain it.
	LookupFunc(ctx context.Context, key Key, fn func(Tile) error) (found bool, err error)
	// Scan streams an inclusive TMS rectangle in stable z/x/y order. Returning
	// an error from fn stops the scan and returns that same error to the caller.
	Scan(ctx context.Context, tileRange Range, fn func(Tile) error) error
}

// MetadataScanner is implemented by readers that can stream the complete
// (normally small) MBTiles metadata table. It is deliberately separate from
// Reader so existing Reader implementations remain source-compatible. The
// callback receives values in stable name order and must not retain mutable
// implementation state.
//
// OpenArtifact always returns a MetadataScanner. Applications that need a
// copied metadata map can build one at open time, keeping the tile request
// hot path free of metadata-table scans.
type MetadataScanner interface {
	Reader

	// ScanMetadata visits all metadata name/value pairs without materializing
	// table rows. Returning an error from fn stops the scan and returns that
	// same error to the caller.
	ScanMetadata(ctx context.Context, fn func(name, value string) error) error
}

// OpenOptions controls one artifact reader. A non-positive MaxMemoryBytes
// delegates to tinySQL's documented default reader budget.
type OpenOptions struct {
	MaxMemoryBytes int64
}

// ImportOptions controls a bounded, atomically published MBTiles import.
// Provenance must be JSON-compatible; it is copied and checksummed into the
// published manifest without interpretation by tinySQL.
type ImportOptions struct {
	Schema          Schema
	BatchSize       int
	MaxMemoryBytes  int64
	MinFreeBytes    int64
	Provenance      map[string]any
	Progress        func(Progress)
	ProgressEvery   time.Duration
	ReplaceExisting bool
}

// Progress is reported after resource preflight and bounded import batches.
// It never retains a source tile or source row.
type Progress struct {
	Phase        string
	RowsRead     int64
	RowsWritten  int64
	BytesRead    int64
	BytesWritten int64
	TotalRows    int64
	BatchSize    int
	Estimate     *ResourceEstimate
}

// ResourceEstimate is the preflight resource plan computed before destination
// files are created.
type ResourceEstimate struct {
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

// Index describes one persisted artifact index.
type Index struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// Table describes one persisted artifact table.
type Table struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Rows    int64    `json:"rows"`
	Indexes []Index  `json:"indexes,omitempty"`
}

// ArtifactInfo is the stable, semantic view of manifest.json. It deliberately
// does not expose tinySQL's private pager or raw index configuration map.
// Call Clone before retaining and modifying an Info value obtained elsewhere.
type ArtifactInfo struct {
	APIVersion           int               `json:"api_version"`
	FormatVersion        int               `json:"format_version"`
	TinySQLVersion       string            `json:"tinysql_version"`
	Schema               Schema            `json:"schema"`
	CoordinateSystem     CoordinateSystem  `json:"coordinate_system"`
	CreatedAt            time.Time         `json:"created_at"`
	Source               string            `json:"source"`
	SourceBytes          int64             `json:"source_bytes"`
	Resources            ResourceEstimate  `json:"resources"`
	Provenance           map[string]any    `json:"provenance,omitempty"`
	Tables               []Table           `json:"tables"`
	PhysicalIndexes      map[string]string `json:"physical_indexes,omitempty"`
	Checksums            map[string]string `json:"checksums"`
	TileDigestSHA256     string            `json:"tile_digest_sha256"`
	MetadataDigestSHA256 string            `json:"metadata_digest_sha256"`
}

// Clone returns a deep copy of all maps and slices in info. It is useful when
// passing metadata to code outside the trust boundary of an application.
func (info ArtifactInfo) Clone() ArtifactInfo {
	out := info
	out.Provenance = cloneJSONMap(info.Provenance)
	out.PhysicalIndexes = cloneStringMap(info.PhysicalIndexes)
	out.Checksums = cloneStringMap(info.Checksums)
	if info.Tables != nil {
		out.Tables = make([]Table, len(info.Tables))
		for i, table := range info.Tables {
			out.Tables[i] = Table{
				Name:    table.Name,
				Columns: append([]string(nil), table.Columns...),
				Rows:    table.Rows,
			}
			if table.Indexes != nil {
				out.Tables[i].Indexes = make([]Index, len(table.Indexes))
				for j, index := range table.Indexes {
					out.Tables[i].Indexes[j] = Index{Name: index.Name, Columns: append([]string(nil), index.Columns...), Unique: index.Unique}
				}
			}
		}
	}
	return out
}

// ImportResult is returned only after full validation and atomic publication.
type ImportResult struct {
	ArtifactPath string           `json:"artifact_path"`
	Info         ArtifactInfo     `json:"info"`
	Estimate     ResourceEstimate `json:"estimate"`
}
