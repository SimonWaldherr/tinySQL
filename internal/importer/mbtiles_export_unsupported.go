//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// OpenMBTilesResult reports what an in-place open exposed. Declared here too so
// callers compile on targets without the SQLite driver.
type OpenMBTilesResult struct {
	TilesExposed    int64
	MetadataExposed int64
	MinZoom         int
	MaxZoom         int
	Truncated       bool
}

// ExportMBTiles reports that MBTiles export requires the sqliteimport build tag.
func ExportMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	filePath string,
	opts *ExportMBTilesOptions,
) (*ExportMBTilesResult, error) {
	return nil, fmt.Errorf("MBTiles/SQLite export requires the sqliteimport build tag")
}

// OpenMBTiles reports that querying an .mbtiles in place requires the
// sqliteimport build tag.
func OpenMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	filePath string,
	opts *OpenMBTilesOptions,
) (*OpenMBTilesResult, error) {
	return nil, fmt.Errorf("MBTiles/SQLite open requires the sqliteimport build tag")
}
