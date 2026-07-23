//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportMBTiles reports that MBTiles import requires the sqliteimport build tag.
func ImportMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	filePath string,
	opts *ImportOptions,
) (*ImportResult, error) {
	return nil, fmt.Errorf("MBTiles/SQLite import requires the sqliteimport build tag")
}

// ImportMBTilesReader reports that MBTiles import requires the sqliteimport build tag.
func ImportMBTilesReader(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	return nil, fmt.Errorf("MBTiles/SQLite import requires the sqliteimport build tag")
}
