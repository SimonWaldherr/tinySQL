//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
	"fmt"
)

// LookupMBTilesTile reports that MBTiles lookup requires the sqliteimport build tag.
func LookupMBTilesTile(ctx context.Context, filePath string, z, x, y int) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("MBTiles/SQLite lookup requires the sqliteimport build tag")
}
