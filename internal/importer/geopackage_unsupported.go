//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func InspectGeoPackage(ctx context.Context, filePath string) (*GeoPackageInfo, error) {
	return nil, fmt.Errorf("GeoPackage/SQLite import requires the sqliteimport build tag")
}

func ImportGeoPackage(ctx context.Context, db *storage.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return nil, fmt.Errorf("GeoPackage/SQLite import requires the sqliteimport build tag")
}

func ImportGeoPackageReader(ctx context.Context, db *storage.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return nil, fmt.Errorf("GeoPackage/SQLite import requires the sqliteimport build tag")
}
