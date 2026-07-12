//go:build !shapefile

package importer

import (
	"context"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportShapefile is available only in builds that opt into the optional
// shapefile dependency: go build -tags=shapefile ./...
func ImportShapefile(ctx context.Context, db *storage.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return nil, fmt.Errorf("shapefile import requires the shapefile build tag")
}

// ImportShapefileZip is available only in builds that opt into the optional
// shapefile dependency: go build -tags=shapefile ./...
func ImportShapefileZip(ctx context.Context, db *storage.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return nil, fmt.Errorf("shapefile import requires the shapefile build tag")
}
