//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
	"errors"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var errMBTilesArtifactSQLiteUnavailable = errors.New("MBTiles artifacts require a build with -tags=sqliteimport")

func ImportMBTilesArtifact(context.Context, string, string, *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	return nil, errMBTilesArtifactSQLiteUnavailable
}

func ValidateMBTilesArtifact(context.Context, string) (*MBTilesArtifactManifest, error) {
	return nil, errMBTilesArtifactSQLiteUnavailable
}

func OpenMBTilesArtifact(context.Context, string, int64) (*storage.DB, *MBTilesArtifactManifest, error) {
	return nil, nil, errMBTilesArtifactSQLiteUnavailable
}

func OpenMBTilesReader(context.Context, string, int64) (*MBTilesReader, error) {
	return nil, errMBTilesArtifactSQLiteUnavailable
}
