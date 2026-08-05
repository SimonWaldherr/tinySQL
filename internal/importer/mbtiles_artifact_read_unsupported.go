//go:build js || wasm || baremetal

package importer

import (
	"context"
	"errors"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var errMBTilesArtifactReaderUnavailable = errors.New("MBTiles artifact readers are unavailable on this target")

func ValidateMBTilesArtifact(context.Context, string) (*MBTilesArtifactManifest, error) {
	return nil, errMBTilesArtifactReaderUnavailable
}

func OpenMBTilesArtifact(context.Context, string, int64) (*storage.DB, *MBTilesArtifactManifest, error) {
	return nil, nil, errMBTilesArtifactReaderUnavailable
}

func OpenMBTilesReader(context.Context, string, int64) (*MBTilesReader, error) {
	return nil, errMBTilesArtifactReaderUnavailable
}
