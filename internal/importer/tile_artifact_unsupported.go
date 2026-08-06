//go:build js || wasm || baremetal

package importer

import "context"

func ImportTileArtifact(context.Context, TileArtifactSource, string, *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	return nil, ErrTileArtifactImportUnavailable
}
