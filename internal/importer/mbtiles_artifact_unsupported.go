//go:build !sqliteimport || js || wasm || baremetal

package importer

import (
	"context"
)

func ImportMBTilesArtifact(context.Context, string, string, *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	return nil, ErrMBTilesArtifactSQLiteUnavailable
}
