//go:build js || wasm || baremetal

package tiles

import "context"

// ImportMBTiles requires tinySQL's optional sqliteimport build tag.
func ImportMBTiles(context.Context, string, string, *ImportOptions) (*ImportResult, error) {
	return nil, ErrSQLiteImportUnavailable
}

// ValidateArtifact is unavailable on browser and bare-metal targets.
func ValidateArtifact(context.Context, string) (ArtifactInfo, error) {
	return ArtifactInfo{}, ErrArtifactReaderUnavailable
}

// OpenArtifact is unavailable on browser and bare-metal targets.
func OpenArtifact(context.Context, string, OpenOptions) (Reader, error) {
	return nil, ErrArtifactReaderUnavailable
}
