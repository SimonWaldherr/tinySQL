//go:build js || wasm

package importer

import (
	"context"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ImportMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	filePath string,
	opts *ImportOptions,
) (*ImportResult, error) {
	return nil, fmt.Errorf("MBTiles import is not supported in WASM builds")
}

func ImportMBTilesReader(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	return nil, fmt.Errorf("MBTiles import is not supported in WASM builds")
}
