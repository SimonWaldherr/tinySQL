//go:build !js && !wasm

package importer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const mbtilesPreviewMaxBytes = 4096

// ImportMBTiles imports MBTiles tiles into tableName and metadata into
// tableName_metadata when the source database exposes a metadata table.
func ImportMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	filePath string,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	src, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("open mbtiles: %w", err)
	}
	defer src.Close()

	if err := importMBTilesMetadata(ctx, db, tenant, tableName+"_metadata", src, opts); err != nil {
		return nil, err
	}

	colNames := []string{"zoom_level", "tile_column", "tile_row", "tile_data", "tile_size", "tile_sha256", "tile_preview_base64"}
	colTypes := []storage.ColType{storage.IntType, storage.IntType, storage.IntType, storage.BlobType, storage.IntType, storage.TextType, storage.TextType}
	result := &ImportResult{Encoding: "binary", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	sqlRows, err := src.QueryContext(ctx, "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
	if err != nil {
		return nil, fmt.Errorf("query mbtiles tiles: %w", err)
	}
	defer sqlRows.Close()

	rows := make([][]any, 0)
	for sqlRows.Next() {
		var zoom, col, row int
		var tile []byte
		if err := sqlRows.Scan(&zoom, &col, &row, &tile); err != nil {
			return nil, fmt.Errorf("scan mbtiles tile: %w", err)
		}
		hash := sha256.Sum256(tile)
		var preview any
		if len(tile) <= mbtilesPreviewMaxBytes {
			preview = base64.StdEncoding.EncodeToString(tile)
		}
		rows = append(rows, []any{zoom, col, row, tile, len(tile), fmt.Sprintf("%x", hash), preview})
	}
	if err := sqlRows.Err(); err != nil {
		return nil, fmt.Errorf("read mbtiles tiles: %w", err)
	}

	if err := insertTypedRows(ctx, db, tenant, tableName, colNames, colTypes, rows, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

// ImportMBTilesReader imports MBTiles from an io.Reader by spooling it to a
// temporary SQLite file. This is useful for upload flows.
func ImportMBTilesReader(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	tmp, err := os.CreateTemp("", "tinysql-mbtiles-*.mbtiles")
	if err != nil {
		return nil, fmt.Errorf("create temp mbtiles: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("write temp mbtiles: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("close temp mbtiles: %w", err)
	}
	return ImportMBTiles(ctx, db, tenant, tableName, tmpName, opts)
}

func importMBTilesMetadata(ctx context.Context, db *storage.DB, tenant, tableName string, src *sql.DB, opts *ImportOptions) error {
	if !mbtilesTableExists(ctx, src, "metadata") {
		return nil
	}

	colNames := []string{"name", "value"}
	colTypes := []storage.ColType{storage.TextType, storage.TextType}
	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	sqlRows, err := src.QueryContext(ctx, "SELECT name, value FROM metadata ORDER BY name")
	if err != nil {
		return fmt.Errorf("query mbtiles metadata: %w", err)
	}
	defer sqlRows.Close()

	rows := make([][]any, 0)
	for sqlRows.Next() {
		var name, value string
		if err := sqlRows.Scan(&name, &value); err != nil {
			return fmt.Errorf("scan mbtiles metadata: %w", err)
		}
		rows = append(rows, []any{name, value})
	}
	if err := sqlRows.Err(); err != nil {
		return fmt.Errorf("read mbtiles metadata: %w", err)
	}

	return insertTypedRows(ctx, db, tenant, tableName, colNames, colTypes, rows, opts, result)
}

func mbtilesTableExists(ctx context.Context, src *sql.DB, table string) bool {
	var name string
	err := src.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1", table).Scan(&name)
	return err == nil
}
