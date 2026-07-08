package importer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func marshalJSONValue(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return json.RawMessage(b)
}

func insertTypedRows(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	colNames []string,
	colTypes []storage.ColType,
	rows [][]any,
	opts *ImportOptions,
	result *ImportResult,
) error {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return err
		}
	}
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return err
		}
	}

	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return fmt.Errorf("get table: %w", err)
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = len(rows)
	}
	if batchSize <= 0 {
		batchSize = 1
	}

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		tbl.Rows = append(tbl.Rows, rows[start:end]...)
		result.RowsInserted += int64(end - start)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}
