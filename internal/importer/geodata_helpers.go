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
	// Deliberately not applyDefaults(opts) here: every caller (ImportMBTiles,
	// OpenMBTiles, ImportOSM, ImportRoutingGraph) already normalizes its own
	// top-level opts once, before deriving a batch-specific copy with
	// CreateTable/Truncate explicitly set to false for every batch after the
	// first -- that is how "table already exists, just keep appending" gets
	// expressed. applyDefaults's CreateTable heuristic ("enable it when
	// neither option is explicitly set") cannot tell that apart from a
	// caller who never set either field, so calling it again here would flip
	// CreateTable back to true on every batch: the resulting fallback path
	// (createTable's db.Put fails because the table exists, falls back to
	// db.Get) forces a full-table materialization of everything imported so
	// far before every single batch, for every storage mode -- silently
	// wasteful in memory (the common case), catastrophically slow when the
	// destination is a ModePagedIndex table larger than memory (Get then
	// costs a real disk scan).
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

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = len(rows)
	}
	if batchSize <= 0 {
		batchSize = 1
	}

	// A ModePagedIndex destination can append each batch straight to its
	// on-disk B+Trees (db.AppendRowsFast) instead of the db.Get-once,
	// append-forever path below. That distinction is what makes importing a
	// tileset larger than memory possible: the fallback needs the whole
	// destination table resident by the time the import finishes -- for
	// every storage mode, paged included, because nothing durable happens
	// until that in-memory slice is eventually saved, and a full-table
	// rewrite at that point would make the save itself cost the size of
	// everything imported. Once the first batch picks a path, later batches
	// in this call stay on it: mixing would let two different row-numbering
	// schemes disagree about where an already-flushed batch ended.
	var tbl *storage.Table
	fastPath := false
	fastPathDecided := false

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		if !fastPathDecided {
			ok, err := db.AppendRowsFast(tenant, tableName, batch)
			if err != nil {
				return fmt.Errorf("append rows: %w", err)
			}
			fastPath = ok
			fastPathDecided = true
			if ok {
				result.RowsInserted += int64(len(batch))
				if err := checkContext(ctx); err != nil {
					return err
				}
				continue
			}
		} else if fastPath {
			if _, err := db.AppendRowsFast(tenant, tableName, batch); err != nil {
				return fmt.Errorf("append rows: %w", err)
			}
			result.RowsInserted += int64(len(batch))
			if err := checkContext(ctx); err != nil {
				return err
			}
			continue
		}

		if tbl == nil {
			var err error
			tbl, err = db.Get(tenant, tableName)
			if err != nil {
				return fmt.Errorf("get table: %w", err)
			}
		}
		tbl.Rows = append(tbl.Rows, batch...)
		result.RowsInserted += int64(len(batch))

		if err := checkContext(ctx); err != nil {
			return err
		}
	}

	if fastPath {
		// Nothing was ever fetched into tbl above, but createTable's initial
		// Put (when opts.CreateTable is true) cached an empty table in the
		// DB regardless -- see DiscardCachedTable. Drop it so a later Get
		// for this table (ExportMBTiles, a SELECT, the next import call)
		// reads what AppendRowsFast actually wrote instead of that empty
		// snapshot from before any of these rows existed.
		db.DiscardCachedTable(tenant, tableName)
	}

	return nil
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
