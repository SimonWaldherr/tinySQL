package importer

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Table Operations
// ============================================================================

// createTable creates a new table in the database with the specified columns and types.
func createTable(ctx context.Context, db *storage.DB, tenant, tableName string, colNames []string, colTypes []storage.ColType) error {
	return createTableWithPrimaryKey(ctx, db, tenant, tableName, colNames, colTypes, "")
}

func createTableWithPrimaryKey(ctx context.Context, db *storage.DB, tenant, tableName string, colNames []string, colTypes []storage.ColType, primaryKey string) error {
	// Build table structure
	cols := make([]storage.Column, len(colNames))
	foundPrimaryKey := primaryKey == ""
	for i, name := range colNames {
		cols[i] = storage.Column{
			Name: name,
			Type: colTypes[i],
		}
		if strings.EqualFold(name, primaryKey) {
			cols[i].Constraint = storage.PrimaryKey
			foundPrimaryKey = true
		}
	}
	if !foundPrimaryKey {
		return fmt.Errorf("primary key column %q is not present in import", primaryKey)
	}

	// Build through storage.NewTable rather than a struct literal: it populates
	// the table's private column-position map, which Table.ColIndex is the only
	// reader of. A literal leaves that map nil, so ColIndex fails for every
	// column of an imported table. Ordinary SELECT/INSERT still worked (the
	// executor resolves names by scanning Cols), which is why this stayed
	// unnoticed — but every feature that resolves a column up front does not:
	// VEC_SEARCH, FTS_SEARCH and HYBRID_SEARCH all call ColIndex, so vector and
	// full-text queries against imported data failed with "unknown column".
	tbl := storage.NewTable(tableName, cols, false)

	// Add to database (creates if not exists)
	if err := db.Put(tenant, tbl); err != nil {
		// If table already exists, that's okay
		if _, getErr := db.Get(tenant, tableName); getErr == nil {
			return nil // Table exists, continue
		}
		return fmt.Errorf("create table %s: %w", tableName, err)
	}

	return nil
}

// truncateTable removes all rows from a table.
func truncateTable(ctx context.Context, db *storage.DB, tenant, tableName string) error {
	// Get the table
	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return fmt.Errorf("get table %s: %w", tableName, err)
	}

	// Clear all rows
	tbl.Rows = make([][]any, 0)

	return nil
}

// ============================================================================
// Data Insertion
// ============================================================================

// flushInsertBatch appends batch to tenant/tableName's rows and returns how
// many rows were added, along with the batch slice reset to length 0 for
// reuse. It is a no-op (0, batch, nil) when batch is empty.
func flushInsertBatch(db *storage.DB, tenant, tableName string, batch [][]any) (rowsAdded int64, remaining [][]any, err error) {
	if len(batch) == 0 {
		return 0, batch, nil
	}
	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return 0, batch, fmt.Errorf("get table: %w", err)
	}
	tbl.Rows = append(tbl.Rows, batch...)
	return int64(len(batch)), batch[:0], nil
}

// insertAllRecords inserts all CSV records from memory into the table with batching.
func insertAllRecords(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	colNames []string,
	colTypes []storage.ColType,
	allRecords [][]string,
	opts *ImportOptions,
) (rowsInserted int64, rowsSkipped int64, errors []string) {

	errors = make([]string, 0)
	batch := make([][]any, 0, opts.BatchSize)

	// Helper to flush batch
	flushBatch := func() error {
		added, rest, err := flushInsertBatch(db, tenant, tableName, batch)
		if err != nil {
			return err
		}
		rowsInserted += added
		batch = rest
		return nil
	}

	// Process all records
	for rowNum, rec := range allRecords {
		// Convert and validate row
		row, err := convertRow(rec, colNames, colTypes, opts)
		if err != nil {
			if opts.StrictTypes {
				errors = append(errors, fmt.Sprintf("row %d: %v", rowNum+1, err))
				return rowsInserted, rowsSkipped + 1, errors
			}
			errors = append(errors, fmt.Sprintf("row %d: %v (skipped)", rowNum+1, err))
			rowsSkipped++
			continue
		}

		batch = append(batch, row)

		// Flush batch when full
		if len(batch) >= opts.BatchSize {
			if err := flushBatch(); err != nil {
				errors = append(errors, err.Error())
				return rowsInserted, rowsSkipped, errors
			}
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			errors = append(errors, "import cancelled")
			return rowsInserted, rowsSkipped, errors
		default:
		}
	}

	// Flush remaining batch
	if err := flushBatch(); err != nil {
		errors = append(errors, err.Error())
	}

	return rowsInserted, rowsSkipped, errors
}

// insertCSVRecords inserts an initial slice of CSV records and then continues
// reading from the provided reader with batching.
func insertCSVRecords(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	colNames []string,
	colTypes []storage.ColType,
	initialRecords [][]string,
	csvr *csv.Reader,
	opts *ImportOptions,
) (rowsInserted int64, rowsSkipped int64, errors []string) {
	errors = make([]string, 0)
	batch := make([][]any, 0, opts.BatchSize)
	rowNum := 0

	flushBatch := func() error {
		added, rest, err := flushInsertBatch(db, tenant, tableName, batch)
		if err != nil {
			return err
		}
		rowsInserted += added
		batch = rest
		return nil
	}

	processRecord := func(rec []string) bool {
		rowNum++
		row, err := convertRow(rec, colNames, colTypes, opts)
		if err != nil {
			if opts.StrictTypes {
				errors = append(errors, fmt.Sprintf("row %d: %v", rowNum, err))
				rowsSkipped++
				return true
			}
			errors = append(errors, fmt.Sprintf("row %d: %v (skipped)", rowNum, err))
			rowsSkipped++
			return false
		}

		batch = append(batch, row)
		if len(batch) >= opts.BatchSize {
			if err := flushBatch(); err != nil {
				errors = append(errors, err.Error())
				return true
			}
		}

		select {
		case <-ctx.Done():
			errors = append(errors, "import cancelled")
			return true
		default:
		}
		return false
	}

	for _, rec := range initialRecords {
		if stop := processRecord(rec); stop {
			return rowsInserted, rowsSkipped, errors
		}
	}

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rowNum++
			errors = append(errors, fmt.Sprintf("row %d: read error: %v", rowNum, err))
			rowsSkipped++
			continue
		}
		if stop := processRecord(rec); stop {
			return rowsInserted, rowsSkipped, errors
		}
	}

	if err := flushBatch(); err != nil {
		errors = append(errors, err.Error())
	}

	return rowsInserted, rowsSkipped, errors
}

// convertRow converts a CSV record to a typed row for insertion.
func convertRow(rec []string, colNames []string, colTypes []storage.ColType, opts *ImportOptions) ([]any, error) {
	row := make([]any, len(colNames))

	for i := 0; i < len(colNames); i++ {
		var val string
		if i < len(rec) {
			val = rec[i]
		}

		converted, err := convertValue(val, colTypes[i], opts.DateTimeFormats, opts.NullLiterals)
		if err != nil {
			// On error, fall back to string if not strict
			if !opts.StrictTypes {
				row[i] = val
			} else {
				return nil, fmt.Errorf("column %s: %w", colNames[i], err)
			}
		} else {
			row[i] = converted
		}
	}

	return row, nil
}
