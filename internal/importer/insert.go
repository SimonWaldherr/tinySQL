package importer

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Table Operations
// ============================================================================

// createTable creates a new table in the database with the specified columns and types.
func createTable(ctx context.Context, db *storage.DB, tenant, tableName string, colNames []string, colTypes []storage.ColType) error {
	// Build table structure
	cols := make([]storage.Column, len(colNames))
	for i, name := range colNames {
		cols[i] = storage.Column{
			Name: name,
			Type: colTypes[i],
		}
	}

	// Create new table
	tbl := &storage.Table{
		Name: tableName,
		Cols: cols,
		Rows: make([][]any, 0),
	}

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
		if len(batch) == 0 {
			return nil
		}

		// Get table and append rows directly
		tbl, err := db.Get(tenant, tableName)
		if err != nil {
			return fmt.Errorf("get table: %w", err)
		}

		// Append batch to table rows
		tbl.Rows = append(tbl.Rows, batch...)

		rowsInserted += int64(len(batch))
		batch = batch[:0] // Clear batch
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

// streamInsertCSV reads CSV records and inserts them into the table with batching.
// DEPRECATED: Use insertAllRecords instead for simpler in-memory processing.
func streamInsertCSV(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	colNames []string,
	colTypes []storage.ColType,
	firstDataRow []string,
	csvr *csv.Reader,
	opts *ImportOptions,
) (rowsInserted int64, rowsSkipped int64, errors []string) {

	errors = make([]string, 0)
	batch := make([][]any, 0, opts.BatchSize)

	// Helper to flush batch
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Get table and append rows directly
		tbl, err := db.Get(tenant, tableName)
		if err != nil {
			return fmt.Errorf("get table: %w", err)
		}

		// Append batch to table rows
		tbl.Rows = append(tbl.Rows, batch...)

		rowsInserted += int64(len(batch))
		batch = batch[:0] // Clear batch
		return nil
	}

	// Process first data row if present (no-header case)
	if firstDataRow != nil {
		row, err := convertRow(firstDataRow, colNames, colTypes, opts)
		if err != nil {
			if opts.StrictTypes {
				errors = append(errors, fmt.Sprintf("row 1: %v", err))
				return rowsInserted, rowsSkipped + 1, errors
			}
			errors = append(errors, fmt.Sprintf("row 1: %v (skipped)", err))
			rowsSkipped++
		} else {
			batch = append(batch, row)
		}
	}

	// Process remaining rows
	rowNum := 1
	if firstDataRow == nil {
		rowNum = 0 // Header was present
	}

	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors = append(errors, fmt.Sprintf("row %d: read error: %v", rowNum+1, err))
			rowsSkipped++
			continue
		}

		rowNum++

		// Convert and validate row
		row, err := convertRow(rec, colNames, colTypes, opts)
		if err != nil {
			if opts.StrictTypes {
				errors = append(errors, fmt.Sprintf("row %d: %v", rowNum, err))
				return rowsInserted, rowsSkipped + 1, errors
			}
			errors = append(errors, fmt.Sprintf("row %d: %v (skipped)", rowNum, err))
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
