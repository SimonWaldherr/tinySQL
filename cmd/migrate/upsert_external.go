package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// upsertRowsIntoExternal applies rows to table in the external database tx
// belongs to, one round-trip per row: a parameterized UPDATE by key first,
// falling back to a parameterized INSERT only when that UPDATE affects zero
// rows (i.e. the row does not exist yet). This mirrors
// upsertRowsIntoTinySQL's UPDATE-then-INSERT-if-zero-rows-affected strategy
// (upsert_tinysql.go), but targets a *sql.Tx against an external
// database/sql driver instead of tinySQL: every value is passed as a bind
// parameter (via placeholderFor) rather than embedded as a SQL literal, and
// every identifier is quoted via quoteIdentifier, matching
// exportToExternal's existing conventions for this package's external-DB
// code path.
//
// keyCols names the row's key columns; colNames lists every column each
// row.Columns entry corresponds to, in the same order (key columns
// included, matching IncrementalRow.Columns' contract). Key columns are
// excluded from the UPDATE's SET list unless every column in colNames is a
// key column, in which case all of them are kept in SET (a no-op write) so
// the statement stays syntactically valid -- same rule as
// upsertRowsIntoTinySQL.
//
// Each row's WHERE clause is built from row.KeyValues directly (not looked
// up in row.Columns), matching the IncrementalRow contract documented in
// incremental.go.
//
// upserted counts every row that was either updated or inserted; err is the
// first error encountered, at which point upserted reflects rows applied
// strictly before it. The caller owns tx: on error it is the caller's
// responsibility to roll back (or otherwise dispose of) the transaction --
// this function never commits or rolls back itself.
func upsertRowsIntoExternal(ctx context.Context, tx *sql.Tx, driver, table string, keyCols, colNames []string, rows []IncrementalRow) (upserted int64, err error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if len(keyCols) == 0 {
		return 0, fmt.Errorf("upsertRowsIntoExternal: keyCols must not be empty")
	}

	keySet := make(map[string]struct{}, len(keyCols))
	for _, k := range keyCols {
		keySet[strings.ToLower(k)] = struct{}{}
	}

	setColIdx := make([]int, 0, len(colNames))
	for i, c := range colNames {
		if _, isKey := keySet[strings.ToLower(c)]; !isKey {
			setColIdx = append(setColIdx, i)
		}
	}
	// Every column is a key column: keep them all in SET so the UPDATE
	// statement doesn't end up with an empty (invalid) SET list. Setting a
	// key column to its own current value is a no-op, so this is safe.
	if len(setColIdx) == 0 {
		setColIdx = make([]int, len(colNames))
		for i := range colNames {
			setColIdx[i] = i
		}
	}

	quotedTable := quoteIdentifier(driver, table)

	// Pre-quote colNames once; both the UPDATE's SET list and the INSERT
	// fallback need it and colNames/keyCols don't change across rows.
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quoteIdentifier(driver, c)
	}
	quotedKeyCols := make([]string, len(keyCols))
	for i, c := range keyCols {
		quotedKeyCols[i] = quoteIdentifier(driver, c)
	}

	insertPlaceholders := make([]string, len(colNames))
	for i := range colNames {
		insertPlaceholders[i] = placeholderFor(driver, i)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTable, strings.Join(quotedCols, ", "), strings.Join(insertPlaceholders, ", "))

	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return upserted, err
		}
		if len(row.Columns) != len(colNames) {
			return upserted, fmt.Errorf("upsertRowsIntoExternal: row for key %q has %d columns, want %d", row.Key, len(row.Columns), len(colNames))
		}
		if len(row.KeyValues) != len(keyCols) {
			return upserted, fmt.Errorf("upsertRowsIntoExternal: row for key %q has %d key values, want %d", row.Key, len(row.KeyValues), len(keyCols))
		}

		var sb strings.Builder
		args := make([]any, 0, len(setColIdx)+len(keyCols))
		ph := 0

		sb.WriteString("UPDATE ")
		sb.WriteString(quotedTable)
		sb.WriteString(" SET ")
		for i, idx := range setColIdx {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quotedCols[idx])
			sb.WriteString("=")
			sb.WriteString(placeholderFor(driver, ph))
			ph++
			args = append(args, row.Columns[idx])
		}
		sb.WriteString(" WHERE ")
		for i, col := range quotedKeyCols {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(col)
			sb.WriteString("=")
			sb.WriteString(placeholderFor(driver, ph))
			ph++
			args = append(args, row.KeyValues[i])
		}

		result, eerr := tx.ExecContext(ctx, sb.String(), args...)
		if eerr != nil {
			return upserted, fmt.Errorf("execute update for table %s: %w", table, eerr)
		}
		affected, raerr := result.RowsAffected()
		if raerr != nil {
			return upserted, fmt.Errorf("rows affected for update on table %s: %w", table, raerr)
		}

		if affected > 0 {
			upserted++
			continue
		}

		// UPDATE affected nothing: the row doesn't exist yet. INSERT is the
		// only write for it -- never both, to avoid double-writing.
		if _, eerr := tx.ExecContext(ctx, insertSQL, row.Columns...); eerr != nil {
			return upserted, fmt.Errorf("execute insert for table %s: %w", table, eerr)
		}
		upserted++
	}

	return upserted, nil
}

// deleteRowsFromExternal removes rows identified by keys from table in the
// external database tx belongs to, batched deleteChunkSize (defined in
// upsert_tinysql.go) keys per DELETE statement to keep any single
// statement's parameter count and WHERE clause a bounded size.
//
// Each entry in keys need only have KeyValues populated (in the same order
// as keyCols) -- Key/Columns/Watermark, if set, are ignored, matching
// deleteRowsFromTinySQL's contract.
//
// For a single-column key, the WHERE clause is a single "<col> IN (?, ?,
// ...)"; for a composite key it's an OR-of-ANDs -- "(k1=? AND k2=?) OR
// (k1=? AND k2=?) OR ..." -- mirroring deleteRowsFromTinySQL's shape but
// parameterized instead of embedding literals.
//
// deleted is the total affected-row count summed across every chunk; err is
// the first error encountered, at which point deleted reflects chunks
// applied strictly before it. The caller owns tx.
func deleteRowsFromExternal(ctx context.Context, tx *sql.Tx, driver, table string, keyCols []string, keys []IncrementalRow) (deleted int64, err error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if len(keyCols) == 0 {
		return 0, fmt.Errorf("deleteRowsFromExternal: keyCols must not be empty")
	}

	quotedTable := quoteIdentifier(driver, table)
	quotedKeyCols := make([]string, len(keyCols))
	for i, c := range keyCols {
		quotedKeyCols[i] = quoteIdentifier(driver, c)
	}

	for start := 0; start < len(keys); start += deleteChunkSize {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}

		end := start + deleteChunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]

		var sb strings.Builder
		args := make([]any, 0, len(chunk)*len(keyCols))
		ph := 0

		sb.WriteString("DELETE FROM ")
		sb.WriteString(quotedTable)
		sb.WriteString(" WHERE ")

		if len(keyCols) == 1 {
			sb.WriteString(quotedKeyCols[0])
			sb.WriteString(" IN (")
			for i, k := range chunk {
				if len(k.KeyValues) != 1 {
					return deleted, fmt.Errorf("deleteRowsFromExternal: key %d has %d key values, want 1", start+i, len(k.KeyValues))
				}
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(placeholderFor(driver, ph))
				ph++
				args = append(args, k.KeyValues[0])
			}
			sb.WriteString(")")
		} else {
			for i, k := range chunk {
				if len(k.KeyValues) != len(keyCols) {
					return deleted, fmt.Errorf("deleteRowsFromExternal: key %d has %d key values, want %d", start+i, len(k.KeyValues), len(keyCols))
				}
				if i > 0 {
					sb.WriteString(" OR ")
				}
				sb.WriteString("(")
				for j, col := range quotedKeyCols {
					if j > 0 {
						sb.WriteString(" AND ")
					}
					sb.WriteString(col)
					sb.WriteString("=")
					sb.WriteString(placeholderFor(driver, ph))
					ph++
					args = append(args, k.KeyValues[j])
				}
				sb.WriteString(")")
			}
		}

		result, eerr := tx.ExecContext(ctx, sb.String(), args...)
		if eerr != nil {
			return deleted, fmt.Errorf("execute delete for table %s: %w", table, eerr)
		}
		affected, raerr := result.RowsAffected()
		if raerr != nil {
			return deleted, fmt.Errorf("rows affected for delete on table %s: %w", table, raerr)
		}
		deleted += affected
	}

	return deleted, nil
}
