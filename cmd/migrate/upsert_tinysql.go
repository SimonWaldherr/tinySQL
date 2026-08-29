package main

import (
	"context"
	"fmt"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// deleteChunkSize caps how many keys go into a single DELETE statement's
// WHERE clause. tinySQL has no bind parameters, so every key becomes a
// literal OR-clause in the SQL text; chunking keeps individual statements
// (and their parse trees) a bounded size regardless of how many rows a
// sync pass needs to remove.
const deleteChunkSize = 400

// writeKeyEquals appends "<col1>=<lit1> AND <col2>=<lit2> ..." to sb for the
// given key columns and their (already typed) values, in matching order.
// Column names are sanitized and values are embedded as SQL literals via
// formatValue, exactly like buildInsert/buildCreateTable do for every other
// tinySQL-target statement main.go builds — tinySQL has no bind parameters,
// and its identifiers are never quoted anywhere else in this package, so we
// don't quote them here either.
func writeKeyEquals(sb *strings.Builder, keyCols []string, keyVals []any) {
	for i, col := range keyCols {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(sanitizeColumnName(col))
		sb.WriteString("=")
		sb.WriteString(formatValue(keyVals[i]))
	}
}

// tinySQLAffectedCount reads the single {cell: n} row tinySQL's UPDATE and
// DELETE statements return (see internal/engine/exec_dml_update.go and
// exec_dml_delete.go) — e.g. {"updated": 3} or {"deleted": 0} — and returns
// n as an int64. Mirrors internal/driver/exec.go's affectedRows helper,
// which cmd/migrate cannot import (internal package).
func tinySQLAffectedCount(result *tinysql.ResultSet, cell string) int64 {
	if result == nil || len(result.Rows) != 1 {
		return 0
	}
	switch n := result.Rows[0][cell].(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

// upsertRowsIntoTinySQL applies rows to table in db, one statement per row:
// an UPDATE by key first, falling back to an INSERT only when that UPDATE
// affects zero rows (i.e. the row does not exist yet). tinySQL has no
// UPSERT/ON CONFLICT and no bind parameters, so both statements are built
// as literal SQL text via formatValue, the same way buildInsert does
// elsewhere in this package.
//
// keyCols names the row's key columns; colNames lists every column each
// row.Columns entry corresponds to, in the same order (key columns
// included, matching IncrementalRow.Columns' contract). Key columns are
// excluded from the UPDATE's SET list (they're already pinned by the WHERE
// clause) unless every column in colNames is a key column, in which case
// all of them are kept in SET so the statement stays syntactically valid.
//
// Each row's WHERE clause is built from row.KeyValues directly (the
// original typed key values IncrementalRow already carries), not by
// looking key columns up in row.Columns — this matches the contract
// documented on IncrementalRow in incremental.go.
//
// upserted counts every row that was either updated or inserted (i.e.
// len(rows) on full success); err is the first error encountered, at which
// point upserted reflects rows applied strictly before it.
func upsertRowsIntoTinySQL(ctx context.Context, db *tinysql.DB, tenant, table string, keyCols []string, colNames []string, rows []IncrementalRow) (upserted int64, err error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if len(keyCols) == 0 {
		return 0, fmt.Errorf("upsertRowsIntoTinySQL: keyCols must not be empty")
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

	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return upserted, err
		}
		if len(row.Columns) != len(colNames) {
			return upserted, fmt.Errorf("upsertRowsIntoTinySQL: row for key %q has %d columns, want %d", row.Key, len(row.Columns), len(colNames))
		}
		if len(row.KeyValues) != len(keyCols) {
			return upserted, fmt.Errorf("upsertRowsIntoTinySQL: row for key %q has %d key values, want %d", row.Key, len(row.KeyValues), len(keyCols))
		}

		var sb strings.Builder
		sb.WriteString("UPDATE ")
		sb.WriteString(table)
		sb.WriteString(" SET ")
		for i, idx := range setColIdx {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(sanitizeColumnName(colNames[idx]))
			sb.WriteString("=")
			sb.WriteString(formatValue(row.Columns[idx]))
		}
		sb.WriteString(" WHERE ")
		writeKeyEquals(&sb, keyCols, row.KeyValues)

		updateSQL := sb.String()
		stmt, perr := tinysql.ParseSQL(updateSQL)
		if perr != nil {
			return upserted, fmt.Errorf("parse update for table %s: %w", table, perr)
		}
		result, eerr := tinysql.Execute(ctx, db, tenant, stmt)
		if eerr != nil {
			return upserted, fmt.Errorf("execute update for table %s: %w", table, eerr)
		}

		if tinySQLAffectedCount(result, "updated") > 0 {
			upserted++
			continue
		}

		// UPDATE affected nothing: the row doesn't exist yet. INSERT is the
		// only write for it — never both, to avoid double-writing.
		//
		// Built as an AST rather than SQL text for the same reason
		// importFromExternal does: rendering a value into SQL cannot represent
		// everything a source database holds, and a non-finite float used to
		// fail here with `unknown column "Inf"`.
		if _, eerr := tinysql.Execute(ctx, db, tenant, buildInsertStmt(table, colNames, row.Columns)); eerr != nil {
			return upserted, fmt.Errorf("execute insert for table %s: %w", table, eerr)
		}
		upserted++
	}

	return upserted, nil
}

// deleteRowsFromTinySQL removes rows identified by keys from table in db,
// batched deleteChunkSize keys per DELETE statement to keep any single
// statement's WHERE clause a bounded size.
//
// Each entry in keys need only have KeyValues populated (in the same order
// as keyCols) — Key/Columns/Watermark, if set, are ignored. This lets
// callers pass either full IncrementalRows for rows they already have in
// hand, or bare IncrementalRow{KeyValues: ...} built from a deleted row's
// recovered key values (see decodeRowKey in incremental.go for how a bare
// toDeleteKeys entry's canonical key string is turned back into per-column
// values by a caller that needs to).
//
// The WHERE clause is a single OR-of-ANDs — "(k1=v1 AND k2=v2) OR (k1=v3
// AND k2=v4) OR ..." — which is correct for both single- and multi-column
// keys, so there is one implementation path rather than a single-column
// special case.
//
// deleted is the total affected-row count summed across every chunk; err
// is the first error encountered, at which point deleted reflects chunks
// applied strictly before it.
func deleteRowsFromTinySQL(ctx context.Context, db *tinysql.DB, tenant, table string, keyCols []string, keys []IncrementalRow) (deleted int64, err error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if len(keyCols) == 0 {
		return 0, fmt.Errorf("deleteRowsFromTinySQL: keyCols must not be empty")
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
		sb.WriteString("DELETE FROM ")
		sb.WriteString(table)
		sb.WriteString(" WHERE ")
		for i, k := range chunk {
			if len(k.KeyValues) != len(keyCols) {
				return deleted, fmt.Errorf("deleteRowsFromTinySQL: key %d has %d key values, want %d", start+i, len(k.KeyValues), len(keyCols))
			}
			if i > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteString("(")
			writeKeyEquals(&sb, keyCols, k.KeyValues)
			sb.WriteString(")")
		}

		deleteSQL := sb.String()
		stmt, perr := tinysql.ParseSQL(deleteSQL)
		if perr != nil {
			return deleted, fmt.Errorf("parse delete for table %s: %w", table, perr)
		}
		result, eerr := tinysql.Execute(ctx, db, tenant, stmt)
		if eerr != nil {
			return deleted, fmt.Errorf("execute delete for table %s: %w", table, eerr)
		}
		deleted += tinySQLAffectedCount(result, "deleted")
	}

	return deleted, nil
}
