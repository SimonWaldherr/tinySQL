package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const externalImportBatchRows = 256
const externalImportBatchBytes = 1 << 20

type externalImportRow struct {
	values []any
	number int
}

func externalImportColumns(cols []string, types []*sql.ColumnType) ([]string, []string, error) {
	names, kinds := make([]string, len(cols)), make([]string, len(cols))
	seen := make(map[string]bool, len(cols))
	for i, col := range cols {
		name := sanitizeColumnName(col)
		if name == "" || seen[strings.ToLower(name)] {
			return nil, nil, fmt.Errorf("source column %q has an empty or duplicate normalized name %q; use explicit query aliases", col, name)
		}
		seen[strings.ToLower(name)] = true
		names[i], kinds[i] = name, mapExternalType(types[i])
	}
	return names, kinds, nil
}

func externalTinyType(kind string) tinysql.ColType {
	switch kind {
	case "INT":
		return tinysql.IntType
	case "FLOAT":
		return tinysql.FloatType
	case "BOOL":
		return tinysql.BoolType
	case "JSON":
		return tinysql.JsonType
	case "DECIMAL":
		return tinysql.DecimalType
	case "BLOB":
		return tinysql.BlobType
	default:
		return tinysql.TextType
	}
}

// Scan into *any gives ownership of byte slices to the caller. Keep each
// row's value slice separate from the reusable scan destinations.
func normalizeExternalRow(values []any, kinds []string) ([]any, int, error) {
	owned := make([]any, len(values))
	size := len(values) * 16
	for i, v := range values {
		if bytes, ok := v.([]byte); ok && kinds[i] != "BLOB" {
			v = string(bytes)
		}
		if v != nil && kinds[i] == "DECIMAL" {
			var ok bool
			v, ok = new(big.Rat).SetString(fmt.Sprint(v))
			if !ok {
				return nil, 0, fmt.Errorf("column %d: invalid decimal %v", i+1, values[i])
			}
		}
		owned[i] = v
		switch value := v.(type) {
		case string:
			size += len(value)
		case []byte:
			size += len(value)
		case *big.Rat:
			size += (value.Num().BitLen()+value.Denom().BitLen())/8 + 32
		}
	}
	return owned, size, nil
}

func externalImportSkipError(stats importStats, table string) error {
	if stats.Skipped > importMaxSkippedRows {
		return fmt.Errorf("import into %s aborted: more than %d source rows could not be imported (last: %s)", table, importMaxSkippedRows, stats.Errors[len(stats.Errors)-1])
	}
	return nil
}

func insertExternalBatch(ctx context.Context, db *tinysql.DB, tenant, table string, names []string, batch []externalImportRow, stats *importStats) error {
	if len(batch) == 0 {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	builder := tinysql.InsertInto(table).Columns(names...)
	literals := make([]tinysql.ExprBuilder, len(names))
	for _, row := range batch {
		for i, v := range row.values {
			literals[i] = tinysql.Val(v)
		}
		builder.Values(literals...)
	}
	if _, err := tinysql.Execute(ctx, db, tenant, builder.Build()); err == nil {
		stats.Imported += len(batch)
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Execute rolls back a failed statement atomically. Retry individual rows
	// in source order to retain the existing partial-import/error accounting.
	for _, row := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, v := range row.values {
			literals[i] = tinysql.Val(v)
		}
		stmt := tinysql.InsertInto(table).Columns(names...).Values(literals...).Build()
		if _, err := tinysql.Execute(ctx, db, tenant, stmt); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			stats.note(row.number, "insert", err)
			if err := externalImportSkipError(*stats, table); err != nil {
				return err
			}
		} else {
			stats.Imported++
		}
	}
	return nil
}
