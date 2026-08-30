package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// ============================================================================
// -mode=incremental orchestration for import-db and export-db.
//
// This file wires stages 2-5 (sync_state.go, incremental.go,
// upsert_tinysql.go, upsert_external.go) together into the two directions
// runImportDB/runExportDB support: external DB -> tinySQL (import) and
// tinySQL -> external DB (export). -mode=full (the default) never touches
// any of this -- runImportDB/runExportDB call importFromExternal/
// exportToExternal directly and unconditionally in that case, exactly as
// before this file existed.
// ============================================================================

// parseKeyCols splits a comma-separated -key-col flag value into trimmed,
// non-empty column names. An empty or all-whitespace input yields nil (no
// key columns), which callers treat as "use -allow-hash-identity instead".
func parseKeyCols(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// indexOfColumn returns the position of name within cols (case-insensitive),
// or -1 if it isn't present.
func indexOfColumn(cols []string, name string) int {
	for i, c := range cols {
		if strings.EqualFold(c, name) {
			return i
		}
	}
	return -1
}

// canonicalKeyPartToLiteral converts one decodeRowKey part back into a
// best-effort typed Go value, for use as an IncrementalRow.KeyValues entry
// when applying a delete for a row that is no longer present in the current
// source pull (see decodeRowKey in incremental.go -- this is exactly the
// situation it documents). canonicalKeyPart (sync_state.go) intentionally
// collapses every key type down to a string, so this is necessarily a
// best-effort reverse: values that round-trip through canonicalKeyPart's
// numeric formatting (int64, float64) are recovered as numbers, which
// matters for tinySQL's typed equality comparisons against an INT/FLOAT key
// column (a string literal would not compare equal to an int column there).
// The NULL sentinel is recovered as nil. Everything else -- including the
// literal text "true"/"false", indistinguishable here from a bool column's
// canonical form -- is passed through as a string; that is correct for TEXT
// key columns, and harmless for external targets since drivers like SQLite
// apply column-type affinity to bound parameters before comparing.
func canonicalKeyPartToLiteral(s string) any {
	if s == "\x00NULL" {
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// deletedRowsFromKeys turns the canonical key strings planIncrementalSync
// reports as toDeleteKeys into bare IncrementalRows carrying only
// KeyValues, suitable for deleteRowsFromTinySQL/deleteRowsFromExternal
// (both of which document that they only read .KeyValues off entries like
// these).
func deletedRowsFromKeys(keys []string) []IncrementalRow {
	out := make([]IncrementalRow, len(keys))
	for i, k := range keys {
		parts := decodeRowKey(k)
		vals := make([]any, len(parts))
		for j, p := range parts {
			vals[j] = canonicalKeyPartToLiteral(p)
		}
		out[i] = IncrementalRow{KeyValues: vals}
	}
	return out
}

// buildIncrementalRows shapes a raw column/row scan (in cols/rows form, as
// returned by fetchExternalRows or fetchTinySQLRows) into IncrementalRows
// keyed by keyCols, optionally carrying a watermark value read from
// watermarkCol (ignored when watermarkCol is "").
func buildIncrementalRows(cols []string, rows [][]any, keyCols []string, watermarkCol string) ([]IncrementalRow, error) {
	keyIdx := make([]int, len(keyCols))
	for i, kc := range keyCols {
		idx := indexOfColumn(cols, kc)
		if idx < 0 {
			return nil, fmt.Errorf("key column %q not found among columns %v", kc, cols)
		}
		keyIdx[i] = idx
	}
	wmIdx := -1
	if watermarkCol != "" {
		wmIdx = indexOfColumn(cols, watermarkCol)
		if wmIdx < 0 {
			return nil, fmt.Errorf("watermark column %q not found among columns %v", watermarkCol, cols)
		}
	}

	out := make([]IncrementalRow, len(rows))
	for i, row := range rows {
		keyVals := make([]any, len(keyCols))
		for j, idx := range keyIdx {
			keyVals[j] = row[idx]
		}
		var wm any
		if wmIdx >= 0 {
			wm = row[wmIdx]
		}
		out[i] = IncrementalRow{
			Key:       computeRowKey(keyVals),
			KeyValues: keyVals,
			Columns:   row,
			Watermark: wm,
		}
	}
	return out, nil
}

// currentKeysFromRows extracts the canonical key string for every row in
// rows, given the positions of the key columns within each row.
func currentKeysFromRows(rows [][]any, keyIdx []int) []string {
	keys := make([]string, len(rows))
	for i, row := range rows {
		vals := make([]any, len(keyIdx))
		for j, idx := range keyIdx {
			vals[j] = row[idx]
		}
		keys[i] = computeRowKey(vals)
	}
	return keys
}

// resolveKeyIdx maps keyCols onto their positions in cols, erroring if any
// key column isn't present.
func resolveKeyIdx(cols []string, keyCols []string) ([]int, error) {
	idx := make([]int, len(keyCols))
	for i, kc := range keyCols {
		p := indexOfColumn(cols, kc)
		if p < 0 {
			return nil, fmt.Errorf("key column %q not found among columns %v", kc, cols)
		}
		idx[i] = p
	}
	return idx, nil
}

// fetchExternalRows runs query (with args bound positionally) against extDB
// and returns its column names, column types, and every row's values in
// column order.
func fetchExternalRows(ctx context.Context, extDB *sql.DB, query string, args ...any) (cols []string, colTypes []*sql.ColumnType, rowsOut [][]any, err error) {
	rows, err := extDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("external query failed: %w", err)
	}
	defer rows.Close()

	cols, err = rows.Columns()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get columns: %w", err)
	}
	colTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get column types: %w", err)
	}

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, nil, fmt.Errorf("scan row: %w", err)
		}
		rowsOut = append(rowsOut, values)
	}
	return cols, colTypes, rowsOut, rows.Err()
}

// fetchTinySQLRows runs query against db and returns its column names and
// every row's values in column order.
func fetchTinySQLRows(ctx context.Context, db *tinysql.DB, tenant, query string) (cols []string, rowsOut [][]any, err error) {
	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		return nil, nil, fmt.Errorf("parse error: %w", err)
	}
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("execute error: %w", err)
	}
	if result == nil {
		return nil, nil, nil
	}
	cols = result.Cols
	rowsOut = make([][]any, len(result.Rows))
	for i, row := range result.Rows {
		values := make([]any, len(cols))
		for j, c := range cols {
			values[j] = row[strings.ToLower(c)]
		}
		rowsOut[i] = values
	}
	return cols, rowsOut, nil
}

// buildCreateTableIfNotExists mirrors buildCreateTable exactly, except for
// the added "IF NOT EXISTS" -- kept as a separate function (rather than a
// parameter on buildCreateTable) so full mode's call to buildCreateTable is
// never touched by this file.
func buildCreateTableIfNotExists(tableName string, cols []string, colTypes []*sql.ColumnType) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sanitizeColumnName(col))
		sb.WriteString(" ")
		sb.WriteString(mapExternalType(colTypes[i]))
	}
	sb.WriteString(")")
	return sb.String()
}

// importDBIncrementalConfig collects the resolved settings
// runImportDBIncremental needs, once runImportDB has parsed and validated
// its flags.
type importDBIncrementalConfig struct {
	dsn               string
	sourceTable       string
	targetTable       string
	keyCols           []string
	allowHashIdentity bool
	watermarkCol      string
	stateFilePath     string
	dbFile            string
	verbose           bool
}

// runImportDBIncremental is import-db's -mode=incremental entry point: it
// owns opening the external and tinySQL sides (including -db-file
// load/save, matching runImportDB's full-mode behavior) and delegates the
// actual sync to runIncrementalImport.
func runImportDBIncremental(cfg importDBIncrementalConfig) error {
	driver, connStr := parseDSN(cfg.dsn)
	extDB, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", driver, err)
	}
	defer extDB.Close()
	if err := extDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping %s: %v", driver, err)
	}

	ctx := context.Background()
	tenant := "default"

	var db *tinysql.DB
	if cfg.dbFile != "" {
		if _, statErr := os.Stat(cfg.dbFile); statErr == nil {
			db, err = tinysql.LoadFromFile(cfg.dbFile)
			if err != nil {
				return fmt.Errorf("failed to load db file %s: %v", cfg.dbFile, err)
			}
		} else {
			db = tinysql.NewDB()
		}
	} else {
		db = tinysql.NewDB()
	}

	runErr := runIncrementalImport(ctx, db, tenant, extDB, driver, cfg)

	if cfg.dbFile != "" {
		saveErr := checkpointAndClose(db, cfg.dbFile)
		if saveErr != nil && runErr == nil {
			runErr = fmt.Errorf("failed to save db file %s: %v", cfg.dbFile, saveErr)
		}
	}

	return runErr
}

// runIncrementalImport performs one incremental sync pass from extDB into
// db: it fetches the source's current rows, plans the sync (stage 3),
// applies it to the tinySQL target (stage 4), and persists the resulting
// sync state (stage 2).
func runIncrementalImport(ctx context.Context, db *tinysql.DB, tenant string, extDB *sql.DB, driver string, cfg importDBIncrementalConfig) error {
	fullCols, colTypes, fullRows, err := fetchExternalRows(ctx, extDB, fmt.Sprintf("SELECT * FROM %s", cfg.sourceTable))
	if err != nil {
		return fmt.Errorf("failed to read source table %s: %w", cfg.sourceTable, err)
	}

	keyCols := cfg.keyCols
	if len(keyCols) == 0 {
		if !cfg.allowHashIdentity {
			return fmt.Errorf("no usable key columns for table %s (pass -key-col or -allow-hash-identity)", cfg.sourceTable)
		}
		keyCols = fullCols
	}
	keyIdx, err := resolveKeyIdx(fullCols, keyCols)
	if err != nil {
		return err
	}
	hasWatermark := cfg.watermarkCol != ""
	if hasWatermark && indexOfColumn(fullCols, cfg.watermarkCol) < 0 {
		return fmt.Errorf("watermark column %q not found in source table %s (columns: %v)", cfg.watermarkCol, cfg.sourceTable, fullCols)
	}

	if err := ensureTinySQLTableFromExternal(ctx, db, tenant, cfg.targetTable, fullCols, colTypes); err != nil {
		return err
	}

	statePath := cfg.stateFilePath
	if statePath == "" {
		targetID := cfg.dbFile
		if targetID == "" {
			targetID = "tinysql-memory"
		}
		statePath = defaultStateFilePath(cfg.dsn, targetID, cfg.targetTable, keyCols)
	}

	prev, err := loadSyncState(statePath)
	if err != nil {
		return fmt.Errorf("failed to load sync state: %w", err)
	}

	currentKeys := currentKeysFromRows(fullRows, keyIdx)

	var changedRows []IncrementalRow
	if hasWatermark && prev.Watermark != nil {
		wmVal, werr := prev.Watermark.Value()
		if werr != nil {
			return fmt.Errorf("failed to decode persisted watermark: %w", werr)
		}
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s > %s", cfg.sourceTable, cfg.watermarkCol, placeholderFor(driver, 0))
		changedCols, _, changedRawRows, ferr := fetchExternalRows(ctx, extDB, query, wmVal)
		if ferr != nil {
			return fmt.Errorf("failed to fetch changed rows: %w", ferr)
		}
		changedRows, err = buildIncrementalRows(changedCols, changedRawRows, keyCols, cfg.watermarkCol)
	} else {
		changedRows, err = buildIncrementalRows(fullCols, fullRows, keyCols, cfg.watermarkCol)
	}
	if err != nil {
		return err
	}

	toUpsert, toDeleteKeys, next := planIncrementalSync(prev, currentKeys, changedRows, hasWatermark)

	upserted, err := upsertRowsIntoTinySQL(ctx, db, tenant, cfg.targetTable, keyCols, fullCols, toUpsert)
	if err != nil {
		return fmt.Errorf("upsert into %s failed after %d rows: %w", cfg.targetTable, upserted, err)
	}

	deleted, err := deleteRowsFromTinySQL(ctx, db, tenant, cfg.targetTable, keyCols, deletedRowsFromKeys(toDeleteKeys))
	if err != nil {
		return fmt.Errorf("delete from %s failed after %d rows: %w", cfg.targetTable, deleted, err)
	}

	next.UpdatedAt = time.Now()
	if err := saveSyncState(statePath, next); err != nil {
		return fmt.Errorf("failed to save sync state: %w", err)
	}

	if cfg.verbose {
		fmt.Fprintf(os.Stderr, "✓ Incremental import into '%s': %d upserted, %d deleted (state: %s)\n", cfg.targetTable, upserted, deleted, statePath)
	}

	return nil
}

// ensureTinySQLTableFromExternal creates targetTable in db if it doesn't
// already exist, with a column for every entry in cols typed via
// mapExternalType(colTypes[i]) -- the same type mapping importFromExternal
// uses for a fresh full-mode import.
func ensureTinySQLTableFromExternal(ctx context.Context, db *tinysql.DB, tenant, targetTable string, cols []string, colTypes []*sql.ColumnType) error {
	createSQL := buildCreateTableIfNotExists(targetTable, cols, colTypes)
	stmt, err := tinysql.ParseSQL(createSQL)
	if err != nil {
		return fmt.Errorf("failed to parse CREATE TABLE for %s: %w", targetTable, err)
	}
	if _, err := tinysql.Execute(ctx, db, tenant, stmt); err != nil {
		return fmt.Errorf("failed to create table %s: %w", targetTable, err)
	}
	return nil
}

// exportDBIncrementalConfig collects the resolved settings
// runExportDBIncremental needs, once runExportDB has parsed and validated
// its flags.
type exportDBIncrementalConfig struct {
	dsn               string
	table             string
	targetTable       string
	keyCols           []string
	allowHashIdentity bool
	watermarkCol      string
	stateFilePath     string
	filesFlag         string
	verbose           bool
}

// runExportDBIncremental is export-db's -mode=incremental entry point: it
// owns opening the external side and building the tinySQL source side
// (including -files loading, matching runExportDB's full-mode behavior) and
// delegates the actual sync to runIncrementalExport.
func runExportDBIncremental(cfg exportDBIncrementalConfig) error {
	driver, connStr := parseDSN(cfg.dsn)
	extDB, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", driver, err)
	}
	defer extDB.Close()
	if err := extDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping %s: %v", driver, err)
	}

	ctx := context.Background()
	tenant := "default"
	db := tinysql.NewDB()

	if cfg.filesFlag != "" {
		for _, f := range strings.Split(cfg.filesFlag, ",") {
			f = strings.TrimSpace(f)
			tableName := tableNameFromFile(f)
			if err := importFileToTinySQL(db, ctx, tenant, f, tableName, true, cfg.verbose); err != nil {
				return fmt.Errorf("failed to load %s: %v", f, err)
			}
		}
	}

	return runIncrementalExport(ctx, db, tenant, extDB, driver, cfg)
}

// runIncrementalExport performs one incremental sync pass from db into
// extDB: it fetches the tinySQL source's current rows, plans the sync
// (stage 3), applies it to the external target (stage 5), and persists the
// resulting sync state (stage 2).
func runIncrementalExport(ctx context.Context, db *tinysql.DB, tenant string, extDB *sql.DB, driver string, cfg exportDBIncrementalConfig) error {
	fullCols, fullRows, err := fetchTinySQLRows(ctx, db, tenant, fmt.Sprintf("SELECT * FROM %s", cfg.table))
	if err != nil {
		return fmt.Errorf("failed to read source table %s: %w", cfg.table, err)
	}

	keyCols := cfg.keyCols
	if len(keyCols) == 0 {
		if !cfg.allowHashIdentity {
			return fmt.Errorf("no usable key columns for table %s (pass -key-col or -allow-hash-identity)", cfg.table)
		}
		keyCols = fullCols
	}
	keyIdx, err := resolveKeyIdx(fullCols, keyCols)
	if err != nil {
		return err
	}
	hasWatermark := cfg.watermarkCol != ""
	if hasWatermark && indexOfColumn(fullCols, cfg.watermarkCol) < 0 {
		return fmt.Errorf("watermark column %q not found in source table %s (columns: %v)", cfg.watermarkCol, cfg.table, fullCols)
	}

	if err := ensureExternalTableForExport(extDB, driver, cfg.targetTable, fullCols); err != nil {
		return err
	}

	statePath := cfg.stateFilePath
	if statePath == "" {
		statePath = defaultStateFilePath("tinysql", cfg.dsn, cfg.targetTable, keyCols)
	}

	prev, err := loadSyncState(statePath)
	if err != nil {
		return fmt.Errorf("failed to load sync state: %w", err)
	}

	currentKeys := currentKeysFromRows(fullRows, keyIdx)

	var changedRows []IncrementalRow
	if hasWatermark && prev.Watermark != nil {
		wmVal, werr := prev.Watermark.Value()
		if werr != nil {
			return fmt.Errorf("failed to decode persisted watermark: %w", werr)
		}
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s > %s", cfg.table, cfg.watermarkCol, formatValue(wmVal))
		changedCols, changedRawRows, ferr := fetchTinySQLRows(ctx, db, tenant, query)
		if ferr != nil {
			return fmt.Errorf("failed to fetch changed rows: %w", ferr)
		}
		changedRows, err = buildIncrementalRows(changedCols, changedRawRows, keyCols, cfg.watermarkCol)
	} else {
		changedRows, err = buildIncrementalRows(fullCols, fullRows, keyCols, cfg.watermarkCol)
	}
	if err != nil {
		return err
	}

	toUpsert, toDeleteKeys, next := planIncrementalSync(prev, currentKeys, changedRows, hasWatermark)

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	upserted, err := upsertRowsIntoExternal(ctx, tx, driver, cfg.targetTable, keyCols, fullCols, toUpsert)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("upsert into %s failed after %d rows: %w", cfg.targetTable, upserted, err)
	}

	deleted, err := deleteRowsFromExternal(ctx, tx, driver, cfg.targetTable, keyCols, deletedRowsFromKeys(toDeleteKeys))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("delete from %s failed after %d rows: %w", cfg.targetTable, deleted, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	next.UpdatedAt = time.Now()
	if err := saveSyncState(statePath, next); err != nil {
		return fmt.Errorf("failed to save sync state: %w", err)
	}

	if cfg.verbose {
		fmt.Fprintf(os.Stderr, "✓ Incremental export to '%s': %d upserted, %d deleted (state: %s)\n", cfg.targetTable, upserted, deleted, statePath)
	}

	return nil
}

// ensureExternalTableForExport creates targetTable in extDB if it doesn't
// already exist, using buildExternalCreateTable -- the same helper (and
// TEXT-for-everything convention) exportToExternal uses for a fresh
// full-mode export.
func ensureExternalTableForExport(extDB *sql.DB, driver, targetTable string, cols []string) error {
	createSQL := buildExternalCreateTable(driver, targetTable, cols)
	if _, err := extDB.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create target table %s: %w", targetTable, err)
	}
	return nil
}
