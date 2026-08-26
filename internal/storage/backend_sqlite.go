//go:build sqliteimport && !js && !wasm && !baremetal

// Package storage - ModeSQLite backend.
//
// What: A StorageBackend that persists every tinySQL table as a native table
// in a real SQLite database file, via the pure-Go modernc.org/sqlite driver
// (already a project dependency for MBTiles/SQLite import — see
// internal/importer/mbtiles_open.go).
// How: A small "__tinysql_meta" table records each table's schema (as JSON,
// reusing the same diskColumn/SecondaryIndex/TableStats shapes ModeJSON
// already serializes), plus the name of the real SQL table holding its rows.
// Column values that map cleanly onto SQLite's INTEGER/REAL/TEXT/BLOB storage
// classes are stored natively; everything else (Decimal, UUID, time values,
// JSON/vector/geometry columns, ...) is JSON-encoded to TEXT, matching
// ModeJSON's existing (and already accepted) lossy-to-string behavior for
// those types.
// Why: Lets a tinySQL database be opened, inspected, and queried by any
// SQLite tool — sqlite3, DB Browser for SQLite, etc. — instead of only by
// tinySQL itself, at the cost of requiring the sqliteimport build tag.
package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// SQLiteBackend implements StorageBackend on top of a live SQLite database
// file. Safe for concurrent use: every method takes mu, mirroring the
// coarse per-backend locking DiskBackend already uses.
type SQLiteBackend struct {
	mu       sync.Mutex
	db       *sql.DB
	path     string
	readOnly bool
	stats    BackendStats
}

// sqliteMetaRow mirrors diskTable's schema/metadata fields (see
// disk_format.go) for one table, stored as a single row of
// "__tinysql_meta" — Cols/Indexes/Stats are JSON-encoded exactly as
// ModeJSON already encodes them, so this reuses an already-tested shape
// instead of inventing a new one.
type sqliteMetaRow struct {
	IsTemp        bool
	Version       int
	Cols          []diskColumn
	Indexes       map[string]*SecondaryIndex
	Stats         *TableStats
	FTSIndexes    map[string]*FTSIndex
	StructVersion int
	DataTable     string
}

// NewSQLiteBackend opens (creating if necessary) a SQLite database file at
// path and returns a StorageBackend backed by it.
func NewSQLiteBackend(path string) (*SQLiteBackend, error) {
	if path == "" {
		return nil, fmt.Errorf("ModeSQLite requires a Path")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	// A single physical connection: modernc's SQLite driver serializes
	// writers internally, and tinySQL's own backend mutex already serializes
	// callers, so a pool of connections would only add contention without
	// adding concurrency.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite db: set journal_mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite db: set synchronous: %w", err)
	}
	const createMeta = `CREATE TABLE IF NOT EXISTS "__tinysql_meta" (
		tenant TEXT NOT NULL,
		name TEXT NOT NULL,
		is_temp INTEGER NOT NULL,
		version INTEGER NOT NULL,
		cols_json TEXT NOT NULL,
		indexes_json TEXT,
		stats_json TEXT,
		fts_indexes_json TEXT,
		struct_version INTEGER NOT NULL DEFAULT 0,
		data_table TEXT NOT NULL UNIQUE,
		PRIMARY KEY (tenant, name)
	)`
	if _, err := db.Exec(createMeta); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite db: create meta table: %w", err)
	}
	// Existing SQLite databases predate the persisted FTS metadata. SQLite has
	// no ADD COLUMN IF NOT EXISTS, so inspect the schema before migrating.
	var hasFTSIndexes, hasStructVersion bool
	rows, err := db.Query(`PRAGMA table_info("__tinysql_meta")`)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite db: inspect meta table: %w", err)
	}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			_ = db.Close()
			return nil, fmt.Errorf("open sqlite db: inspect meta column: %w", err)
		}
		switch name {
		case "fts_indexes_json":
			hasFTSIndexes = true
		case "struct_version":
			hasStructVersion = true
		}
	}
	_ = rows.Close()
	if !hasFTSIndexes {
		if _, err := db.Exec(`ALTER TABLE "__tinysql_meta" ADD COLUMN fts_indexes_json TEXT`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open sqlite db: add FTS metadata: %w", err)
		}
	}
	if !hasStructVersion {
		if _, err := db.Exec(`ALTER TABLE "__tinysql_meta" ADD COLUMN struct_version INTEGER NOT NULL DEFAULT 0`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open sqlite db: add structure version: %w", err)
		}
	}
	return &SQLiteBackend{db: db, path: path}, nil
}

// SetReadOnly toggles whether SaveTable/DeleteTable are rejected, matching
// DiskBackend/HybridBackend's SetReadOnly (see open.go's ReadOnly wiring).
func (b *SQLiteBackend) SetReadOnly(ro bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readOnly = ro
}

// quoteIdent double-quotes a SQL identifier, doubling any embedded quote —
// standard ANSI SQL identifier escaping, so any table/tenant name is safe to
// splice into DDL/DML text (SQLite has no parameter-binding syntax for
// identifiers, only for values).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (b *SQLiteBackend) loadMeta(tenant, name string) (*sqliteMetaRow, error) {
	row := b.db.QueryRow(
		`SELECT is_temp, version, struct_version, cols_json, indexes_json, stats_json, fts_indexes_json, data_table FROM "__tinysql_meta" WHERE tenant = ? AND name = ?`,
		tenant, name,
	)
	var (
		isTemp                                 int
		version                                int
		colsJSON                               string
		indexesJSON, statsJSON, ftsIndexesJSON sql.NullString
		structVersion                          int
		dataTable                              string
	)
	if err := row.Scan(&isTemp, &version, &structVersion, &colsJSON, &indexesJSON, &statsJSON, &ftsIndexesJSON, &dataTable); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	meta := &sqliteMetaRow{IsTemp: isTemp != 0, Version: version, StructVersion: structVersion, DataTable: dataTable}
	if err := json.Unmarshal([]byte(colsJSON), &meta.Cols); err != nil {
		return nil, fmt.Errorf("decode column metadata for %s.%s: %w", tenant, name, err)
	}
	if indexesJSON.Valid && indexesJSON.String != "" {
		if err := json.Unmarshal([]byte(indexesJSON.String), &meta.Indexes); err != nil {
			return nil, fmt.Errorf("decode index metadata for %s.%s: %w", tenant, name, err)
		}
	}
	if statsJSON.Valid && statsJSON.String != "" {
		if err := json.Unmarshal([]byte(statsJSON.String), &meta.Stats); err != nil {
			return nil, fmt.Errorf("decode stats metadata for %s.%s: %w", tenant, name, err)
		}
	}
	if ftsIndexesJSON.Valid && ftsIndexesJSON.String != "" {
		if err := json.Unmarshal([]byte(ftsIndexesJSON.String), &meta.FTSIndexes); err != nil {
			return nil, fmt.Errorf("decode FTS index metadata for %s.%s: %w", tenant, name, err)
		}
	}
	return meta, nil
}

// dataTableNameFor picks the real SQL table name for (tenant, name): the bare
// table name for the default tenant (the common case, so a single-tenant
// database reads naturally in any SQLite tool), tenant-prefixed for others,
// and a numeric-suffixed fallback in the rare case that name is already
// taken by a different (tenant, name) pair.
func (b *SQLiteBackend) dataTableNameFor(tenant, name string) (string, error) {
	base := name
	if tenant != "" && tenant != "default" {
		base = tenant + "__" + name
	}
	candidate := base
	for suffix := 0; ; suffix++ {
		if suffix > 0 {
			candidate = fmt.Sprintf("%s__%d", base, suffix)
		}
		var existingTenant, existingName string
		err := b.db.QueryRow(`SELECT tenant, name FROM "__tinysql_meta" WHERE data_table = ?`, candidate).Scan(&existingTenant, &existingName)
		if errors.Is(err, sql.ErrNoRows) {
			return candidate, nil
		}
		if err != nil {
			return "", err
		}
		if existingTenant == tenant && existingName == name {
			return candidate, nil
		}
	}
}

// LoadTable retrieves a table from the SQLite file, or (nil, nil) if it has
// no meta row.
func (b *SQLiteBackend) LoadTable(tenant, name string) (*Table, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	meta, err := b.loadMeta(tenant, name)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}

	cols := make([]Column, len(meta.Cols))
	for i, c := range meta.Cols {
		cols[i] = Column(c)
	}
	t := NewTable(name, cols, meta.IsTemp)
	t.Version = meta.Version
	t.structVersion = meta.StructVersion
	t.Indexes = cloneSecondaryIndexes(meta.Indexes)
	t.FTSIndexes = cloneFTSIndexes(meta.FTSIndexes)
	t.Stats = cloneTableStats(meta.Stats)

	rows, err := b.db.Query(fmt.Sprintf(`SELECT * FROM %s ORDER BY "__seq"`, quoteIdent(meta.DataTable)))
	if err != nil {
		return nil, fmt.Errorf("load table %s.%s: %w", tenant, name, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		// __seq (first column) plus one destination per data column.
		dest := make([]any, len(cols)+1)
		raw := make([]any, len(cols)+1)
		for i := range dest {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("load table %s.%s: scan row: %w", tenant, name, err)
		}
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = decodeSQLiteCell(c.Type, raw[i+1])
		}
		t.Rows = append(t.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load table %s.%s: %w", tenant, name, err)
	}
	return t, nil
}

// SaveTable persists t's full current state as dt's authoritative snapshot,
// replacing whatever the backing SQL table previously held — the same
// whole-table-rewrite semantics DiskBackend/JSONBackend's SaveTable already
// has.
func (b *SQLiteBackend) SaveTable(tenant string, t *Table) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.readOnly {
		return ErrReadOnlyStorage
	}

	existing, err := b.loadMeta(tenant, t.Name)
	if err != nil {
		return err
	}
	dataTable := ""
	if existing != nil {
		dataTable = existing.DataTable
	} else {
		dataTable, err = b.dataTableNameFor(tenant, t.Name)
		if err != nil {
			return err
		}
	}

	colsJSON, err := json.Marshal(diskColumnsOf(t.Cols))
	if err != nil {
		return fmt.Errorf("save table %s.%s: encode columns: %w", tenant, t.Name, err)
	}
	// t.Indexes is JSON-marshaled directly below (json.Marshal only sees the
	// exported Entries field, never the runtime-only skip list), so it must
	// be synced from the live skip list right now.
	for _, idx := range t.Indexes {
		idx.materialize()
	}
	indexesJSON, err := json.Marshal(t.Indexes)
	if err != nil {
		return fmt.Errorf("save table %s.%s: encode indexes: %w", tenant, t.Name, err)
	}
	statsJSON, err := json.Marshal(t.Stats)
	if err != nil {
		return fmt.Errorf("save table %s.%s: encode stats: %w", tenant, t.Name, err)
	}
	ftsIndexesJSON, err := json.Marshal(t.snapshotFTSIndexes())
	if err != nil {
		return fmt.Errorf("save table %s.%s: encode FTS indexes: %w", tenant, t.Name, err)
	}

	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("save table %s.%s: %w", tenant, t.Name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	quoted := quoteIdent(dataTable)
	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoted)); err != nil {
		return fmt.Errorf("save table %s.%s: drop old data table: %w", tenant, t.Name, err)
	}
	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE ")
	ddl.WriteString(quoted)
	ddl.WriteString(` ("__seq" INTEGER NOT NULL`)
	for _, c := range t.Cols {
		ddl.WriteString(", ")
		ddl.WriteString(quoteIdent(c.Name))
		ddl.WriteString(" ")
		ddl.WriteString(sqliteAffinityForColType(c.Type))
	}
	ddl.WriteString(")")
	if _, err := tx.Exec(ddl.String()); err != nil {
		return fmt.Errorf("save table %s.%s: create data table: %w", tenant, t.Name, err)
	}

	if len(t.Rows) > 0 {
		var ins strings.Builder
		ins.WriteString("INSERT INTO ")
		ins.WriteString(quoted)
		ins.WriteString(` ("__seq"`)
		for _, c := range t.Cols {
			ins.WriteString(", ")
			ins.WriteString(quoteIdent(c.Name))
		}
		ins.WriteString(") VALUES (?")
		for range t.Cols {
			ins.WriteString(", ?")
		}
		ins.WriteString(")")
		stmt, err := tx.Prepare(ins.String())
		if err != nil {
			return fmt.Errorf("save table %s.%s: prepare insert: %w", tenant, t.Name, err)
		}
		defer func() { _ = stmt.Close() }()

		args := make([]any, len(t.Cols)+1)
		for seq, r := range t.Rows {
			args[0] = seq
			for i, c := range t.Cols {
				if i < len(r) {
					args[i+1], err = encodeSQLiteCell(c.Type, r[i])
					if err != nil {
						return fmt.Errorf("save table %s.%s: encode row %d col %s: %w", tenant, t.Name, seq, c.Name, err)
					}
				} else {
					args[i+1] = nil
				}
			}
			if _, err := stmt.Exec(args...); err != nil {
				return fmt.Errorf("save table %s.%s: insert row %d: %w", tenant, t.Name, seq, err)
			}
		}
	}

	_, err = tx.Exec(
		`INSERT INTO "__tinysql_meta" (tenant, name, is_temp, version, struct_version, cols_json, indexes_json, stats_json, fts_indexes_json, data_table)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant, name) DO UPDATE SET
		   is_temp = excluded.is_temp,
		   version = excluded.version,
		   struct_version = excluded.struct_version,
		   cols_json = excluded.cols_json,
		   indexes_json = excluded.indexes_json,
		   stats_json = excluded.stats_json,
		   fts_indexes_json = excluded.fts_indexes_json,
		   data_table = excluded.data_table`,
		tenant, t.Name, t.IsTemp, t.Version, t.structVersion, string(colsJSON), string(indexesJSON), string(statsJSON), string(ftsIndexesJSON), dataTable,
	)
	if err != nil {
		return fmt.Errorf("save table %s.%s: update meta: %w", tenant, t.Name, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("save table %s.%s: commit: %w", tenant, t.Name, err)
	}
	committed = true
	b.stats.SyncCount++
	return nil
}

// DeleteTable removes a table's data and metadata from the SQLite file.
func (b *SQLiteBackend) DeleteTable(tenant, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.readOnly {
		return ErrReadOnlyStorage
	}

	meta, err := b.loadMeta(tenant, name)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}
	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("delete table %s.%s: %w", tenant, name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdent(meta.DataTable))); err != nil {
		return fmt.Errorf("delete table %s.%s: drop data table: %w", tenant, name, err)
	}
	if _, err := tx.Exec(`DELETE FROM "__tinysql_meta" WHERE tenant = ? AND name = ?`, tenant, name); err != nil {
		return fmt.Errorf("delete table %s.%s: delete meta: %w", tenant, name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delete table %s.%s: commit: %w", tenant, name, err)
	}
	committed = true
	return nil
}

// ListTableNames returns every table name recorded for tenant.
func (b *SQLiteBackend) ListTableNames(tenant string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	rows, err := b.db.Query(`SELECT name FROM "__tinysql_meta" WHERE tenant = ? ORDER BY name`, tenant)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// TableExists reports whether (tenant, name) has a meta row.
func (b *SQLiteBackend) TableExists(tenant, name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	var one int
	err := b.db.QueryRow(`SELECT 1 FROM "__tinysql_meta" WHERE tenant = ? AND name = ?`, tenant, name).Scan(&one)
	return err == nil
}

// Sync checkpoints the SQLite WAL so committed data is durable in the main
// database file, not just its journal sidecar.
func (b *SQLiteBackend) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// Close checkpoints and closes the underlying SQLite connection.
func (b *SQLiteBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _ = b.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return b.db.Close()
}

func (b *SQLiteBackend) Mode() StorageMode { return ModeSQLite }

func (b *SQLiteBackend) Stats() BackendStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stats
	s.Mode = ModeSQLite
	return s
}

func diskColumnsOf(cols []Column) []diskColumn {
	out := make([]diskColumn, len(cols))
	for i, c := range cols {
		out[i] = diskColumn(c)
	}
	return out
}

// sqliteNativeColType reports whether t's values are stored as plain
// SQLite-native values (matching the runtime Go value directly) rather than
// JSON-encoded text. Kept deliberately narrow: only types whose runtime
// representation already is what database/sql exchanges natively, so
// encoding and decoding never have to guess which convention a given cell
// used.
func sqliteNativeColType(t ColType) bool {
	switch t {
	case IntType, Int8Type, Int16Type, Int32Type, Int64Type,
		UintType, Uint8Type, Uint16Type, Uint32Type, Uint64Type,
		Float32Type, Float64Type, FloatType,
		StringType, TextType,
		BoolType,
		BlobType:
		return true
	default:
		return false
	}
}

// sqliteAffinityForColType maps a tinySQL column type to a SQLite type
// affinity for CREATE TABLE, purely for readability in external SQLite
// tools — SQLite's own dynamic typing means this never affects correctness,
// only how a value is displayed/coerced by other SQLite clients.
func sqliteAffinityForColType(t ColType) string {
	switch t {
	case IntType, Int8Type, Int16Type, Int32Type, Int64Type,
		UintType, Uint8Type, Uint16Type, Uint32Type, Uint64Type,
		BoolType, RuneType:
		return "INTEGER"
	case Float32Type, Float64Type, FloatType:
		return "REAL"
	case BlobType, BitmapType:
		return "BLOB"
	default:
		return "TEXT"
	}
}

// encodeSQLiteCell converts one cell value to whatever database/sql should
// bind for colType: a native int64/float64/string/[]byte for the types
// sqliteNativeColType recognizes, or JSON-encoded text (via JSONMarshal,
// exactly as ModeJSON already encodes Decimal/UUID/etc.) for everything
// else — including time values, geometry/vector columns, and any other
// exotic ColType.
func encodeSQLiteCell(colType ColType, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if sqliteNativeColType(colType) {
		switch colType {
		case BoolType:
			if b, ok := v.(bool); ok {
				if b {
					return int64(1), nil
				}
				return int64(0), nil
			}
		case BlobType:
			if bs, ok := v.([]byte); ok {
				return bs, nil
			}
		case StringType, TextType:
			if s, ok := v.(string); ok {
				return s, nil
			}
		default:
			switch n := v.(type) {
			case int64:
				return n, nil
			case int:
				return int64(n), nil
			case int8:
				return int64(n), nil
			case int16:
				return int64(n), nil
			case int32:
				return int64(n), nil
			case uint:
				return int64(n), nil
			case uint8:
				return int64(n), nil
			case uint16:
				return int64(n), nil
			case uint32:
				return int64(n), nil
			case uint64:
				return int64(n), nil
			case float64:
				return n, nil
			case float32:
				return float64(n), nil
			}
		}
	}
	b, err := JSONMarshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v), nil
	}
	return string(b), nil
}

// decodeSQLiteCell reverses encodeSQLiteCell given the column's type and the
// raw value database/sql scanned back (int64/float64/string/[]byte/nil for
// SQLite's native storage classes).
func decodeSQLiteCell(colType ColType, raw any) any {
	if raw == nil {
		return nil
	}
	if sqliteNativeColType(colType) {
		if colType == BoolType {
			switch n := raw.(type) {
			case int64:
				return n != 0
			case bool:
				return n
			}
		}
		return raw
	}
	s, ok := raw.(string)
	if !ok {
		return raw
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	if colType == VectorType {
		return normalizeVectorValue(v)
	}
	return v
}
