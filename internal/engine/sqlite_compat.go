package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executePragma(env ExecEnv, p *Pragma) (*ResultSet, error) {
	name := strings.ToLower(strings.TrimSpace(p.Name))
	// The parser has always accepted the assignment form and stored the
	// right-hand side in p.Value, but this function only ever switched on the
	// name -- so `PRAGMA foreign_keys = OFF` answered 1 with foreign keys still
	// enforced, and `PRAGMA journal_mode = WAL` answered "memory". A SQLite
	// migrant's durability preamble therefore reported success while changing
	// nothing at all, which is strictly worse than refusing it: the caller
	// walks away believing it got WAL durability or a relaxed FK check.
	// Every knob a PRAGMA would touch here is settled once at Open time from
	// the DSN, so there is no runtime state for an assignment to write. Reject
	// it loudly and name the DSN option that does configure the same thing.
	// The read form (p.Value == nil) is deliberately untouched.
	if p.Value != nil {
		return nil, pragmaReadOnlyError(name, p.Name)
	}
	switch name {
	case "table_info", "table_xinfo":
		if len(p.Args) != 1 {
			return nil, fmt.Errorf("PRAGMA %s requires exactly one table name", p.Name)
		}
		return pragmaTableInfo(env, p.Args[0], name == "table_xinfo"), nil
	case "table_list":
		return pragmaTableList(env), nil
	case "index_list":
		if len(p.Args) != 1 {
			return nil, fmt.Errorf("PRAGMA %s requires exactly one table name", p.Name)
		}
		return pragmaIndexList(env, p.Args[0]), nil
	case "index_info":
		if len(p.Args) != 1 {
			return nil, fmt.Errorf("PRAGMA %s requires exactly one index name", p.Name)
		}
		return pragmaIndexInfo(env, p.Args[0]), nil
	case "foreign_key_list":
		if len(p.Args) != 1 {
			return nil, fmt.Errorf("PRAGMA %s requires exactly one table name", p.Name)
		}
		return pragmaForeignKeyList(env, p.Args[0]), nil
	case "database_list":
		return pragmaDatabaseList(env), nil
	case "schema_version":
		return pragmaSingleInt("schema_version", 0), nil
	case "user_version":
		return pragmaSingleInt("user_version", 0), nil
	case "application_id":
		return pragmaSingleInt("application_id", 0), nil
	case "foreign_keys":
		return pragmaSingleInt("foreign_keys", 1), nil
	case "journal_mode":
		return &ResultSet{Cols: []string{"journal_mode"}, Rows: []Row{{"journal_mode": sqliteJournalMode(env.db.StorageMode())}}}, nil
	case "integrity_check", "quick_check":
		return &ResultSet{Cols: []string{name}, Rows: []Row{{name: "ok"}}}, nil
	case "compile_options":
		return pragmaCompileOptions(), nil
	default:
		return nil, fmt.Errorf("unsupported PRAGMA %q", p.Name)
	}
}

// pragmaReadOnlyError explains why `PRAGMA <name> = <value>` cannot work and,
// where an equivalent knob exists, points at the DSN option that owns it. The
// mapping is worth spelling out per pragma rather than emitting one generic
// message: the person hitting this is porting a SQLite preamble and needs to
// know where the setting moved to, not merely that it is gone.
func pragmaReadOnlyError(name, rawName string) error {
	switch name {
	case "journal_mode":
		return fmt.Errorf(`PRAGMA %s is read-only in tinySQL; the journal strategy follows the storage mode chosen at open time via the DSN option "mode=" (for example "mode=wal" or "mode=advancedwal")`, rawName)
	case "synchronous":
		return fmt.Errorf(`PRAGMA %s is read-only in tinySQL; use the DSN option "wal_sync=" to choose how aggressively the write-ahead log is fsynced`, rawName)
	case "foreign_keys":
		return fmt.Errorf("PRAGMA %s is read-only in tinySQL and has no DSN equivalent: foreign key enforcement is unconditional and cannot be turned off", rawName)
	case "cache_size":
		return fmt.Errorf(`PRAGMA %s is read-only in tinySQL; use the DSN option "max_memory_bytes=" to bound how much table data stays resident`, rawName)
	default:
		return fmt.Errorf("PRAGMA %s is read-only in tinySQL", rawName)
	}
}

func pragmaTableInfo(env ExecEnv, tableName string, includeHidden bool) *ResultSet {
	tableName = strings.Trim(tableName, `"'`)
	cols := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	if includeHidden {
		cols = append(cols, "hidden")
	}
	t, err := env.db.Get(env.tenant, tableName)
	if err != nil {
		return &ResultSet{Cols: cols}
	}
	rows := make([]Row, 0, len(t.Cols))
	for i, c := range t.Cols {
		declaredType := c.DeclaredType
		if declaredType == "" {
			declaredType = c.Type.String()
		}
		var defaultValue any
		if c.HasDefault {
			defaultValue = sqliteDefaultSQL(c.DefaultValue)
		}
		row := Row{
			"cid":        i,
			"name":       c.Name,
			"type":       declaredType,
			"notnull":    sqliteBoolInt(c.NotNull || c.Constraint == storage.PrimaryKey),
			"dflt_value": defaultValue,
			"pk":         sqliteBoolInt(c.Constraint == storage.PrimaryKey),
		}
		if includeHidden {
			row["hidden"] = 0
		}
		rows = append(rows, row)
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

func pragmaTableList(env ExecEnv) *ResultSet {
	cols := []string{"schema", "name", "type", "ncol", "wr", "strict"}
	schemaRows := sqliteSchemaRows(env)
	rows := make([]Row, 0, len(schemaRows))
	for _, r := range schemaRows {
		typ, _ := r["type"].(string)
		if typ != "table" && typ != "view" {
			continue
		}
		row := Row{
			"schema": r["schema"],
			"name":   r["name"],
			"type":   typ,
			"ncol":   sqliteColumnCount(env, r),
			"wr":     0,
			"strict": 0,
		}
		rows = append(rows, row)
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

// pragmaIndexList exposes explicitly-created secondary indexes in SQLite's
// index_list shape. tinySQL does not surface its constraint-enforcement
// structures as fictitious sqlite_autoindex_* entries, so this intentionally
// reports only the index definitions users can create and drop by name.
func pragmaIndexList(env ExecEnv, tableName string) *ResultSet {
	cols := []string{"seq", "name", "unique", "origin", "partial"}
	_, tableName = splitObjectName(strings.Trim(tableName, `"'`))
	indexes := env.db.Catalog().GetIndexesForTenant(env.tenant)
	matching := make([]*storage.CatalogIndex, 0, len(indexes))
	for _, idx := range indexes {
		if strings.EqualFold(idx.Table, tableName) {
			matching = append(matching, idx)
		}
	}
	sort.Slice(matching, func(i, j int) bool {
		return strings.ToLower(matching[i].Name) < strings.ToLower(matching[j].Name)
	})
	rows := make([]Row, len(matching))
	for i, idx := range matching {
		rows[i] = Row{
			"seq":     i,
			"name":    idx.Name,
			"unique":  sqliteBoolInt(idx.Unique),
			"origin":  "c", // SQLite's marker for CREATE INDEX.
			"partial": 0,
		}
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

// pragmaIndexInfo returns the ordinal and table-column position for each
// column in one explicitly-created secondary index. Like SQLite, an unknown
// index returns an empty result set rather than an error, which makes schema
// probing by drivers straightforward.
func pragmaIndexInfo(env ExecEnv, indexName string) *ResultSet {
	cols := []string{"seqno", "cid", "name"}
	schema, indexName := splitObjectName(strings.Trim(indexName, `"'`))
	idx, found := env.db.Catalog().GetIndexForTenant(env.tenant, schema, indexName)
	if !found {
		return &ResultSet{Cols: cols}
	}
	table, err := env.db.Get(env.tenant, idx.Table)
	if err != nil {
		return &ResultSet{Cols: cols}
	}
	rows := make([]Row, 0, len(idx.Columns))
	for seq, name := range idx.Columns {
		cid, err := table.ColIndex(name)
		if err != nil {
			// Catalog metadata can outlive an ALTER/DROP failure; omit only the
			// stale column and preserve SQLite's empty-or-partial introspection
			// behavior instead of turning a metadata probe into a query error.
			continue
		}
		rows = append(rows, Row{"seqno": seq, "cid": cid, "name": table.Cols[cid].Name})
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

// pragmaForeignKeyList exposes column-level foreign-key metadata in SQLite's
// foreign_key_list shape. tinySQL supports one local column per foreign key,
// so every emitted constraint has sequence number zero.
func pragmaForeignKeyList(env ExecEnv, tableName string) *ResultSet {
	cols := []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
	_, tableName = splitObjectName(strings.Trim(tableName, `"'`))
	table, err := env.db.Get(env.tenant, tableName)
	if err != nil {
		return &ResultSet{Cols: cols}
	}
	rows := make([]Row, 0)
	for _, col := range table.Cols {
		if col.Constraint != storage.ForeignKey || col.ForeignKey == nil {
			continue
		}
		fk := col.ForeignKey
		rows = append(rows, Row{
			"id":        len(rows),
			"seq":       0,
			"table":     fk.Table,
			"from":      col.Name,
			"to":        fk.Column,
			"on_update": fk.OnUpdate.String(),
			"on_delete": fk.OnDelete.String(),
			"match":     "NONE",
		})
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

func pragmaDatabaseList(env ExecEnv) *ResultSet {
	path := ""
	if cfg := env.db.Config(); cfg != nil {
		path = cfg.Path
	}
	return &ResultSet{
		Cols: []string{"seq", "name", "file"},
		Rows: []Row{{"seq": 0, "name": "main", "file": path}},
	}
}

func pragmaSingleInt(name string, val int) *ResultSet {
	return &ResultSet{Cols: []string{name}, Rows: []Row{{name: val}}}
}

func pragmaCompileOptions() *ResultSet {
	options := []string{
		"ENABLE_FTS",
		"ENABLE_JSON",
		"ENABLE_MATERIALIZED_VIEWS",
		"ENABLE_RAG",
		"ENABLE_VECTOR",
		"THREADSAFE=1",
	}
	rows := make([]Row, len(options))
	for i, option := range options {
		rows[i] = Row{"compile_options": option}
	}
	return &ResultSet{Cols: []string{"compile_options"}, Rows: rows}
}

func sqliteBoolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// sqliteJournalMode reports the SQLite journal_mode keyword that most honestly
// describes a tinySQL storage mode. Honesty matters more than familiarity here:
// journal_mode is the value tooling reads to decide whether a crash leaves a
// recoverable database behind, so a value must never promise a guarantee the
// mode does not actually provide.
func sqliteJournalMode(mode storage.StorageMode) string {
	switch mode {
	case storage.ModeWAL, storage.ModeAdvancedWAL:
		return "wal"
	case storage.ModeSQLite:
		// ModeSQLite writes into a real SQLite file, and that file is opened
		// with PRAGMA journal_mode=WAL by the backend itself (see
		// internal/storage/backend_sqlite.go). "wal" is the literal truth for
		// it, not an approximation -- it used to fall through to the default
		// arm and report a rollback journal it does not use.
		return "wal"
	case storage.ModeMemory:
		return "memory"
	default:
		// ModeDisk/ModeJSON/ModeIndex/ModeHybrid/ModePagedIndex persist by
		// snapshotting whole tables. None of them keeps a rollback journal and
		// none offers crash-atomic commit across several tables, so the former
		// "delete" was an affirmative lie: it tells a reader that a journal
		// file exists and will be replayed or rolled back on restart, and that
		// ROLLBACK is therefore well defined. "off" is SQLite's own documented
		// value for "no rollback journal, and the behaviour of ROLLBACK is
		// undefined", which is exactly what these modes guarantee.
		return "off"
	}
}

// sqliteMasterCols is the sqlite_master/sqlite_schema shape, in SQLite's own
// ordinal order. Positional consumers -- ORMs and migration tools that scan
// row[3] for rootpage and row[4] for sql -- depend on both the membership and
// the order, so nothing may be inserted, appended, or reordered here. tinySQL's
// own extra columns live on tinysql_schema instead (see schemaTableKind).
//
// Known gap that cannot be closed from this file: `SELECT *` builds its output
// order by ranging over the Row map in the star expansion in exec_group.go, so
// for any map-backed virtual source it emits Go's randomized map order rather
// than the order below. Naming the columns explicitly
// (SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master) is ordered
// correctly. Making `SELECT *` ordinal-stable needs a column-order channel
// from resolveFromClause into that star expansion.
var sqliteMasterCols = []string{"type", "name", "tbl_name", "rootpage", "sql"}

// schemaTableKind distinguishes the two catalogue views that sqliteSchemaRows
// feeds. They share every row; they differ only in which columns are visible.
type schemaTableKind int

const (
	// notSchemaTable means the name is an ordinary table reference.
	notSchemaTable schemaTableKind = iota
	// sqliteCompatSchema is sqlite_master / sqlite_schema: strictly SQLite's
	// five columns, so anything written against real SQLite keeps working.
	sqliteCompatSchema
	// tinysqlSchema is tinysql_schema / tinysql_master: the same rows plus
	// tinySQL's schema and full_name columns. This exists because sqlite_master
	// used to leak those two extras, which broke positional reads; rather than
	// dropping the information, it moved to a name that cannot be mistaken for
	// SQLite's and is free to carry tinySQL-specific columns.
	tinysqlSchema
)

func classifySchemaTable(name string) schemaTableKind {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(name), `"'`)) {
	case "sqlite_schema", "sqlite_master":
		return sqliteCompatSchema
	case "tinysql_schema", "tinysql_master":
		return tinysqlSchema
	default:
		return notSchemaTable
	}
}

func isSQLiteSchemaTable(name string) bool {
	return classifySchemaTable(name) != notSchemaTable
}

func resolveSQLiteSchemaTable(env ExecEnv, s *Select) []Row {
	rows := sqliteSchemaRows(env)
	if classifySchemaTable(s.From.Table) == sqliteCompatSchema {
		for i, r := range rows {
			rows[i] = projectSQLiteMasterRow(r)
		}
	}
	if s.From.Alias != "" {
		for _, r := range rows {
			for k, v := range r {
				if !strings.Contains(k, ".") {
					r[s.From.Alias+"."+k] = v
				}
			}
		}
	}
	return rows
}

func sqliteSchemaRows(env ExecEnv) []Row {
	tenants := env.db.ListTenants()
	tablesByTenant := make([][]*storage.Table, len(tenants))
	tableCount := 0
	for i, tn := range tenants {
		tablesByTenant[i] = env.db.ListTables(tn)
		tableCount += len(tablesByTenant[i])
	}
	catalog := env.db.Catalog()
	views := catalog.GetViews()
	materializedViews := catalog.GetMaterializedViews()
	triggers := catalog.ListTriggers()
	rows := make([]Row, 0, tableCount+len(views)+len(materializedViews)+len(triggers))
	for i := range tenants {
		for _, t := range tablesByTenant[i] {
			if strings.HasPrefix(strings.ToLower(t.Name), "__mv_") {
				continue
			}
			schema, name := splitObjectName(t.Name)
			rows = append(rows, sqliteSchemaRow("table", schema, name, name, sqliteCreateTableSQL(schema, name, t)))
		}
	}
	for _, v := range views {
		fullName := catalogDisplayName(v.Schema, v.Name)
		rows = append(rows, sqliteSchemaRow("view", v.Schema, v.Name, v.Name, "CREATE VIEW "+sqliteIdent(fullName)+" AS "+v.SQLText))
	}
	for _, v := range materializedViews {
		fullName := catalogDisplayName(v.Schema, v.Name)
		rows = append(rows, sqliteSchemaRow("view", v.Schema, v.Name, v.Name, "CREATE MATERIALIZED VIEW "+sqliteIdent(fullName)+" AS "+v.SQLText))
	}
	for _, tr := range triggers {
		sql := "CREATE TRIGGER " + sqliteIdent(tr.Name) + " " + string(tr.Timing) + " " + string(tr.Event) + " ON " + sqliteIdent(tr.Table)
		if tr.ForEachRow {
			sql += " FOR EACH ROW"
		}
		if strings.TrimSpace(tr.WhenExpr) != "" {
			sql += " WHEN " + strings.TrimSpace(tr.WhenExpr)
		}
		sql += " BEGIN " + strings.TrimSpace(tr.Body) + " END"
		rows = append(rows, sqliteSchemaRow("trigger", "main", tr.Name, tr.Table, sql))
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ti, _ := rows[i]["type"].(string)
		tj, _ := rows[j]["type"].(string)
		if ti != tj {
			return ti < tj
		}
		ni, _ := rows[i]["name"].(string)
		nj, _ := rows[j]["name"].(string)
		return ni < nj
	})
	return rows
}

// projectSQLiteMasterRow narrows a catalogue row to SQLite's five columns.
// sqliteSchemaRows keeps building the richer row because PRAGMA table_list and
// sqliteColumnCount both need schema/full_name to resolve a schema-qualified
// table; only the sqlite_master/sqlite_schema projection drops them.
func projectSQLiteMasterRow(r Row) Row {
	out := make(Row, len(sqliteMasterCols))
	for _, col := range sqliteMasterCols {
		putVal(out, col, r[col])
	}
	return out
}

func sqliteSchemaRow(typ, schema, name, tableName, sql string) Row {
	return Row{
		"type":      typ,
		"name":      name,
		"tbl_name":  tableName,
		"rootpage":  0,
		"sql":       sql,
		"schema":    schema,
		"full_name": catalogDisplayName(schema, name),
	}
}

func sqliteCreateTableSQL(schema, name string, t *storage.Table) string {
	// Build directly instead of allocating one intermediate string per column
	// and a parts slice solely for strings.Join. sqlite_schema is commonly
	// queried by migration tools over every table at once, so this removes a
	// proportional amount of short-lived garbage from that path.
	var b strings.Builder
	b.Grow(24 + len(schema) + len(name) + len(t.Cols)*24)
	b.WriteString("CREATE TABLE ")
	writeSQLiteIdent(&b, catalogDisplayName(schema, name))
	b.WriteString(" (")
	for i, c := range t.Cols {
		if i > 0 {
			b.WriteString(", ")
		}
		declaredType := c.DeclaredType
		if declaredType == "" {
			declaredType = c.Type.String()
		}
		writeSQLiteIdent(&b, c.Name)
		b.WriteByte(' ')
		b.WriteString(declaredType)
		switch c.Constraint {
		case storage.PrimaryKey:
			b.WriteString(" PRIMARY KEY")
		case storage.Unique:
			b.WriteString(" UNIQUE")
		case storage.ForeignKey:
			if c.ForeignKey != nil {
				b.WriteString(" REFERENCES ")
				writeSQLiteIdent(&b, c.ForeignKey.Table)
				b.WriteByte('(')
				writeSQLiteIdent(&b, c.ForeignKey.Column)
				b.WriteByte(')')
			}
		}
		if c.NotNull && c.Constraint != storage.PrimaryKey {
			b.WriteString(" NOT NULL")
		}
		if c.HasDefault {
			b.WriteString(" DEFAULT ")
			b.WriteString(sqliteDefaultSQL(c.DefaultValue))
		}
	}
	b.WriteByte(')')
	return b.String()
}

func sqliteDefaultSQL(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "X'" + fmt.Sprintf("%X", x) + "'"
	default:
		return fmt.Sprint(x)
	}
}

func sqliteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func writeSQLiteIdent(b *strings.Builder, name string) {
	b.WriteByte('"')
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			b.WriteByte('"')
		}
		b.WriteByte(name[i])
	}
	b.WriteByte('"')
}

func sqliteColumnCount(env ExecEnv, schemaRow Row) int {
	typ, _ := schemaRow["type"].(string)
	if typ != "table" {
		return 0
	}
	name, _ := schemaRow["full_name"].(string)
	if name == "" {
		name, _ = schemaRow["name"].(string)
	}
	t, err := env.db.Get(env.tenant, name)
	if err != nil {
		return 0
	}
	return len(t.Cols)
}
