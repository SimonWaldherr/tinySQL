package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executePragma(env ExecEnv, p *Pragma) (*ResultSet, error) {
	name := strings.ToLower(strings.TrimSpace(p.Name))
	switch name {
	case "table_info", "table_xinfo":
		if len(p.Args) != 1 {
			return nil, fmt.Errorf("PRAGMA %s requires exactly one table name", p.Name)
		}
		return pragmaTableInfo(env, p.Args[0], name == "table_xinfo"), nil
	case "table_list":
		return pragmaTableList(env), nil
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

func sqliteJournalMode(mode storage.StorageMode) string {
	switch mode {
	case storage.ModeWAL, storage.ModeAdvancedWAL:
		return "wal"
	case storage.ModeMemory:
		return "memory"
	default:
		return "delete"
	}
}

func isSQLiteSchemaTable(name string) bool {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(name), `"'`)) {
	case "sqlite_schema", "sqlite_master":
		return true
	default:
		return false
	}
}

func resolveSQLiteSchemaTable(env ExecEnv, s *Select) []Row {
	rows := sqliteSchemaRows(env)
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
	rows := make([]Row, 0)
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			if strings.HasPrefix(strings.ToLower(t.Name), "__mv_") {
				continue
			}
			schema, name := splitObjectName(t.Name)
			rows = append(rows, sqliteSchemaRow("table", schema, name, name, sqliteCreateTableSQL(schema, name, t)))
		}
	}
	for _, v := range env.db.Catalog().GetViews() {
		fullName := catalogDisplayName(v.Schema, v.Name)
		rows = append(rows, sqliteSchemaRow("view", v.Schema, v.Name, v.Name, "CREATE VIEW "+sqliteIdent(fullName)+" AS "+v.SQLText))
	}
	for _, v := range env.db.Catalog().GetMaterializedViews() {
		fullName := catalogDisplayName(v.Schema, v.Name)
		rows = append(rows, sqliteSchemaRow("view", v.Schema, v.Name, v.Name, "CREATE MATERIALIZED VIEW "+sqliteIdent(fullName)+" AS "+v.SQLText))
	}
	for _, tr := range env.db.Catalog().ListTriggers() {
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

func sqliteSchemaRow(typ, schema, name, tableName, sql string) Row {
	r := make(Row)
	putVal(r, "type", typ)
	putVal(r, "name", name)
	putVal(r, "tbl_name", tableName)
	putVal(r, "rootpage", 0)
	putVal(r, "sql", sql)
	putVal(r, "schema", schema)
	putVal(r, "full_name", catalogDisplayName(schema, name))
	return r
}

func sqliteCreateTableSQL(schema, name string, t *storage.Table) string {
	parts := make([]string, 0, len(t.Cols))
	for _, c := range t.Cols {
		declaredType := c.DeclaredType
		if declaredType == "" {
			declaredType = c.Type.String()
		}
		part := sqliteIdent(c.Name) + " " + declaredType
		switch c.Constraint {
		case storage.PrimaryKey:
			part += " PRIMARY KEY"
		case storage.Unique:
			part += " UNIQUE"
		case storage.ForeignKey:
			if c.ForeignKey != nil {
				part += " REFERENCES " + sqliteIdent(c.ForeignKey.Table) + "(" + sqliteIdent(c.ForeignKey.Column) + ")"
			}
		}
		if c.NotNull && c.Constraint != storage.PrimaryKey {
			part += " NOT NULL"
		}
		if c.HasDefault {
			part += " DEFAULT " + sqliteDefaultSQL(c.DefaultValue)
		}
		parts = append(parts, part)
	}
	return "CREATE TABLE " + sqliteIdent(catalogDisplayName(schema, name)) + " (" + strings.Join(parts, ", ") + ")"
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
