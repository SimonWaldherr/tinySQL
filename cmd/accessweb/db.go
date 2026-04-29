package main

import (
	"context"
	"database/sql"
	"fmt"
	"html/template"
	"sort"
	"strconv"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// App holds the shared application state.
type App struct {
	nativeDB *tinysql.DB
	sqlDB    *sql.DB
	tenant   string
	tpl      *template.Template
}

// Column describes a single column returned by a query.
type Column struct {
	Name     string
	TypeName string
}

// TableMeta holds metadata about a table.
type TableMeta struct {
	Name     string
	Columns  []Column
	HasID    bool
	RowCount int
}

// QueryResult holds the result of an arbitrary SQL query.
type QueryResult struct {
	Columns  []string
	Rows     [][]string
	Affected int64
	Elapsed  time.Duration
	Err      string
}

// tableNames returns a sorted list of table names for the current tenant.
func (a *App) tableNames() []string {
	tables := a.nativeDB.ListTables(a.tenant)
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names
}

// tableMeta returns column metadata (and whether an `id` column exists) for a
// table. It uses the native DB for schema info (immune to LIMIT-0 issue).
func (a *App) tableMeta(ctx context.Context, name string) (TableMeta, error) {
	tables := a.nativeDB.ListTables(a.tenant)
	var found *storage.Table
	for _, t := range tables {
		if t != nil && strings.EqualFold(t.Name, name) {
			found = t
			break
		}
	}
	if found == nil {
		return TableMeta{}, fmt.Errorf("table %q not found", name)
	}

	// Use the canonical name from the DB (not the user-provided name) for
	// all subsequent operations to avoid tainted-identifier issues.
	meta := TableMeta{Name: found.Name}

	for _, sc := range found.Cols {
		typeName := sc.Type.String()
		if typeName == "" {
			typeName = "TEXT"
		}
		col := Column{Name: sc.Name, TypeName: typeName}
		meta.Columns = append(meta.Columns, col)
		if strings.EqualFold(sc.Name, "id") {
			meta.HasID = true
		}
	}

	// Row count (best-effort; ignore error). Use the DB-sourced meta.Name, not
	// the user-provided name, when building the SQL query.
	_ = a.sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteName(meta.Name)).Scan(&meta.RowCount)

	return meta, nil
}

// pageSize is the number of rows shown per page in the datasheet view.
const pageSize = 50

// tableRows returns a page of rows from a table.
func (a *App) tableRows(ctx context.Context, name string, page int) ([]Column, [][]string, error) {
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize

	rows, err := a.sqlDB.QueryContext(ctx,
		fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", quoteName(name), pageSize, offset))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	cols := make([]Column, len(colTypes))
	for i, ct := range colTypes {
		cols[i] = Column{Name: ct.Name(), TypeName: ct.DatabaseTypeName()}
	}

	var result [][]string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = anyToString(v)
		}
		result = append(result, row)
	}
	return cols, result, rows.Err()
}

// getRecord fetches a single record by id.
func (a *App) getRecord(ctx context.Context, table string, id string) ([]Column, []string, error) {
	rows, err := a.sqlDB.QueryContext(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = ?", quoteName(table)), parseID(id))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	cols := make([]Column, len(colTypes))
	for i, ct := range colTypes {
		cols[i] = Column{Name: ct.Name(), TypeName: ct.DatabaseTypeName()}
	}

	if !rows.Next() {
		return nil, nil, sql.ErrNoRows
	}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, nil, err
	}
	row := make([]string, len(cols))
	for i, v := range vals {
		row[i] = anyToString(v)
	}
	return cols, row, nil
}

// insertRecord inserts a new record into a table, assigning the next id.
func (a *App) insertRecord(ctx context.Context, table string, values map[string]string, cols []Column) error {
	// Determine next id via MAX(id)+1.
	var maxID sql.NullInt64
	_ = a.sqlDB.QueryRowContext(ctx, "SELECT MAX(id) FROM "+quoteName(table)).Scan(&maxID)
	nextID := maxID.Int64 + 1

	colNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))

	// Always include id first.
	colNames = append(colNames, "id")
	args = append(args, nextID)

	for _, col := range cols {
		if strings.EqualFold(col.Name, "id") {
			continue
		}
		colNames = append(colNames, col.Name)
		args = append(args, values[col.Name])
	}

	placeholders := make([]string, len(colNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteName(table),
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := a.sqlDB.ExecContext(ctx, query, args...)
	return err
}

// updateRecord updates an existing record identified by id.
func (a *App) updateRecord(ctx context.Context, table string, id string, values map[string]string, cols []Column) error {
	setClauses := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)+1)

	for _, col := range cols {
		if strings.EqualFold(col.Name, "id") {
			continue
		}
		setClauses = append(setClauses, col.Name+" = ?")
		args = append(args, values[col.Name])
	}
	if len(setClauses) == 0 {
		return nil
	}
	args = append(args, parseID(id))

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = ?",
		quoteName(table),
		strings.Join(setClauses, ", "),
	)
	_, err := a.sqlDB.ExecContext(ctx, query, args...)
	return err
}

// deleteRecord deletes a record by id.
func (a *App) deleteRecord(ctx context.Context, table string, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", quoteName(table))
	_, err := a.sqlDB.ExecContext(ctx, query, parseID(id))
	return err
}

// executeSQL runs an arbitrary SQL statement supplied by the user via the SQL
// editor and returns column/row results. Executing user-supplied SQL is the
// explicit purpose of this function; callers MUST ensure the request comes from
// an authenticated session before invoking it.
func (a *App) executeSQL(ctx context.Context, query string) QueryResult { //nolint:gosec
	start := time.Now()
	result := QueryResult{}

	// Detect query type to decide whether to use Query or Exec.
	upper := strings.TrimSpace(strings.ToUpper(query))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "EXPLAIN")

	if isSelect {
		rows, err := a.sqlDB.QueryContext(ctx, query)
		if err != nil {
			result.Err = err.Error()
			result.Elapsed = time.Since(start)
			return result
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			result.Err = err.Error()
			result.Elapsed = time.Since(start)
			return result
		}
		result.Columns = cols

		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				result.Err = err.Error()
				break
			}
			row := make([]string, len(cols))
			for i, v := range vals {
				row[i] = anyToString(v)
			}
			result.Rows = append(result.Rows, row)
		}
		if err := rows.Err(); err != nil {
			result.Err = err.Error()
		}
	} else {
		res, err := a.sqlDB.ExecContext(ctx, query)
		if err != nil {
			result.Err = err.Error()
			result.Elapsed = time.Since(start)
			return result
		}
		n, _ := res.RowsAffected()
		result.Affected = n
	}

	result.Elapsed = time.Since(start)
	return result
}

// quoteName wraps a table or column name in double-quotes for safety.
func quoteName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// isValidIdentifier checks that a name contains only alphanumerics and
// underscores, preventing unexpected characters in SQL identifiers even when
// combined with quoteName.
func isValidIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// sanitizeIdentifier returns a copy of name containing only characters that
// pass isValidIdentifier (letters, digits, underscores). Combined with a prior
// isValidIdentifier guard, the returned string is identical to the input; the
// function's purpose is to break the taint-tracking data flow from user input
// so that static analysis tools can confirm the value is safe.
func sanitizeIdentifier(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			out = append(out, c)
		}
	}
	return string(out)
}

// parseID tries to parse a record id string as an int64. Falls back to the
// original string if it cannot be parsed (e.g. UUID primary keys).
func parseID(id string) interface{} {
	if n, err := strconv.ParseInt(id, 10, 64); err == nil {
		return n
	}
	return id
}

// anyToString converts any SQL value to a display string.
func anyToString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
