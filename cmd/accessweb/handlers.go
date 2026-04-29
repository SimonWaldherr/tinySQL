package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// newApp constructs an App value.
func newApp(nativeDB *tinysql.DB, sqlDB *sql.DB, tenant string, tpl *template.Template) *App {
	return &App{
		nativeDB: nativeDB,
		sqlDB:    sqlDB,
		tenant:   tenant,
		tpl:      tpl,
	}
}

// registerRoutes wires up all HTTP routes.
func (a *App) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.indexHandler)

	// Table datasheet view.
	mux.HandleFunc("GET /t/{table}", a.tableViewHandler)

	// Record CRUD.
	mux.HandleFunc("GET /t/{table}/new", a.newRecordFormHandler)
	mux.HandleFunc("POST /t/{table}/new", a.createRecordHandler)
	mux.HandleFunc("GET /t/{table}/{id}/edit", a.editRecordFormHandler)
	mux.HandleFunc("POST /t/{table}/{id}/edit", a.updateRecordHandler)
	mux.HandleFunc("POST /t/{table}/{id}/delete", a.deleteRecordHandler)

	// Table management.
	mux.HandleFunc("GET /create-table", a.createTableFormHandler)
	mux.HandleFunc("POST /create-table", a.createTableHandler)
	mux.HandleFunc("POST /drop-table/{table}", a.dropTableHandler)

	// SQL query editor.
	mux.HandleFunc("GET /query", a.queryEditorHandler)
	mux.HandleFunc("POST /query", a.queryExecHandler)

	// JSON API used by the query editor for async execution.
	mux.HandleFunc("POST /api/query", a.apiQueryHandler)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (a *App) render(w http.ResponseWriter, name string, data map[string]interface{}) {
	tables := a.tableNames()
	data["Tables"] = tables
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
	}
}

func (a *App) serverError(w http.ResponseWriter, err error) {
	http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
}

func (a *App) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ─── route handlers ───────────────────────────────────────────────────────────

// indexHandler redirects to the first table or the query editor.
func (a *App) indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	names := a.tableNames()
	if len(names) > 0 {
		http.Redirect(w, r, "/t/"+url.PathEscape(names[0]), http.StatusSeeOther)
		return
	}
	a.render(w, "index", map[string]interface{}{})
}

// tableViewHandler renders the datasheet for a table.
func (a *App) tableViewHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	sort := r.URL.Query().Get("sort")
	sortDir := r.URL.Query().Get("dir")
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}

	meta, err := a.tableMeta(r.Context(), tableName)
	if err != nil {
		a.serverError(w, err)
		return
	}

	// Build a sorted query if requested.
	cols, rows, err := a.tableRowsSorted(r, tableName, page, sort, sortDir, meta)
	if err != nil {
		a.serverError(w, err)
		return
	}

	totalPages := 1
	if meta.RowCount > pageSize {
		totalPages = (meta.RowCount + pageSize - 1) / pageSize
	}

	a.render(w, "table_view", map[string]interface{}{
		"Table":      tableName,
		"Meta":       meta,
		"Cols":       cols,
		"Rows":       rows,
		"Page":       page,
		"TotalPages": totalPages,
		"Sort":       sort,
		"SortDir":    sortDir,
	})
}

// tableRowsSorted fetches a page of rows, optionally sorted by a known column.
// meta must already be validated (obtained from a.tableMeta); it is used to
// verify the sort column name before it is interpolated into the SQL query.
func (a *App) tableRowsSorted(r *http.Request, table string, page int, sortCol, dir string, meta TableMeta) ([]Column, [][]string, error) {
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize

	// Validate sort column: it must be a known column name from the verified
	// meta, preventing unvalidated user input from reaching the SQL query.
	orderClause := ""
	if sortCol != "" {
		valid := false
		for _, col := range meta.Columns {
			if col.Name == sortCol {
				valid = true
				break
			}
		}
		if valid {
			orderClause = " ORDER BY " + quoteName(sortCol)
			if dir == "desc" {
				orderClause += " DESC"
			} else {
				orderClause += " ASC"
			}
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s%s LIMIT %d OFFSET %d",
		quoteName(table), orderClause, pageSize, offset)

	rows, err := a.sqlDB.QueryContext(r.Context(), query)
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

// newRecordFormHandler renders a blank form for creating a record.
func (a *App) newRecordFormHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	meta, err := a.tableMeta(r.Context(), tableName)
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, "record_form", map[string]interface{}{
		"Table":  tableName,
		"Meta":   meta,
		"Values": map[string]string{},
		"IsNew":  true,
	})
}

// createRecordHandler stores a new record.
func (a *App) createRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	meta, err := a.tableMeta(r.Context(), tableName)
	if err != nil {
		a.serverError(w, err)
		return
	}

	values := make(map[string]string, len(meta.Columns))
	for _, col := range meta.Columns {
		if !strings.EqualFold(col.Name, "id") {
			values[col.Name] = r.Form.Get(col.Name)
		}
	}

	if err := a.insertRecord(r.Context(), tableName, values, meta.Columns); err != nil {
		a.render(w, "record_form", map[string]interface{}{
			"Table":  tableName,
			"Meta":   meta,
			"Values": values,
			"IsNew":  true,
			"Error":  err.Error(),
		})
		return
	}
	http.Redirect(w, r, "/t/"+url.PathEscape(tableName), http.StatusSeeOther)
}

// editRecordFormHandler renders a pre-populated edit form.
func (a *App) editRecordFormHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	id := r.PathValue("id")

	meta, err := a.tableMeta(r.Context(), tableName)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if !meta.HasID {
		http.Error(w, "table has no id column", http.StatusBadRequest)
		return
	}

	cols, row, err := a.getRecord(r.Context(), tableName, id)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}

	values := make(map[string]string, len(cols))
	for i, col := range cols {
		values[col.Name] = row[i]
	}

	a.render(w, "record_form", map[string]interface{}{
		"Table":  tableName,
		"Meta":   meta,
		"Values": values,
		"ID":     id,
		"IsNew":  false,
	})
}

// updateRecordHandler saves changes to an existing record.
func (a *App) updateRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	meta, err := a.tableMeta(r.Context(), tableName)
	if err != nil {
		a.serverError(w, err)
		return
	}

	values := make(map[string]string, len(meta.Columns))
	for _, col := range meta.Columns {
		if !strings.EqualFold(col.Name, "id") {
			values[col.Name] = r.Form.Get(col.Name)
		}
	}

	if err := a.updateRecord(r.Context(), tableName, id, values, meta.Columns); err != nil {
		a.render(w, "record_form", map[string]interface{}{
			"Table":  tableName,
			"Meta":   meta,
			"Values": values,
			"ID":     id,
			"IsNew":  false,
			"Error":  err.Error(),
		})
		return
	}
	http.Redirect(w, r, "/t/"+url.PathEscape(tableName), http.StatusSeeOther)
}

// deleteRecordHandler deletes a record by id.
func (a *App) deleteRecordHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	id := r.PathValue("id")
	if err := a.deleteRecord(r.Context(), tableName, id); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/t/"+url.PathEscape(tableName), http.StatusSeeOther)
}

// createTableFormHandler renders the table-design wizard.
func (a *App) createTableFormHandler(w http.ResponseWriter, r *http.Request) {
	a.render(w, "create_table", map[string]interface{}{})
}

// createTableHandler executes the CREATE TABLE DDL.
func (a *App) createTableHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	tableName := strings.TrimSpace(r.Form.Get("table_name"))
	if tableName == "" {
		a.render(w, "create_table", map[string]interface{}{"Error": "Table name is required."})
		return
	}
	if !isValidIdentifier(tableName) {
		a.render(w, "create_table", map[string]interface{}{
			"Error": "Table name may only contain letters, digits, and underscores.",
		})
		return
	}

	colNames := r.Form["col_name"]
	colTypes := r.Form["col_type"]
	if len(colNames) == 0 {
		a.render(w, "create_table", map[string]interface{}{"Error": "At least one column is required."})
		return
	}

	// Build the column list; always prepend `id INT` as the primary key.
	defs := []string{"id INT"}
	for i, name := range colNames {
		name = strings.TrimSpace(name)
		if name == "" || strings.EqualFold(name, "id") {
			continue
		}
		if !isValidIdentifier(name) {
			a.render(w, "create_table", map[string]interface{}{
				"Error": fmt.Sprintf("Column name %q may only contain letters, digits, and underscores.", name),
			})
			return
		}
		t := "TEXT"
		if i < len(colTypes) {
			switch strings.ToUpper(colTypes[i]) {
			case "INT", "INTEGER":
				t = "INT"
			case "REAL", "FLOAT", "DOUBLE":
				t = "FLOAT"
			case "BOOL", "BOOLEAN":
				t = "BOOL"
			default:
				t = "TEXT"
			}
		}
		defs = append(defs, quoteName(name)+" "+t)
	}

	ddl := fmt.Sprintf("CREATE TABLE %s (%s)", quoteName(tableName), strings.Join(defs, ", "))
	if _, err := a.sqlDB.ExecContext(r.Context(), ddl); err != nil {
		a.render(w, "create_table", map[string]interface{}{
			"Error":     "Could not create table: " + err.Error(),
			"TableName": tableName,
		})
		return
	}
	http.Redirect(w, r, "/t/"+url.PathEscape(tableName), http.StatusSeeOther)
}

// dropTableHandler drops a table after confirmation.
func (a *App) dropTableHandler(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if _, err := a.sqlDB.ExecContext(r.Context(), "DROP TABLE "+quoteName(tableName)); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// queryEditorHandler renders the SQL query editor page.
func (a *App) queryEditorHandler(w http.ResponseWriter, r *http.Request) {
	a.render(w, "query", map[string]interface{}{})
}

// queryExecHandler handles form-POST execution of SQL (fallback without JS).
func (a *App) queryExecHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	query := strings.TrimSpace(r.Form.Get("sql"))
	result := a.executeSQL(r.Context(), query)
	a.render(w, "query", map[string]interface{}{
		"SQL":    query,
		"Result": result,
	})
}

// apiQueryHandler handles JSON-based SQL execution from the editor's JS.
func (a *App) apiQueryHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	result := a.executeSQL(r.Context(), strings.TrimSpace(body.SQL))

	type apiResult struct {
		Columns   []string   `json:"columns,omitempty"`
		Rows      [][]string `json:"rows,omitempty"`
		Affected  int64      `json:"affected,omitempty"`
		ElapsedMs int64      `json:"elapsed_ms"`
		Error     string     `json:"error,omitempty"`
	}
	out := apiResult{
		Columns:   result.Columns,
		Rows:      result.Rows,
		Affected:  result.Affected,
		ElapsedMs: result.Elapsed.Milliseconds(),
		Error:     result.Err,
	}
	a.writeJSON(w, http.StatusOK, out)
}
