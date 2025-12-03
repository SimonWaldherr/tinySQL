package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	drv "github.com/SimonWaldherr/tinySQL/internal/driver"
	wailsrt "github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct
type App struct {
	ctx      context.Context
	db       *sql.DB
	nativeDB *tinysql.DB
	tenant   string
}

// NewApp creates a new App application struct
func NewApp() *App {
	return &App{tenant: "default"}
}

// startup is called when the app starts
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx

	// Initialize tinySQL with in-memory database
	native := tinysql.NewDB()
	a.nativeDB = native
	drv.SetDefaultDB(native)

	var err error
	a.db, err = sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
	}
}

func (a *App) shutdown(ctx context.Context) {
	if a.db != nil {
		a.db.Close()
	}
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
	Error   string   `json:"error,omitempty"`
	Message string   `json:"message,omitempty"`
	Count   int      `json:"count"`
	Elapsed int64    `json:"elapsed_ms"`
}

// TableInfo contains metadata about a table
type TableInfo struct {
	Name     string       `json:"name"`
	Columns  []ColumnInfo `json:"columns"`
	RowCount int          `json:"rowCount"`
}

// ColumnInfo contains metadata about a column
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ExecuteQuery executes a SQL query and returns the result
func (a *App) ExecuteQuery(sqlStr string) QueryResult {
	start := time.Now()

	if a.db == nil {
		return QueryResult{Error: "Database not initialized"}
	}

	rows, err := a.db.QueryContext(a.ctx, sqlStr)
	if err != nil {
		return QueryResult{
			Error:   err.Error(),
			Elapsed: time.Since(start).Milliseconds(),
		}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{
			Error:   err.Error(),
			Elapsed: time.Since(start).Milliseconds(),
		}
	}

	resultRows := make([][]any, 0)
	colCount := len(columns)

	for rows.Next() {
		values := make([]interface{}, colCount)
		valuePtrs := make([]interface{}, colCount)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return QueryResult{
				Error:   err.Error(),
				Elapsed: time.Since(start).Milliseconds(),
			}
		}

		resultRows = append(resultRows, values)
	}

	return QueryResult{
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
		Elapsed: time.Since(start).Milliseconds(),
	}
}

// ExecuteStatement for non-query statements
func (a *App) ExecuteStatement(sqlStr string) QueryResult {
	start := time.Now()
	if a.db == nil {
		return QueryResult{Error: "Database not initialized"}
	}

	res, err := a.db.ExecContext(a.ctx, sqlStr)
	if err != nil {
		return QueryResult{
			Error:   err.Error(),
			Elapsed: time.Since(start).Milliseconds(),
		}
	}

	rowsAffected, _ := res.RowsAffected()

	return QueryResult{
		Message: fmt.Sprintf("Success. Rows affected: %d", rowsAffected),
		Count:   int(rowsAffected),
		Elapsed: time.Since(start).Milliseconds(),
	}
}

// ImportResponse contains information about a file import operation
type ImportResponse struct {
	Success      bool     `json:"success"`
	TableName    string   `json:"tableName,omitempty"`
	RowsImported int      `json:"rowsImported,omitempty"`
	RowsSkipped  int      `json:"rowsSkipped,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	Delimiter    string   `json:"delimiter,omitempty"`
	HadHeader    bool     `json:"hadHeader,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// ExecuteImport imports a file's content into a table
func (a *App) ExecuteImport(fileName, content, tableName string) ImportResponse {
	if a.nativeDB == nil {
		return ImportResponse{Success: false, Error: "native DB not initialized"}
	}

	if tableName == "" {
		tableName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
	}

	ext := strings.ToLower(filepath.Ext(fileName))
	reader := strings.NewReader(content)
	ctx := context.Background()

	opts := &tinysql.FuzzyImportOptions{
		ImportOptions: &tinysql.ImportOptions{
			CreateTable:   true,
			Truncate:      false,
			HeaderMode:    "auto",
			TypeInference: true,
			TableName:     tableName,
		},
		SkipInvalidRows:    true,
		TrimWhitespace:     true,
		FixQuotes:          true,
		CoerceTypes:        true,
		AllowMixedTypes:    true,
		MaxSkippedRows:     100,
		FuzzyJSON:          true,
		RemoveInvalidChars: true,
		AutoFixDelimiters:  true,
	}

	var impResult *tinysql.ImportResult
	var err error

	switch ext {
	case ".csv", ".tsv", ".txt":
		impResult, err = tinysql.FuzzyImportCSV(ctx, a.nativeDB, a.tenant, tableName, reader, opts)
	case ".json", ".jsonl", ".ndjson":
		impResult, err = tinysql.FuzzyImportJSON(ctx, a.nativeDB, a.tenant, tableName, reader, opts)
	case ".sql":
		if a.db == nil {
			return ImportResponse{Success: false, Error: "no SQL connection available"}
		}
		_, err = a.db.ExecContext(ctx, content)
		if err != nil {
			return ImportResponse{Success: false, Error: err.Error()}
		}
		return ImportResponse{Success: true, TableName: tableName}
	default:
		impResult, err = tinysql.FuzzyImportCSV(ctx, a.nativeDB, a.tenant, tableName, reader, opts)
	}

	if err != nil {
		return ImportResponse{Success: false, Error: err.Error()}
	}

	if impResult == nil {
		return ImportResponse{Success: false, Error: "import failed: no result returned"}
	}

	warnings := make([]string, 0)
	if len(impResult.Errors) > 0 {
		maxWarnings := 20
		for i, errMsg := range impResult.Errors {
			if i >= maxWarnings {
				warnings = append(warnings, "... and more")
				break
			}
			warnings = append(warnings, errMsg)
		}
	}

	cols := make([]string, 0)
	if impResult.ColumnNames != nil {
		cols = impResult.ColumnNames
	}

	delim := ","
	if impResult.Delimiter != 0 {
		delim = string(impResult.Delimiter)
	}

	return ImportResponse{
		Success:      true,
		TableName:    tableName,
		RowsImported: int(impResult.RowsInserted),
		RowsSkipped:  int(impResult.RowsSkipped),
		Columns:      cols,
		Warnings:     warnings,
		Delimiter:    delim,
		HadHeader:    impResult.HadHeader,
	}
}

// OpenFileDialog opens the native file dialog
func (a *App) OpenFileDialog() (string, error) {
	if a.ctx == nil {
		return "", fmt.Errorf("application context not available")
	}
	path, err := wailsrt.OpenFileDialog(a.ctx, wailsrt.OpenDialogOptions{
		Title: "Select file to import",
	})
	if err != nil {
		return "", err
	}
	return path, nil
}

// ExecuteImportFromPath reads a file from disk and imports it
func (a *App) ExecuteImportFromPath(path, tableName string) ImportResponse {
	if path == "" {
		return ImportResponse{Success: false, Error: "no file path provided"}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ImportResponse{Success: false, Error: err.Error()}
	}
	filename := filepath.Base(path)
	return a.ExecuteImport(filename, string(b), tableName)
}

// ListTables returns the list of table names
func (a *App) ListTables() []string {
	if a.nativeDB == nil {
		return nil
	}
	tbls := a.nativeDB.ListTables(a.tenant)
	out := make([]string, 0, len(tbls))
	for _, t := range tbls {
		if t != nil {
			out = append(out, t.Name)
		}
	}
	return out
}

// GetTableInfo returns detailed information about a table
func (a *App) GetTableInfo(tableName string) TableInfo {
	if a.nativeDB == nil {
		return TableInfo{}
	}

	table, _ := a.nativeDB.Get(a.tenant, tableName)
	if table == nil {
		return TableInfo{}
	}

	cols := make([]ColumnInfo, len(table.Cols))
	for i, col := range table.Cols {
		cols[i] = ColumnInfo{
			Name: col.Name,
			Type: col.Type.String(),
		}
	}

	return TableInfo{
		Name:     table.Name,
		Columns:  cols,
		RowCount: len(table.Rows),
	}
}

// SaveDatabaseToFile saves the database to a file
func (a *App) SaveDatabaseToFile() (string, error) {
	if a.ctx == nil {
		return "", fmt.Errorf("application context not available")
	}
	if a.nativeDB == nil {
		return "", fmt.Errorf("no database to save")
	}

	path, err := wailsrt.SaveFileDialog(a.ctx, wailsrt.SaveDialogOptions{
		Title:           "Save Database",
		DefaultFilename: "database.gob",
	})
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", nil // User cancelled
	}

	err = tinysql.SaveToFile(a.nativeDB, path)
	if err != nil {
		return "", err
	}

	return path, nil
}

// LoadDatabaseFromFile loads a database from a file
func (a *App) LoadDatabaseFromFile() (string, error) {
	if a.ctx == nil {
		return "", fmt.Errorf("application context not available")
	}

	path, err := wailsrt.OpenFileDialog(a.ctx, wailsrt.OpenDialogOptions{
		Title: "Load Database",
	})
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", nil // User cancelled
	}

	db, err := tinysql.LoadFromFile(path)
	if err != nil {
		return "", err
	}

	a.nativeDB = db
	drv.SetDefaultDB(db)

	// Reconnect SQL driver
	if a.db != nil {
		a.db.Close()
	}
	a.db, err = sql.Open("tinysql", "mem://?tenant="+a.tenant)
	if err != nil {
		return "", err
	}

	return path, nil
}

// ExportTableToCSV exports a table to CSV format
func (a *App) ExportTableToCSV(tableName string) (string, error) {
	path, err := wailsrt.SaveFileDialog(a.ctx, wailsrt.SaveDialogOptions{
		Title:           "Export Table to CSV",
		DefaultFilename: tableName + ".csv",
		Filters: []wailsrt.FileFilter{
			{DisplayName: "CSV Files (*.csv)", Pattern: "*.csv"},
		},
	})

	if err != nil || path == "" {
		return "", err
	}

	// Query all data from table
	rows, err := a.db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	// Create CSV file
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write(cols); err != nil {
		return "", err
	}

	// Write rows
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}

		record := make([]string, len(cols))
		for i, v := range values {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", v)
			}
		}

		if err := writer.Write(record); err != nil {
			return "", err
		}
		rowCount++
	}

	return fmt.Sprintf("Exported %d rows to %s", rowCount, path), nil
}
