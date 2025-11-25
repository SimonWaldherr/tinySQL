package main

import (
	"context"
	"fmt"
	"strings"
	"syscall/js"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

var (
	db     *tinysql.DB
	tenant = "default"
)

func main() {
	c := make(chan struct{})

	// Initialize database
	db = tinysql.NewDB()

	// Register JavaScript functions
	js.Global().Set("importFile", js.FuncOf(importFile))
	js.Global().Set("executeQuery", js.FuncOf(executeQuery))
	js.Global().Set("clearDatabase", js.FuncOf(clearDatabase))

	println("TinySQL Query Files WASM initialized!")
	<-c
}

// importFile imports a file (CSV, JSON) into the database
func importFile(this js.Value, args []js.Value) interface{} {
	if len(args) < 3 {
		return map[string]interface{}{
			"success": false,
			"error":   "Usage: importFile(fileName, fileContent, tableName)",
		}
	}

	fileName := args[0].String()
	fileContent := args[1].String()
	tableName := args[2].String()

	// Determine file type
	ext := ""
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		ext = strings.ToLower(fileName[idx:])
	}

	// Create reader from string
	reader := strings.NewReader(fileContent)
	ctx := context.Background()

	// Import options with fuzzy parsing
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
		impResult, err = tinysql.FuzzyImportCSV(ctx, db, tenant, tableName, reader, opts)
	case ".json", ".jsonl", ".ndjson":
		impResult, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, reader, opts)
	default:
		return map[string]interface{}{
			"success": false,
			"error":   "Unsupported file format: " + ext + ". Supported: .csv, .tsv, .txt, .json, .jsonl",
		}
	}

	if err != nil {
		return map[string]interface{}{
			"success": false,
			"error":   "Import failed: " + err.Error(),
		}
	}

	if impResult == nil {
		return map[string]interface{}{
			"success": false,
			"error":   "Import failed: no result returned",
		}
	}

	warnings := []string{}
	if len(impResult.Errors) > 0 {
		maxWarnings := 10
		for i, errMsg := range impResult.Errors {
			if i >= maxWarnings {
				warnings = append(warnings, "... and more")
				break
			}
			warnings = append(warnings, errMsg)
		}
	}

	// Columns and delimiter safe handling
	columns := []string{}
	if impResult.ColumnNames != nil {
		columns = impResult.ColumnNames
	}
	delimiter := ","
	if impResult.Delimiter != 0 {
		delimiter = string(impResult.Delimiter)
	}

	// Build JS-friendly response
	return map[string]interface{}{
		"success":      true,
		"tableName":    tableName,
		"rowsImported": int(impResult.RowsInserted),
		"rowsSkipped":  int(impResult.RowsSkipped),
		"columns":      stringsToInterfaces(columns),
		"warnings":     stringsToInterfaces(warnings),
		"delimiter":    delimiter,
		"hadHeader":    impResult.HadHeader,
	}
}

// executeQuery executes a SQL query and returns results
func executeQuery(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{
			"success": false,
			"error":   "Usage: executeQuery(sqlQuery)",
		}
	}

	queryStr := args[0].String()
	ctx := context.Background()

	// Parse query
	stmt, err := tinysql.ParseSQL(queryStr)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"error":   "Parse error: " + err.Error(),
		}
	}

	// Execute query
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"error":   "Execute error: " + err.Error(),
		}
	}

	// Convert result to JSON/JS-friendly format
	if result == nil {
		return map[string]interface{}{
			"success": true,
			"columns": []interface{}{},
			"rows":    []interface{}{},
		}
	}

	// Always return []interface{} for rows, and sanitize all cell values
	safeRows := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		r := make(map[string]interface{})
		for _, col := range result.Cols {
			val, ok := row[strings.ToLower(col)]
			if !ok || val == nil {
				r[col] = ""
				continue
			}
			switch v := val.(type) {
			case string:
				r[col] = v
			case int, int32, int64, float32, float64:
				r[col] = v
			case bool:
				r[col] = v
			default:
				r[col] = fmt.Sprintf("%v", v)
			}
		}
		safeRows = append(safeRows, r)
	}
	return map[string]interface{}{
		"success": true,
		"columns": stringsToInterfaces(result.Cols),
		"rows":    safeRows,
	}
}

// clearDatabase clears all tables from the database
func clearDatabase(this js.Value, args []js.Value) interface{} {
	db = tinysql.NewDB()
	return map[string]interface{}{
		"success": true,
		"message": "Database cleared",
	}
}

// stringsToInterfaces converts a []string to []interface{} for JS interop
func stringsToInterfaces(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
