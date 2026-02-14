//go:build js && wasm

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

var (
	db     *tinysql.DB
	tenant = "default"
	// lastResult caches the most recent query result for client-side export.
	lastResult     *tinysql.ResultSet
	lastQueryDurMs float64
)

func main() {
	c := make(chan struct{})

	// Initialize database
	db = tinysql.NewDB()

	// Register JavaScript functions
	js.Global().Set("importFile", js.FuncOf(importFile))
	js.Global().Set("executeQuery", js.FuncOf(executeQuery))
	js.Global().Set("executeMulti", js.FuncOf(executeMulti))
	js.Global().Set("clearDatabase", js.FuncOf(clearDatabase))
	js.Global().Set("listTables", js.FuncOf(listTables))
	js.Global().Set("exportResults", js.FuncOf(exportResults))
	js.Global().Set("getTableSchema", js.FuncOf(getTableSchema))

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
	case ".xml":
		// XML import: convert simple row-based XML to JSON objects client-side
		// then feed through the JSON importer.
		xmlRows, xmlErr := parseSimpleXML(fileContent)
		if xmlErr != nil {
			return map[string]interface{}{
				"success": false,
				"error":   "XML parse error: " + xmlErr.Error(),
			}
		}
		jsonBytes, _ := json.Marshal(xmlRows)
		reader = strings.NewReader(string(jsonBytes))
		impResult, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, reader, opts)
	default:
		return map[string]interface{}{
			"success": false,
			"error":   "Unsupported file format: " + ext + ". Supported: .csv, .tsv, .txt, .json, .jsonl, .ndjson, .xml",
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

	start := time.Now()

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

	lastQueryDurMs = float64(time.Since(start).Microseconds()) / 1000.0

	// Convert result to JSON/JS-friendly format
	if result == nil {
		lastResult = nil
		return map[string]interface{}{
			"success":    true,
			"columns":    []interface{}{},
			"rows":       []interface{}{},
			"durationMs": lastQueryDurMs,
		}
	}

	lastResult = result

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
		"success":    true,
		"columns":    stringsToInterfaces(result.Cols),
		"rows":       safeRows,
		"durationMs": lastQueryDurMs,
	}
}

// executeMulti runs multiple semicolon-separated SQL statements and returns
// the result of the last SELECT (or an aggregate summary).
func executeMulti(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{"success": false, "error": "Usage: executeMulti(sql)"}
	}
	raw := args[0].String()
	stmts := splitStatements(raw)
	if len(stmts) == 0 {
		return map[string]interface{}{"success": false, "error": "No SQL statements found"}
	}

	ctx := context.Background()
	start := time.Now()

	var lastRS *tinysql.ResultSet
	for i, s := range stmts {
		stmt, err := tinysql.ParseSQL(s)
		if err != nil {
			return map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("Parse error in statement %d: %v", i+1, err),
			}
		}
		rs, err := tinysql.Execute(ctx, db, tenant, stmt)
		if err != nil {
			return map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("Execute error in statement %d: %v", i+1, err),
			}
		}
		if rs != nil {
			lastRS = rs
		}
	}

	lastQueryDurMs = float64(time.Since(start).Microseconds()) / 1000.0
	lastResult = lastRS

	if lastRS == nil {
		return map[string]interface{}{
			"success":       true,
			"columns":       []interface{}{},
			"rows":          []interface{}{},
			"durationMs":    lastQueryDurMs,
			"statementsRun": len(stmts),
		}
	}

	safeRows := make([]interface{}, 0, len(lastRS.Rows))
	for _, row := range lastRS.Rows {
		r := make(map[string]interface{})
		for _, col := range lastRS.Cols {
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
		"success":       true,
		"columns":       stringsToInterfaces(lastRS.Cols),
		"rows":          safeRows,
		"durationMs":    lastQueryDurMs,
		"statementsRun": len(stmts),
	}
}

// clearDatabase clears all tables from the database
func clearDatabase(this js.Value, args []js.Value) interface{} {
	db = tinysql.NewDB()
	lastResult = nil
	return map[string]interface{}{
		"success": true,
		"message": "Database cleared",
	}
}

// listTables returns the names and row counts of all loaded tables,
// plus virtual sys.* and catalog.* tables.
func listTables(this js.Value, args []js.Value) interface{} {
	tables := db.ListTables(tenant)
	out := make([]interface{}, 0, len(tables)+20)

	// ── Real (user) tables ────────────────────────────────────────────────
	for _, tbl := range tables {
		cols := make([]interface{}, len(tbl.Cols))
		for i, c := range tbl.Cols {
			cols[i] = map[string]interface{}{
				"name": c.Name,
				"type": c.Type.String(),
			}
		}
		out = append(out, map[string]interface{}{
			"name":    tbl.Name,
			"rows":    len(tbl.Rows),
			"columns": cols,
			"kind":    "table",
		})
	}

	// ── Virtual sys.* tables ──────────────────────────────────────────────
	sysNames := []string{
		"tables", "columns", "constraints", "indexes",
		"views", "functions", "variables", "status",
		"memory", "storage", "config", "connections",
	}
	for _, n := range sysNames {
		out = append(out, map[string]interface{}{
			"name":    "sys." + n,
			"rows":    -1, // dynamic
			"columns": []interface{}{},
			"kind":    "virtual",
		})
	}

	// ── Virtual catalog.* tables ──────────────────────────────────────────
	catNames := []string{"tables", "columns", "functions", "jobs", "views"}
	for _, n := range catNames {
		out = append(out, map[string]interface{}{
			"name":    "catalog." + n,
			"rows":    -1,
			"columns": []interface{}{},
			"kind":    "virtual",
		})
	}

	return map[string]interface{}{
		"success": true,
		"tables":  out,
	}
}

// getTableSchema returns column names, types and row count for a table.
// For virtual tables (sys.*, catalog.*) it runs SELECT * LIMIT 1 to discover columns.
func getTableSchema(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{"success": false, "error": "Usage: getTableSchema(tableName)"}
	}
	name := args[0].String()

	// Virtual tables – discover columns by running a LIMIT 1 query.
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "sys.") || strings.HasPrefix(lower, "catalog.") {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		q := fmt.Sprintf("SELECT * FROM %s LIMIT 1", name)
		stmt, err := tinysql.ParseSQL(q)
		if err != nil {
			return map[string]interface{}{"success": false, "error": err.Error()}
		}
		rs, err := tinysql.Execute(ctx, db, tenant, stmt)
		if err != nil {
			return map[string]interface{}{"success": false, "error": err.Error()}
		}
		cols := make([]interface{}, len(rs.Cols))
		for i, c := range rs.Cols {
			cols[i] = map[string]interface{}{"name": c, "type": "DYNAMIC"}
		}
		return map[string]interface{}{
			"success": true,
			"name":    name,
			"rows":    len(rs.Rows),
			"virtual": true,
			"columns": cols,
		}
	}

	tbl, err := db.Get(tenant, name)
	if err != nil {
		return map[string]interface{}{"success": false, "error": "Table not found: " + name}
	}
	cols := make([]interface{}, len(tbl.Cols))
	for i, c := range tbl.Cols {
		cols[i] = map[string]interface{}{
			"name": c.Name,
			"type": c.Type.String(),
		}
	}
	return map[string]interface{}{
		"success": true,
		"name":    name,
		"rows":    len(tbl.Rows),
		"columns": cols,
	}
}

// exportResults exports the last query result in the requested format.
// format: "csv", "json", "xml"
func exportResults(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{"success": false, "error": "Usage: exportResults(format)"}
	}
	format := strings.ToLower(args[0].String())
	if lastResult == nil || len(lastResult.Rows) == 0 {
		return map[string]interface{}{"success": false, "error": "No results to export"}
	}

	var buf bytes.Buffer
	var mimeType, ext string

	switch format {
	case "csv":
		mimeType, ext = "text/csv", "csv"
		w := csv.NewWriter(&buf)
		_ = w.Write(lastResult.Cols)
		for _, row := range lastResult.Rows {
			rec := make([]string, len(lastResult.Cols))
			for i, c := range lastResult.Cols {
				rec[i] = fmt.Sprintf("%v", row[strings.ToLower(c)])
			}
			_ = w.Write(rec)
		}
		w.Flush()
	case "json":
		mimeType, ext = "application/json", "json"
		out := make([]map[string]interface{}, len(lastResult.Rows))
		for i, row := range lastResult.Rows {
			m := make(map[string]interface{}, len(lastResult.Cols))
			for _, c := range lastResult.Cols {
				m[c] = row[strings.ToLower(c)]
			}
			out[i] = m
		}
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	case "xml":
		mimeType, ext = "application/xml", "xml"
		buf.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n")
		for _, row := range lastResult.Rows {
			buf.WriteString("  <row>\n")
			for _, c := range lastResult.Cols {
				v := fmt.Sprintf("%v", row[strings.ToLower(c)])
				buf.WriteString("    <")
				xml.Escape(&buf, []byte(c))
				buf.WriteString(">")
				xml.Escape(&buf, []byte(v))
				buf.WriteString("</")
				xml.Escape(&buf, []byte(c))
				buf.WriteString(">\n")
			}
			buf.WriteString("  </row>\n")
		}
		buf.WriteString("</rows>\n")
	default:
		return map[string]interface{}{"success": false, "error": "Unknown format: " + format + ". Use csv, json or xml."}
	}

	return map[string]interface{}{
		"success":  true,
		"data":     buf.String(),
		"mimeType": mimeType,
		"ext":      ext,
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

// stringsToInterfaces converts a []string to []interface{} for JS interop
func stringsToInterfaces(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// splitStatements splits a multi-statement SQL string on semicolons,
// ignoring semicolons inside single-quoted string literals.
func splitStatements(raw string) []string {
	var stmts []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' {
			inQuote = !inQuote
			cur.WriteByte(ch)
		} else if ch == ';' && !inQuote {
			s := strings.TrimSpace(cur.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
		} else {
			cur.WriteByte(ch)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// parseSimpleXML converts row-based XML into []map[string]string so it can be
// fed through the JSON importer. Supports both element-content and attribute
// styles.
func parseSimpleXML(data string) ([]map[string]string, error) {
	decoder := xml.NewDecoder(strings.NewReader(data))
	var rows []map[string]string
	var currentRow map[string]string
	var currentKey string
	depth := 0

	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				// row-level element
				currentRow = make(map[string]string)
				// attributes become columns
				for _, a := range t.Attr {
					currentRow[a.Name.Local] = a.Value
				}
			} else if depth == 3 {
				currentKey = t.Name.Local
			}
		case xml.CharData:
			if depth == 3 && currentKey != "" && currentRow != nil {
				currentRow[currentKey] = strings.TrimSpace(string(t))
			}
		case xml.EndElement:
			if depth == 3 {
				currentKey = ""
			}
			if depth == 2 && currentRow != nil {
				if len(currentRow) > 0 {
					rows = append(rows, currentRow)
				}
				currentRow = nil
			}
			depth--
		}
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("no row elements found in XML")
	}
	return rows, nil
}
