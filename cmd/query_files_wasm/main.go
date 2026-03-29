//go:build js && wasm

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"syscall/js"
	"time"
	"unicode"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const (
	defaultTenant        = "default"
	defaultQueryTimeout  = 30 * time.Second
	defaultImportTimeout = 60 * time.Second
	maxSQLBytes          = 256 * 1024
	queryCacheSize       = 256
)

var (
	db         *tinysql.DB
	tenant     = defaultTenant
	queryCache *tinysql.QueryCache

	// lastResult caches the most recent query result for client-side export.
	lastResult     *tinysql.ResultSet
	lastQueryDurMs float64
)

func main() {
	c := make(chan struct{})

	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)

	js.Global().Set("importFile", js.FuncOf(importFile))
	js.Global().Set("executeQuery", js.FuncOf(executeQuery))
	js.Global().Set("executeMulti", js.FuncOf(executeMulti))
	js.Global().Set("clearDatabase", js.FuncOf(clearDatabase))
	js.Global().Set("dropTable", js.FuncOf(dropTable))
	js.Global().Set("listTables", js.FuncOf(listTables))
	js.Global().Set("exportResults", js.FuncOf(exportResults))
	js.Global().Set("getTableSchema", js.FuncOf(getTableSchema))

	println("TinySQL Query Files WASM initialized!")
	<-c
}

func jsErr(msg string) map[string]interface{} {
	return map[string]interface{}{"success": false, "error": msg}
}

func normalizeSQLInput(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("SQL must not be empty")
	}
	if len(raw) > maxSQLBytes {
		return "", fmt.Errorf("SQL exceeds max size (%d bytes)", maxSQLBytes)
	}
	return raw, nil
}

func executeSQLText(sqlText string) (*tinysql.ResultSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
	defer cancel()

	compiled, err := queryCache.Compile(sqlText)
	if err != nil {
		return nil, fmt.Errorf("Parse error: %w", err)
	}

	result, err := tinysql.ExecuteCompiled(ctx, db, tenant, compiled)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("Query timeout after %s", defaultQueryTimeout)
		}
		return nil, fmt.Errorf("Execute error: %w", err)
	}
	return result, nil
}

func resultRowsToJS(result *tinysql.ResultSet) []interface{} {
	safeRows := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		outRow := make(map[string]interface{}, len(result.Cols))
		for _, col := range result.Cols {
			val, ok := row[strings.ToLower(col)]
			if !ok || val == nil {
				outRow[col] = ""
				continue
			}
			switch v := val.(type) {
			case string, int, int32, int64, float32, float64, bool:
				outRow[col] = v
			default:
				outRow[col] = fmt.Sprintf("%v", v)
			}
		}
		safeRows = append(safeRows, outRow)
	}
	return safeRows
}

func successResultPayload(result *tinysql.ResultSet, statementsRun int) map[string]interface{} {
	if result == nil {
		payload := map[string]interface{}{
			"success":    true,
			"columns":    []interface{}{},
			"rows":       []interface{}{},
			"durationMs": lastQueryDurMs,
		}
		if statementsRun > 0 {
			payload["statementsRun"] = statementsRun
		}
		return payload
	}

	payload := map[string]interface{}{
		"success":    true,
		"columns":    stringsToInterfaces(result.Cols),
		"rows":       resultRowsToJS(result),
		"durationMs": lastQueryDurMs,
	}
	if statementsRun > 0 {
		payload["statementsRun"] = statementsRun
	}
	return payload
}

// importFile imports a file (CSV, JSON, XML) into the database.
func importFile(this js.Value, args []js.Value) interface{} {
	if len(args) < 3 {
		return jsErr("Usage: importFile(fileName, fileContent, tableName)")
	}

	fileName := strings.TrimSpace(args[0].String())
	fileContent := args[1].String()
	tableName := strings.TrimSpace(args[2].String())
	if tableName == "" {
		tableName = "table"
	}

	ext := ""
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		ext = strings.ToLower(fileName[idx:])
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultImportTimeout)
	defer cancel()

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

	reader := strings.NewReader(fileContent)
	var impResult *tinysql.ImportResult
	var err error

	switch ext {
	case ".csv", ".tsv", ".txt":
		impResult, err = tinysql.FuzzyImportCSV(ctx, db, tenant, tableName, reader, opts)
	case ".json", ".jsonl", ".ndjson":
		impResult, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, reader, opts)
	case ".xml":
		xmlRows, xmlErr := parseSimpleXML(fileContent)
		if xmlErr != nil {
			return jsErr("XML parse error: " + xmlErr.Error())
		}
		jsonBytes, _ := json.Marshal(xmlRows)
		impResult, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, strings.NewReader(string(jsonBytes)), opts)
	default:
		return jsErr("Unsupported file format: " + ext + ". Supported: .csv, .tsv, .txt, .json, .jsonl, .ndjson, .xml")
	}

	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return jsErr("Import timeout after " + defaultImportTimeout.String())
		}
		return jsErr("Import failed: " + err.Error())
	}
	if impResult == nil {
		return jsErr("Import failed: no result returned")
	}

	warnings := []string{}
	if len(impResult.Errors) > 0 {
		for i, errMsg := range impResult.Errors {
			if i >= 10 {
				warnings = append(warnings, "... and more")
				break
			}
			warnings = append(warnings, errMsg)
		}
	}

	columns := []string{}
	if impResult.ColumnNames != nil {
		columns = impResult.ColumnNames
	}
	delimiter := ","
	if impResult.Delimiter != 0 {
		delimiter = string(impResult.Delimiter)
	}

	// DDL/DML changed; clear cached last result only.
	lastResult = nil

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

// executeQuery executes a single SQL query and returns results.
func executeQuery(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return jsErr("Usage: executeQuery(sqlQuery)")
	}

	queryStr, err := normalizeSQLInput(args[0].String())
	if err != nil {
		return jsErr(err.Error())
	}

	start := time.Now()
	result, err := executeSQLText(queryStr)
	if err != nil {
		return jsErr(err.Error())
	}
	lastQueryDurMs = float64(time.Since(start).Microseconds()) / 1000.0
	lastResult = result

	return successResultPayload(result, 0)
}

// executeMulti runs multiple semicolon-separated SQL statements and returns
// the result of the last SELECT (or an aggregate summary).
func executeMulti(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return jsErr("Usage: executeMulti(sql)")
	}

	raw := args[0].String()
	if len(raw) > maxSQLBytes {
		return jsErr(fmt.Sprintf("SQL exceeds max size (%d bytes)", maxSQLBytes))
	}

	stmts := splitStatements(raw)
	if len(stmts) == 0 {
		return jsErr("No SQL statements found")
	}

	start := time.Now()
	var lastRS *tinysql.ResultSet
	for i, stmtRaw := range stmts {
		stmtSQL, err := normalizeSQLInput(stmtRaw)
		if err != nil {
			return jsErr(fmt.Sprintf("Statement %d: %v", i+1, err))
		}
		rs, err := executeSQLText(stmtSQL)
		if err != nil {
			return jsErr(fmt.Sprintf("Statement %d: %v", i+1, err))
		}
		if rs != nil {
			lastRS = rs
		}
	}

	lastQueryDurMs = float64(time.Since(start).Microseconds()) / 1000.0
	lastResult = lastRS
	return successResultPayload(lastRS, len(stmts))
}

// clearDatabase clears all tables from the database.
func clearDatabase(this js.Value, args []js.Value) interface{} {
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	lastResult = nil
	return map[string]interface{}{
		"success": true,
		"message": "Database cleared",
	}
}

// dropTable removes a user table from the database.
func dropTable(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return jsErr("Usage: dropTable(tableName)")
	}
	name := strings.TrimSpace(args[0].String())
	if name == "" {
		return jsErr("table name must not be empty")
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "sys.") || strings.HasPrefix(lower, "catalog.") {
		return jsErr("virtual tables cannot be dropped")
	}
	if err := db.Drop(tenant, name); err != nil {
		return jsErr("drop failed: " + err.Error())
	}
	lastResult = nil
	return map[string]interface{}{
		"success": true,
		"message": "Table dropped",
		"table":   name,
	}
}

// listTables returns the names and row counts of all loaded tables,
// plus virtual sys.* and catalog.* tables.
func listTables(this js.Value, args []js.Value) interface{} {
	tables := db.ListTables(tenant)
	sort.Slice(tables, func(i, j int) bool {
		return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
	})

	out := make([]interface{}, 0, len(tables)+20)
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

	sysNames := []string{"tables", "columns", "constraints", "indexes", "views", "functions", "variables", "status", "memory", "storage", "config", "connections"}
	for _, n := range sysNames {
		out = append(out, map[string]interface{}{
			"name":    "sys." + n,
			"rows":    -1,
			"columns": []interface{}{},
			"kind":    "virtual",
		})
	}

	catNames := []string{"tables", "columns", "functions", "jobs", "views"}
	for _, n := range catNames {
		out = append(out, map[string]interface{}{
			"name":    "catalog." + n,
			"rows":    -1,
			"columns": []interface{}{},
			"kind":    "virtual",
		})
	}

	return map[string]interface{}{"success": true, "tables": out}
}

// getTableSchema returns column names, types and row count for a table.
func getTableSchema(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return jsErr("Usage: getTableSchema(tableName)")
	}
	name := strings.TrimSpace(args[0].String())
	if name == "" {
		return jsErr("table name must not be empty")
	}

	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "sys.") || strings.HasPrefix(lower, "catalog.") {
		q := fmt.Sprintf("SELECT * FROM %s LIMIT 1", name)
		rs, err := executeSQLText(q)
		if err != nil {
			return jsErr(err.Error())
		}
		if rs == nil {
			rs = &tinysql.ResultSet{}
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
		return jsErr("Table not found: " + name)
	}
	cols := make([]interface{}, len(tbl.Cols))
	for i, c := range tbl.Cols {
		cols[i] = map[string]interface{}{"name": c.Name, "type": c.Type.String()}
	}
	return map[string]interface{}{
		"success": true,
		"name":    name,
		"rows":    len(tbl.Rows),
		"columns": cols,
	}
}

// exportResults exports the last query result in the requested format.
// format: csv, json, xml
func exportResults(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return jsErr("Usage: exportResults(format)")
	}
	format := strings.ToLower(strings.TrimSpace(args[0].String()))
	if lastResult == nil || len(lastResult.Rows) == 0 {
		return jsErr("No results to export")
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
				v := row[strings.ToLower(c)]
				if v != nil {
					rec[i] = fmt.Sprintf("%v", v)
				}
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
				tag := xmlTagName(c)
				v := row[strings.ToLower(c)]
				text := ""
				if v != nil {
					text = fmt.Sprintf("%v", v)
				}
				buf.WriteString("    <" + tag + ">")
				xml.EscapeText(&buf, []byte(text))
				buf.WriteString("</" + tag + ">\n")
			}
			buf.WriteString("  </row>\n")
		}
		buf.WriteString("</rows>\n")
	default:
		return jsErr("Unknown format: " + format + ". Use csv, json or xml.")
	}

	return map[string]interface{}{
		"success":  true,
		"data":     buf.String(),
		"mimeType": mimeType,
		"ext":      ext,
	}
}

// stringsToInterfaces converts a []string to []interface{} for JS interop.
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
			if inQuote && i+1 < len(raw) && raw[i+1] == '\'' {
				// SQL escaped quote inside literal.
				cur.WriteByte(ch)
				cur.WriteByte(raw[i+1])
				i++
				continue
			}
			inQuote = !inQuote
			cur.WriteByte(ch)
			continue
		}
		if ch == ';' && !inQuote {
			s := strings.TrimSpace(cur.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
			continue
		}
		cur.WriteByte(ch)
	}

	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// parseSimpleXML converts row-based XML into []map[string]string so it can be
// fed through the JSON importer.
func parseSimpleXML(data string) ([]map[string]string, error) {
	decoder := xml.NewDecoder(strings.NewReader(data))
	var rows []map[string]string
	var currentRow map[string]string
	var currentKey string
	depth := 0

	for {
		tok, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				currentRow = make(map[string]string)
				for _, a := range t.Attr {
					currentRow[a.Name.Local] = a.Value
				}
			} else if depth == 3 {
				currentKey = t.Name.Local
			}
		case xml.CharData:
			if depth == 3 && currentKey != "" && currentRow != nil {
				value := strings.TrimSpace(string(t))
				if value != "" {
					currentRow[currentKey] = value
				}
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

func xmlTagName(col string) string {
	col = strings.TrimSpace(col)
	if col == "" {
		return "col"
	}
	var b strings.Builder
	for i, r := range col {
		valid := unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.'
		if i == 0 && !(unicode.IsLetter(r) || r == '_') {
			b.WriteString("c_")
		}
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "col"
	}
	return b.String()
}
