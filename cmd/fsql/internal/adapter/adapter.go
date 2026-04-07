// Package adapter implements FSQL table-valued functions for the tinySQL engine.
// It registers the following functions:
//
//   - files(path [, recursive])  – filesystem metadata table
//   - lines(file)                – text file line-by-line table
//   - csv_rows(file [, header])  – CSV file table
//   - json_rows(file [, path])   – JSON file table
//
// All functions are registered via tinysql.RegisterExternalTableFunc and run
// entirely in the FSQL layer without touching the tinySQL core.
package adapter

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// RegisterAll registers all FSQL table-valued functions with the tinySQL engine.
// It must be called once before any FSQL queries are executed.
func RegisterAll(scopeResolver func(string) (string, error)) {
	tinysql.RegisterExternalTableFunc(&filesFunc{resolve: scopeResolver})
	tinysql.RegisterExternalTableFunc(&linesFunc{resolve: scopeResolver})
	tinysql.RegisterExternalTableFunc(&csvRowsFunc{resolve: scopeResolver})
	tinysql.RegisterExternalTableFunc(&jsonRowsFunc{resolve: scopeResolver})
}

// ─────────────────────────────────────────────────────────────────────────────
// files(path [, recursive])
// ─────────────────────────────────────────────────────────────────────────────

type filesFunc struct {
	resolve func(string) (string, error)
}

func (f *filesFunc) Name() string { return "files" }

func (f *filesFunc) ValidateArgCount(n int) error {
	if n < 1 || n > 2 {
		return fmt.Errorf("files() expects 1-2 arguments: files(path [, recursive])")
	}
	return nil
}

func (f *filesFunc) Execute(ctx context.Context, args []any) (*tinysql.ResultSet, error) {
	root, err := resolvePathArg(f.resolve, args[0])
	if err != nil {
		return nil, fmt.Errorf("files(): %w", err)
	}

	recursive := false
	if len(args) >= 2 && args[1] != nil {
		switch v := args[1].(type) {
		case bool:
			recursive = v
		case int64:
			recursive = v != 0
		case float64:
			recursive = v != 0
		case string:
			recursive = strings.EqualFold(v, "true") || v == "1"
		}
	}

	cols := []string{"path", "name", "size", "ext", "mod_time", "is_dir"}
	var rows []tinysql.Row

	walkFn := func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !recursive && info.IsDir() && p != root {
			return filepath.SkipDir
		}
		r := tinysql.Row{
			"path":     p,
			"name":     info.Name(),
			"size":     info.Size(),
			"ext":      strings.TrimPrefix(filepath.Ext(info.Name()), "."),
			"mod_time": info.ModTime().Format(time.RFC3339),
			"is_dir":   info.IsDir(),
			// expose both qualified and unqualified names (lower-cased)
			"files.path":     p,
			"files.name":     info.Name(),
			"files.size":     info.Size(),
			"files.ext":      strings.TrimPrefix(filepath.Ext(info.Name()), "."),
			"files.mod_time": info.ModTime().Format(time.RFC3339),
			"files.is_dir":   info.IsDir(),
		}
		rows = append(rows, r)
		return nil
	}

	if err := filepath.Walk(root, walkFn); err != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("files(): walk %q: %w", root, err)
	}

	return &tinysql.ResultSet{Cols: cols, Rows: rows}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// lines(file)
// ─────────────────────────────────────────────────────────────────────────────

type linesFunc struct {
	resolve func(string) (string, error)
}

func (f *linesFunc) Name() string { return "lines" }

func (f *linesFunc) ValidateArgCount(n int) error {
	if n != 1 {
		return fmt.Errorf("lines() expects 1 argument: lines(file)")
	}
	return nil
}

func (f *linesFunc) Execute(ctx context.Context, args []any) (*tinysql.ResultSet, error) {
	path, err := resolvePathArg(f.resolve, args[0])
	if err != nil {
		return nil, fmt.Errorf("lines(): %w", err)
	}

	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lines(): open %q: %w", path, err)
	}
	defer fh.Close()

	cols := []string{"line_number", "line"}
	var rows []tinysql.Row
	scanner := bufio.NewScanner(fh)
	lineNum := 0
	for scanner.Scan() {
		if ctx.Err() != nil {
			break
		}
		lineNum++
		text := scanner.Text()
		r := tinysql.Row{
			"line_number":       lineNum,
			"line":              text,
			"lines.line_number": lineNum,
			"lines.line":        text,
		}
		rows = append(rows, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("lines(): scan %q: %w", path, err)
	}

	return &tinysql.ResultSet{Cols: cols, Rows: rows}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// csv_rows(file [, header])
// ─────────────────────────────────────────────────────────────────────────────

type csvRowsFunc struct {
	resolve func(string) (string, error)
}

func (f *csvRowsFunc) Name() string { return "csv_rows" }

func (f *csvRowsFunc) ValidateArgCount(n int) error {
	if n < 1 || n > 2 {
		return fmt.Errorf("csv_rows() expects 1-2 arguments: csv_rows(file [, header])")
	}
	return nil
}

func (f *csvRowsFunc) Execute(ctx context.Context, args []any) (*tinysql.ResultSet, error) {
	path, err := resolvePathArg(f.resolve, args[0])
	if err != nil {
		return nil, fmt.Errorf("csv_rows(): %w", err)
	}

	hasHeader := true // default: treat first row as header
	if len(args) >= 2 && args[1] != nil {
		switch v := args[1].(type) {
		case bool:
			hasHeader = v
		case int64:
			hasHeader = v != 0
		case float64:
			hasHeader = v != 0
		case string:
			hasHeader = strings.EqualFold(v, "true") || v == "1"
		}
	}

	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("csv_rows(): open %q: %w", path, err)
	}
	defer fh.Close()

	reader := csv.NewReader(fh)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	var cols []string
	var rows []tinysql.Row
	rowIdx := 0

	for {
		if ctx.Err() != nil {
			break
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv_rows(): read %q: %w", path, err)
		}
		if rowIdx == 0 {
			if hasHeader {
				cols = make([]string, len(record))
				for i, c := range record {
					cols[i] = sanitizeColName(c)
				}
				rowIdx++
				continue
			}
			// No header: generate col0, col1, ...
			cols = make([]string, len(record))
			for i := range record {
				cols[i] = fmt.Sprintf("col%d", i)
			}
		}
		r := make(tinysql.Row, len(cols)*2)
		for i, col := range cols {
			val := ""
			if i < len(record) {
				val = record[i]
			}
			r[col] = val
			r["csv_rows."+col] = val
		}
		rows = append(rows, r)
		rowIdx++
	}

	return &tinysql.ResultSet{Cols: cols, Rows: rows}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// json_rows(file [, path_expr])
// ─────────────────────────────────────────────────────────────────────────────

type jsonRowsFunc struct {
	resolve func(string) (string, error)
}

func (f *jsonRowsFunc) Name() string { return "json_rows" }

func (f *jsonRowsFunc) ValidateArgCount(n int) error {
	if n < 1 || n > 2 {
		return fmt.Errorf("json_rows() expects 1-2 arguments: json_rows(file [, path])")
	}
	return nil
}

func (f *jsonRowsFunc) Execute(ctx context.Context, args []any) (*tinysql.ResultSet, error) {
	path, err := resolvePathArg(f.resolve, args[0])
	if err != nil {
		return nil, fmt.Errorf("json_rows(): %w", err)
	}

	var pathExpr string
	if len(args) >= 2 && args[1] != nil {
		pathExpr, _ = args[1].(string)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("json_rows(): read %q: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("json_rows(): parse %q: %w", path, err)
	}

	// Navigate to the requested path
	if pathExpr != "" {
		root, err = jsonNavigate(root, pathExpr)
		if err != nil {
			return nil, fmt.Errorf("json_rows(): %w", err)
		}
	}

	return jsonToResultSet(root)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// resolvePathArg converts an argument value to a filesystem path.
func resolvePathArg(resolve func(string) (string, error), arg any) (string, error) {
	s, ok := arg.(string)
	if !ok || s == "" {
		return "", fmt.Errorf("path argument must be a non-empty string")
	}
	if resolve != nil {
		return resolve(s)
	}
	return filepath.Abs(s)
}

// sanitizeColName lowercases and replaces spaces/special chars with underscores.
func sanitizeColName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if r == ' ' || r == '-' || r == '.' || r == '/' || r == '\\' {
			b.WriteRune('_')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// jsonNavigate traverses a JSON value using a dot-separated or slash-separated
// key path (e.g. "data.items" or "data/items").
func jsonNavigate(v any, path string) (any, error) {
	parts := strings.FieldsFunc(path, func(r rune) bool { return r == '.' || r == '/' })
	cur := v
	for _, p := range parts {
		switch m := cur.(type) {
		case map[string]any:
			next, ok := m[p]
			if !ok {
				return nil, fmt.Errorf("key %q not found in JSON object", p)
			}
			cur = next
		default:
			return nil, fmt.Errorf("cannot navigate key %q in non-object JSON value", p)
		}
	}
	return cur, nil
}

// jsonToResultSet converts a JSON value (array of objects or single object)
// into a ResultSet.
func jsonToResultSet(v any) (*tinysql.ResultSet, error) {
	switch arr := v.(type) {
	case []any:
		colSet := map[string]struct{}{}
		rowMaps := make([]map[string]any, 0, len(arr))
		for _, item := range arr {
			obj, ok := item.(map[string]any)
			if !ok {
				// Treat primitive array as single-column table
				colSet["value"] = struct{}{}
				rowMaps = append(rowMaps, map[string]any{"value": item})
				continue
			}
			rm := make(map[string]any, len(obj))
			for k, val := range obj {
				key := strings.ToLower(k)
				colSet[key] = struct{}{}
				rm[key] = val
			}
			rowMaps = append(rowMaps, rm)
		}
		cols := sortedKeys(colSet)
		rows := make([]tinysql.Row, len(rowMaps))
		for i, rm := range rowMaps {
			r := make(tinysql.Row, len(rm)*2)
			for k, val := range rm {
				r[k] = val
				r["json_rows."+k] = val
			}
			rows[i] = r
		}
		return &tinysql.ResultSet{Cols: cols, Rows: rows}, nil

	case map[string]any:
		cols := make([]string, 0, len(arr))
		r := make(tinysql.Row, len(arr)*2)
		for k, val := range arr {
			key := strings.ToLower(k)
			cols = append(cols, key)
			r[key] = val
			r["json_rows."+key] = val
		}
		sort.Strings(cols)
		return &tinysql.ResultSet{Cols: cols, Rows: []tinysql.Row{r}}, nil

	default:
		return &tinysql.ResultSet{
			Cols: []string{"value"},
			Rows: []tinysql.Row{{"value": v, "json_rows.value": v}},
		}, nil
	}
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
