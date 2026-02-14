package engine

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
)

// TableFunction represents a table-valued function that can be used in FROM clauses
// Example: SELECT * FROM table_from_json(http('...'), spec)
type TableFunction interface {
	// Name returns the function name (e.g., "table_from_json")
	Name() string

	// Execute evaluates the TVF and returns a result set (columns + rows)
	Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error)

	// ValidateArgs checks if the provided arguments are valid
	ValidateArgs(args []Expr) error
}

// TableFuncRegistry stores registered table-valued functions
var tableFuncRegistry = make(map[string]TableFunction)

// RegisterTableFunc registers a table-valued function globally
func RegisterTableFunc(fn TableFunction) {
	// Store registry keys in upper-case for case-insensitive lookup from SQL
	tableFuncRegistry[strings.ToUpper(fn.Name())] = fn
}

// GetTableFunc retrieves a registered table function by name
func GetTableFunc(name string) (TableFunction, bool) {
	fn, ok := tableFuncRegistry[strings.ToUpper(name)]
	return fn, ok
}

// Scalar-stub handlers for table-valued function names.
// These are used when a user accidentally calls a TVF in scalar context
// (e.g. SELECT TABLE_FROM_JSON(...)) — provide a clear error message.
func evalTableFromJSONScalar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("table-valued function %s used as scalar; use in FROM clause (parser support pending)", ex.Name)
}

func evalTableFromJSONLinesScalar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("table-valued function %s used as scalar; use in FROM clause (parser support pending)", ex.Name)
}

func evalTableFromCSVScalar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("table-valued function %s used as scalar; use in FROM clause (parser support pending)", ex.Name)
}

// TableFuncCall represents a table function call in a FROM clause
type TableFuncCall struct {
	Name  string
	Args  []Expr
	Alias string // Optional table alias
}

// ==================== JSON Table Function ====================

// JSONTableFunc implements table_from_json(source, spec)
type JSONTableFunc struct{}

func (f *JSONTableFunc) Name() string {
	return "table_from_json"
}

func (f *JSONTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("table_from_json expects 1-2 arguments: (source [, spec])")
	}
	return nil
}

func (f *JSONTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	// Evaluate source argument (can be file(), http(), or direct JSON string)
	sourceVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("table_from_json: %v", err)
	}
	if sourceVal == nil {
		return &ResultSet{Cols: []string{}, Rows: []Row{}}, nil
	}

	source, ok := sourceVal.(string)
	if !ok {
		return nil, fmt.Errorf("table_from_json: source must be a string")
	}

	// Optional spec for schema definition
	var spec string
	if len(args) > 1 {
		specVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, fmt.Errorf("table_from_json: %v", err)
		}
		if specVal != nil {
			spec, ok = specVal.(string)
			if !ok {
				return nil, fmt.Errorf("table_from_json: spec must be a string")
			}
		}
	}

	// Parse JSON and convert to table
	return parseJSONToTable(source, spec)
}

// ==================== JSON Lines Table Function ====================

// JSONLinesTableFunc implements table_from_json_lines(source)
type JSONLinesTableFunc struct{}

func (f *JSONLinesTableFunc) Name() string {
	return "table_from_json_lines"
}

func (f *JSONLinesTableFunc) ValidateArgs(args []Expr) error {
	if len(args) != 1 {
		return fmt.Errorf("table_from_json_lines expects 1 argument: source")
	}
	return nil
}

func (f *JSONLinesTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	sourceVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("table_from_json_lines: %v", err)
	}
	if sourceVal == nil {
		return &ResultSet{Cols: []string{}, Rows: []Row{}}, nil
	}

	source, ok := sourceVal.(string)
	if !ok {
		return nil, fmt.Errorf("table_from_json_lines: source must be a string")
	}

	return parseJSONLinesToTable(source)
}

// ==================== CSV Table Function ====================

// CSVTableFunc implements table_from_csv(source, options)
type CSVTableFunc struct{}

func (f *CSVTableFunc) Name() string {
	return "table_from_csv"
}

func (f *CSVTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("table_from_csv expects 1-2 arguments: (source [, options])")
	}
	return nil
}

func (f *CSVTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	sourceVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("table_from_csv: %v", err)
	}
	if sourceVal == nil {
		return &ResultSet{Cols: []string{}, Rows: []Row{}}, nil
	}

	source, ok := sourceVal.(string)
	if !ok {
		return nil, fmt.Errorf("table_from_csv: source must be a string")
	}

	// Optional options JSON
	var options string
	if len(args) > 1 {
		optVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, fmt.Errorf("table_from_csv: %v", err)
		}
		if optVal != nil {
			options, ok = optVal.(string)
			if !ok {
				return nil, fmt.Errorf("table_from_csv: options must be a string")
			}
		}
	}

	// Parse options if provided (JSON) and map to importer.ImportOptions
	var opts importer.ImportOptions
	if options != "" {
		if err := json.Unmarshal([]byte(options), &opts); err != nil {
			return nil, fmt.Errorf("table_from_csv: invalid options JSON: %v", err)
		}
	} else {
		// sensible defaults: allow importer detect header/delimiter
		opts = importer.ImportOptions{}
	}

	// Use a temporary table name to let the importer create/insert data, then read and drop it
	tmp := fmt.Sprintf("__tvf_csv_%x", md5.Sum([]byte(source)))
	// Ensure importer will create the table and not attempt to truncate existing data unless requested
	opts.TableName = tmp
	// Let importer create the table and insert rows
	_, err = importer.ImportCSV(ctx, env.db, env.tenant, tmp, strings.NewReader(source), &opts)
	if err != nil {
		return nil, fmt.Errorf("table_from_csv import: %v", err)
	}
	// Read table back from DB
	t, err := env.db.Get(env.tenant, tmp)
	if err != nil {
		return nil, fmt.Errorf("table_from_csv read back: %v", err)
	}
	cols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		cols[i] = strings.ToLower(c.Name)
	}
	rows := make([]Row, 0, len(t.Rows))
	for _, r := range t.Rows {
		rr := make(Row)
		for i, c := range t.Cols {
			putVal(rr, c.Name, r[i])
		}
		rows = append(rows, rr)
	}
	// Cleanup: drop temporary table (best-effort)
	_ = env.db.Drop(env.tenant, tmp)
	// Return result set
	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// ==================== Helper functions (to be implemented) ====================

func parseJSONToTable(jsonStr string, spec string) (*ResultSet, error) {
	// Try to unmarshal JSON. Accept either a single object or an array of objects.
	var anyv any
	if err := json.Unmarshal([]byte(jsonStr), &anyv); err != nil {
		return nil, fmt.Errorf("parse JSON: %v", err)
	}

	switch v := anyv.(type) {
	case []any:
		// Array: each element may be an object or primitive
		colsSet := map[string]struct{}{}
		rows := make([]Row, 0, len(v))
		for _, item := range v {
			switch it := item.(type) {
			case map[string]any:
				for k := range it {
					colsSet[strings.ToLower(k)] = struct{}{}
				}
			default:
				colsSet["value"] = struct{}{}
			}
		}
		cols := make([]string, 0, len(colsSet))
		for c := range colsSet {
			cols = append(cols, c)
		}
		sort.Strings(cols)

		for _, item := range v {
			r := make(Row)
			switch it := item.(type) {
			case map[string]any:
				for k, val := range it {
					r[strings.ToLower(k)] = val
				}
			default:
				r["value"] = it
			}
			rows = append(rows, r)
		}
		return &ResultSet{Cols: cols, Rows: rows}, nil
	case map[string]any:
		// Single object -> treat as single-row table with object keys
		cols := make([]string, 0, len(v))
		for k := range v {
			cols = append(cols, strings.ToLower(k))
		}
		sort.Strings(cols)
		r := make(Row)
		for k, val := range v {
			r[strings.ToLower(k)] = val
		}
		return &ResultSet{Cols: cols, Rows: []Row{r}}, nil
	default:
		// Primitive -> single-column table
		return &ResultSet{Cols: []string{"value"}, Rows: []Row{{"value": v}}}, nil
	}
}

func parseJSONLinesToTable(jsonlStr string) (*ResultSet, error) {
	scanner := bufio.NewScanner(strings.NewReader(jsonlStr))
	colsSet := map[string]struct{}{}
	rows := []Row{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var anyv any
		if err := json.Unmarshal([]byte(line), &anyv); err != nil {
			return nil, fmt.Errorf("parse JSONL line: %v", err)
		}
		switch it := anyv.(type) {
		case map[string]any:
			for k := range it {
				colsSet[strings.ToLower(k)] = struct{}{}
			}
			r := make(Row)
			for k, val := range it {
				r[strings.ToLower(k)] = val
			}
			rows = append(rows, r)
		default:
			colsSet["value"] = struct{}{}
			rows = append(rows, Row{"value": it})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	cols := make([]string, 0, len(colsSet))
	for c := range colsSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// Note: CSV parsing is now delegated to importer.ImportCSV in CSVTableFunc.Execute.
// The older parseCSVToTable helper is intentionally removed in favor of the
// robust importer implementation.

// ==================== XML Table Function ====================

// XMLTableFunc implements table_from_xml(source, record_name)
type XMLTableFunc struct{}

func (f *XMLTableFunc) Name() string { return "table_from_xml" }

func (f *XMLTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("table_from_xml expects 1-2 arguments: (source [, record_name])")
	}
	return nil
}

func (f *XMLTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	srcVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("table_from_xml: %v", err)
	}
	if srcVal == nil {
		return &ResultSet{Cols: []string{}, Rows: []Row{}}, nil
	}
	source, ok := srcVal.(string)
	if !ok {
		return nil, fmt.Errorf("table_from_xml: source must be a string")
	}
	var recordName string
	if len(args) > 1 {
		rn, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, fmt.Errorf("table_from_xml: %v", err)
		}
		if rn != nil {
			recordName, _ = rn.(string)
		}
	}
	return parseXMLToTable(source, recordName)
}

func parseXMLToTable(xmlStr string, recordName string) (*ResultSet, error) {
	// Support simple XPath-like paths (e.g. "root/records/record")
	// and include attributes as columns named "attr_<name>".
	path := strings.TrimPrefix(strings.TrimSpace(recordName), "/")
	var pathSegments []string
	if path != "" {
		pathSegments = strings.Split(path, "/")
	}

	dec := xml.NewDecoder(strings.NewReader(xmlStr))
	stack := []string{}
	colsSet := map[string]struct{}{}
	rows := []Row{}

	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
			// check if current stack ends with pathSegments (if provided)
			match := false
			if len(pathSegments) == 0 {
				// no explicit path: we guess a repeating child under root later
				match = false
			} else if len(stack) >= len(pathSegments) {
				ok := true
				offset := len(stack) - len(pathSegments)
				for i := range pathSegments {
					if stack[offset+i] != pathSegments[i] {
						ok = false
						break
					}
				}
				match = ok
			}
			if match {
				// decode this element into tokens until its matching end
				// collect first-level child elements and attributes
				r := make(Row)
				// attributes
				for _, a := range t.Attr {
					key := strings.ToLower("attr_" + a.Name.Local)
					r[key] = strings.TrimSpace(a.Value)
					colsSet[key] = struct{}{}
				}

				// read tokens inside the element
				depth := 1
				var curElem string
				var buf strings.Builder
				for depth > 0 {
					nt, err := dec.Token()
					if err != nil {
						break
					}
					switch tt := nt.(type) {
					case xml.StartElement:
						// new child; set current element name and reset buffer
						curElem = tt.Name.Local
						// capture its attributes too
						for _, a := range tt.Attr {
							k := strings.ToLower("attr_" + tt.Name.Local + "_" + a.Name.Local)
							r[k] = strings.TrimSpace(a.Value)
							colsSet[k] = struct{}{}
						}
						buf.Reset()
						depth++
					case xml.CharData:
						if curElem != "" {
							buf.WriteString(string(tt))
						}
					case xml.EndElement:
						if curElem != "" && strings.EqualFold(tt.Name.Local, curElem) {
							key := strings.ToLower(curElem)
							val := strings.TrimSpace(buf.String())
							r[key] = val
							colsSet[key] = struct{}{}
							curElem = ""
						}
						depth--
					}
				}
				rows = append(rows, r)
				// pop the element we matched from stack
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
			}
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}

	// If no explicit path was given, attempt to guess a repeating child under root
	if len(pathSegments) == 0 && len(rows) == 0 {
		// naive approach: find first element name that repeats and use it
		dec2 := xml.NewDecoder(strings.NewReader(xmlStr))
		counts := map[string]int{}
		for {
			tok, err := dec2.Token()
			if err != nil {
				break
			}
			if se, ok := tok.(xml.StartElement); ok {
				counts[se.Name.Local]++
			}
		}
		// pick most frequent element (excluding the document root)
		best := ""
		bestCount := 0
		for k, v := range counts {
			if v > bestCount {
				best = k
				bestCount = v
			}
		}
		if best != "" {
			// re-run extraction for best
			return parseXMLToTable(xmlStr, best)
		}
	}

	cols := make([]string, 0, len(colsSet))
	for c := range colsSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// Register table functions on package init
func init() {
	RegisterTableFunc(&JSONTableFunc{})
	RegisterTableFunc(&JSONLinesTableFunc{})
	RegisterTableFunc(&CSVTableFunc{})
	RegisterTableFunc(&XMLTableFunc{})
}
