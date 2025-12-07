package engine

import (
	"context"
	"fmt"
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
	tableFuncRegistry[fn.Name()] = fn
}

// GetTableFunc retrieves a registered table function by name
func GetTableFunc(name string) (TableFunction, bool) {
	fn, ok := tableFuncRegistry[name]
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

	return parseCSVToTable(source, options)
}

// ==================== Helper functions (to be implemented) ====================

func parseJSONToTable(jsonStr string, spec string) (*ResultSet, error) {
	// TODO: Implement JSON parsing logic
	// Parse JSON, infer or apply schema, return ResultSet
	return &ResultSet{
		Cols: []string{"placeholder"},
		Rows: []Row{{"placeholder": "JSON parsing not yet implemented"}},
	}, nil
}

func parseJSONLinesToTable(jsonlStr string) (*ResultSet, error) {
	// TODO: Implement JSONL parsing logic
	return &ResultSet{
		Cols: []string{"placeholder"},
		Rows: []Row{{"placeholder": "JSONL parsing not yet implemented"}},
	}, nil
}

func parseCSVToTable(csvStr string, options string) (*ResultSet, error) {
	// TODO: Implement CSV parsing logic using existing importer package
	return &ResultSet{
		Cols: []string{"placeholder"},
		Rows: []Row{{"placeholder": "CSV parsing not yet implemented"}},
	}, nil
}

// Register table functions on package init
func init() {
	RegisterTableFunc(&JSONTableFunc{})
	RegisterTableFunc(&JSONLinesTableFunc{})
	RegisterTableFunc(&CSVTableFunc{})
}
