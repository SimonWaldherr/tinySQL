package engine

import (
	"context"
	"fmt"
	"strings"
)

// stringSplitFunc emits one row per field. Delimiters are literal strings,
// including multiple Unicode characters; empty fields retain their ordinals.
type stringSplitFunc struct{}

func (*stringSplitFunc) Name() string { return "STRING_SPLIT" }

func (*stringSplitFunc) ValidateArgs(args []Expr) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("STRING_SPLIT expects 2 or 3 arguments: (text, delimiter[, enable_ordinal])")
	}
	return nil
}

func (f *stringSplitFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	delimiter, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	ordinal := false
	if len(args) == 3 {
		flag, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		n, ok := integerOperand(flag)
		if !ok || n < 0 || n > 1 {
			return nil, fmt.Errorf("STRING_SPLIT enable_ordinal must be integer 0 or 1")
		}
		ordinal = n == 1
	}
	cols := []string{"value"}
	if ordinal {
		cols = append(cols, "ordinal")
	}
	result := &ResultSet{Cols: cols, Rows: []Row{}}
	if value == nil || delimiter == nil {
		return result, nil
	}
	text, separator := valueText(value), valueText(delimiter)
	if separator == "" {
		return nil, fmt.Errorf("STRING_SPLIT delimiter must not be empty")
	}
	if text == "" {
		return result, nil
	}
	// SplitSeq avoids an intermediate slice proportional to the field count.
	index := 0
	for part := range strings.SplitSeq(text, separator) {
		if index&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		index++
		output := Row{"value": part}
		if ordinal {
			output["ordinal"] = index
		}
		result.Rows = append(result.Rows, output)
	}
	return result, nil
}

func evalStringSplitScalar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("STRING_SPLIT returns a table; use SELECT * FROM STRING_SPLIT(text, delimiter[, enable_ordinal])")
}

func init() { RegisterTableFunc(&stringSplitFunc{}) }
