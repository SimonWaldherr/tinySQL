package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// textToColumnsFunc splits one text into one row, retaining empty fields.
// This is delimiter splitting, not CSV parsing: quotes have no special meaning.
type textToColumnsFunc struct{}

func (*textToColumnsFunc) Name() string { return "TEXT_TO_COLUMNS" }

func (*textToColumnsFunc) ValidateArgs(args []Expr) error {
	if len(args) != 2 {
		return fmt.Errorf("TEXT_TO_COLUMNS expects 2 arguments: (text, delimiter)")
	}
	return nil
}

func (f *textToColumnsFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
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
	if value == nil || delimiter == nil {
		return &ResultSet{Cols: []string{}, Rows: []Row{}}, nil
	}
	parts := strings.Split(valueText(value), valueText(delimiter))
	cols := make([]string, len(parts))
	result := make(Row, len(parts))
	for i, part := range parts {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		name := "column" + strconv.Itoa(i+1)
		cols[i] = name
		result[name] = part
	}
	return &ResultSet{Cols: cols, Rows: []Row{result}}, nil
}

func evalTextToColumnsScalar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("TEXT_TO_COLUMNS returns a table; use SELECT * FROM TEXT_TO_COLUMNS(text, delimiter)")
}

func init() { RegisterTableFunc(&textToColumnsFunc{}) }
