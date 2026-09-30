//go:build !tinygo.wasm && !baremetal

package engine

import (
	"fmt"
	"html"
)

// HTML_ESCAPE stays available in minimal builds without html/template.
func evalHTMLEscape(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("HTML_ESCAPE expects 1 argument")
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil || value == nil {
		return nil, err
	}
	return html.EscapeString(valueText(value)), nil
}
