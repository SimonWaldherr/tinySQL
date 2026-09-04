//go:build tinygo.wasm || baremetal

package engine

import "fmt"

func evalHTMLEscape(ExecEnv, *FuncCall, Row) (any, error) {
	return nil, fmt.Errorf("HTML_ESCAPE is not supported on this target")
}

func evalHTMLTemplate(ExecEnv, *FuncCall, Row) (any, error) {
	return nil, fmt.Errorf("HTML_TEMPLATE is not supported on this target")
}
