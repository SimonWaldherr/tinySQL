//go:build tinygo.wasm || baremetal

package engine

import "fmt"

func evalHTMLEscape(ExecEnv, *FuncCall, Row) (any, error) {
	return nil, fmt.Errorf("HTML_ESCAPE is not supported on this target")
}
