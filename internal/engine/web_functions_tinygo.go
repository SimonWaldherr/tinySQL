//go:build tinygo.wasm || baremetal || tinysql_minimal

package engine

import "fmt"

// HTML templates are stubbed on TinyGo and in tinysql_minimal builds so the
// html/template dependency (and its reflection-driven linker cost) is dropped.

func evalHTMLTemplate(ExecEnv, *FuncCall, Row) (any, error) {
	return nil, fmt.Errorf("HTML_TEMPLATE is not supported on this target")
}
