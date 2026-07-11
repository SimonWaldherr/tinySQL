//go:build tinygo.wasm || baremetal

package engine

import "fmt"

// evalHTTPFunc is intentionally unavailable on TinyGo targets. Keeping the
// SQL function registered produces an actionable query error while avoiding a
// dependency on net/http, which is not available on all TinyGo targets.
func evalHTTPFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("http(): unavailable on TinyGo targets")
}
