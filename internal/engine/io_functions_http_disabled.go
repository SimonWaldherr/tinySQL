//go:build no_http

package engine

import "fmt"

// evalHTTPFunc is intentionally unavailable when built with -tags no_http.
// Keeping the SQL function registered produces an actionable query error
// instead of an "unknown function" parse error, mirroring
// io_functions_tinygo.go's identical stub for TinyGo targets. Unlike that
// case, net/http works fine under the standard js/wasm target this tag is
// meant for (it dispatches through the browser's fetch API) -- this is an
// opt-in size/feature trade a specific binary's build script makes for
// itself, not a platform limitation.
func evalHTTPFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("http(): unavailable in this build")
}
