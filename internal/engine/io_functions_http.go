//go:build !tinygo.wasm && !baremetal

package engine

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// evalHTTPFunc fetches content from a URL on full Go runtimes.
func evalHTTPFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("http() expects 1 argument: url")
	}

	urlVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if urlVal == nil {
		return nil, nil
	}

	url, ok := urlVal.(string)
	if !ok {
		return nil, fmt.Errorf("http(): url must be a string")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http(): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http(): server returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("http(): %v", err)
	}
	return string(data), nil
}
