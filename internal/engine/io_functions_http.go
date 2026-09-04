//go:build !tinygo.wasm && !baremetal

package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const sqlHTTPMaxResponseBytes int64 = 8 << 20

// sqlHTTPClient is shared so repeated HTTP() calls reuse DNS results and idle
// TCP/TLS connections. http.Client and Transport are safe for concurrent use.
var sqlHTTPClient = func() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 16
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 20 * time.Second
	return &http.Client{Timeout: 30 * time.Second, Transport: transport}
}()

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

	return fetchHTTP(env.ctx, sqlHTTPClient, url, sqlHTTPMaxResponseBytes)
}

func fetchHTTP(ctx context.Context, client *http.Client, rawURL string, maxResponseBytes int64) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("http(): invalid URL: %v", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("http(): URL must be absolute http or https")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", fmt.Errorf("http(): %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http(): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Drain small error bodies so the shared transport can reuse the
		// connection without retaining an unbounded response in memory.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32<<10))
		return "", fmt.Errorf("http(): server returned status %d", resp.StatusCode)
	}
	if maxResponseBytes > 0 && resp.ContentLength > maxResponseBytes {
		return "", fmt.Errorf("http(): response exceeds %d bytes", maxResponseBytes)
	}

	reader := io.Reader(resp.Body)
	if maxResponseBytes > 0 {
		reader = io.LimitReader(resp.Body, maxResponseBytes+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("http(): %v", err)
	}
	if maxResponseBytes > 0 && int64(len(data)) > maxResponseBytes {
		return "", fmt.Errorf("http(): response exceeds %d bytes", maxResponseBytes)
	}
	return string(data), nil
}
