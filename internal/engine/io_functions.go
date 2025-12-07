package engine

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ==================== IO Scalar Functions ====================
// file(path) - Read file contents as bytes/text
// http(url) - Fetch URL contents as bytes/text
// http_get(url, headers_json) - Fetch with custom headers
// http_post(url, body, headers_json) - POST request

// evalFileFunc reads a file from the filesystem and returns its contents
func evalFileFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("file() expects 1 argument: path")
	}

	pathVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if pathVal == nil {
		return nil, nil
	}

	path, ok := pathVal.(string)
	if !ok {
		return nil, fmt.Errorf("file(): path must be a string")
	}

	// Security: restrict to safe paths only (no parent directory traversal)
	cleanPath := filepath.Clean(path)
	if strings.Contains(cleanPath, "..") {
		return nil, fmt.Errorf("file(): path traversal not allowed")
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("file(): %v", err)
	}

	// Return as string (UTF-8 text)
	return string(data), nil
}

// evalHTTPFunc fetches content from a URL
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

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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

// ==================== Transform Functions ====================
// gunzip(data) - Decompress gzip data
// gzip(data) - Compress data with gzip
// base64_encode(data) - Encode to base64
// base64_decode(data) - Decode from base64
// unzip(data) - Alias for gunzip

// evalGunzipFunc decompresses gzip-compressed data
func evalGunzipFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("gunzip() expects 1 argument: data")
	}

	dataVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if dataVal == nil {
		return nil, nil
	}

	var data []byte
	switch v := dataVal.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return nil, fmt.Errorf("gunzip(): data must be string or bytes")
	}

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gunzip(): %v", err)
	}
	defer reader.Close()

	uncompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("gunzip(): %v", err)
	}

	return string(uncompressed), nil
}

// evalGzipFunc compresses data with gzip
func evalGzipFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("gzip() expects 1 argument: data")
	}

	dataVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if dataVal == nil {
		return nil, nil
	}

	data, ok := dataVal.(string)
	if !ok {
		return nil, fmt.Errorf("gzip(): data must be a string")
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write([]byte(data)); err != nil {
		return nil, fmt.Errorf("gzip(): %v", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("gzip(): %v", err)
	}

	return buf.String(), nil
}

// evalBase64EncodeFunc encodes data to base64
func evalBase64EncodeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("base64_encode() expects 1 argument: data")
	}

	dataVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if dataVal == nil {
		return nil, nil
	}

	data, ok := dataVal.(string)
	if !ok {
		return nil, fmt.Errorf("base64_encode(): data must be a string")
	}

	return base64.StdEncoding.EncodeToString([]byte(data)), nil
}
