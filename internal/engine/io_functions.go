package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ==================== IO Scalar Functions ====================
// file(path) - Read file contents as bytes/text
// http(url) - Fetch URL contents as bytes/text
// http_get(url, headers_json) - Fetch with custom headers
// http_post(url, body, headers_json) - POST request

// evalFileFunc reads a file from the filesystem and returns its contents
func evalFileFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if sqlHostIODisabled.Load() {
		return nil, errSQLHostIODisabled
	}
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

	// Preserve the path check used by local FILE callers. The host I/O policy
	// above controls whether a SQL service permits filesystem access.
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

	var src io.Reader
	switch v := dataVal.(type) {
	case string:
		src = strings.NewReader(v)
	case []byte:
		src = bytes.NewReader(v)
	default:
		return nil, fmt.Errorf("gunzip(): data must be string or bytes")
	}
	return gunzipText(env.ctx, src, MaxBlobBytes)
}

// The limit covers the combined decoded size of all members in the stream.
func gunzipText(ctx context.Context, src io.Reader, limit int) (string, error) {
	reader, err := gzip.NewReader(src)
	if err != nil {
		return "", fmt.Errorf("gunzip(): %w", err)
	}
	defer reader.Close()
	var data strings.Builder
	var buf [4 << 10]byte
	for {
		if err := checkCtx(ctx); err != nil {
			return "", err
		}
		n, err := reader.Read(buf[:])
		if n > limit-data.Len() {
			return "", fmt.Errorf("gunzip(): decoded data exceeds %d bytes", limit)
		}
		data.Write(buf[:n])
		if err == io.EOF {
			return data.String(), nil
		}
		if err != nil {
			return "", fmt.Errorf("gunzip(): %w", err)
		}
	}
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
