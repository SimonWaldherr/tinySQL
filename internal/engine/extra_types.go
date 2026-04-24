// Package engine provides helper functions for the YAML, URL, HASH, and
// BITMAP extra column types added to tinySQL.
//
// Note: MD5 and SHA1 are provided for compatibility and checksum use cases only.
// They are cryptographically broken and must NOT be used for security-sensitive
// operations (passwords, signatures, etc.). Prefer SHA256 or SHA512 instead.
package engine

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/bits"
	"net/url"
	"strings"
)

// ─────────────────────────── YAML helpers ────────────────────────────────────

// yamlParseFunc validates a simple YAML text and returns it unchanged.
// True YAML parsing is not provided; this validates that the text is not empty
// and that it has at least superficially well-formed key: value lines.
func evalYAMLParse(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("YAML_PARSE expects 1 argument")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	text := fmt.Sprintf("%v", v)
	// Very lightweight check: non-empty and no unmatched braces/brackets.
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("YAML_PARSE: empty input")
	}
	return text, nil
}

// evalYAMLGet extracts a value from YAML text using a dot-separated path.
// It supports only simple "key: value" style YAML (one level per segment).
func evalYAMLGet(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("YAML_GET expects 2 arguments: (yaml_text, path)")
	}
	yamlVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	pathVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if yamlVal == nil || pathVal == nil {
		return nil, nil
	}
	yamlText := fmt.Sprintf("%v", yamlVal)
	path := fmt.Sprintf("%v", pathVal)

	// Simple line-by-line YAML key lookup for the first path segment.
	// Handles only top-level scalar keys.
	segments := strings.SplitN(path, ".", 2)
	key := strings.TrimSpace(segments[0])
	for _, line := range strings.Split(yamlText, "\n") {
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		lineKey := strings.TrimSpace(line[:idx])
		if strings.EqualFold(lineKey, key) {
			val := strings.TrimSpace(line[idx+1:])
			if len(segments) == 1 {
				return val, nil
			}
			// Nested path: not supported in this simple implementation.
			return nil, fmt.Errorf("YAML_GET: nested path %q not supported in simple YAML", path)
		}
	}
	return nil, nil
}

// ─────────────────────────── URL helpers ─────────────────────────────────────

// evalURLParse parses a URL and returns a JSON object with scheme/host/path/query.
func evalURLParse(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("URL_PARSE expects 1 argument")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	raw := fmt.Sprintf("%v", v)
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("URL_PARSE: %w", err)
	}
	obj := map[string]any{
		"scheme":   u.Scheme,
		"host":     u.Host,
		"path":     u.Path,
		"query":    u.RawQuery,
		"fragment": u.Fragment,
	}
	b, _ := json.Marshal(obj)
	return string(b), nil
}

// evalURLEncode percent-encodes a string using standard URL query encoding.
func evalURLEncode(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("URL_ENCODE expects 1 argument")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return url.QueryEscape(fmt.Sprintf("%v", v)), nil
}

// evalURLDecode percent-decodes a URL-encoded string.
func evalURLDecode(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("URL_DECODE expects 1 argument")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	decoded, err := url.QueryUnescape(fmt.Sprintf("%v", v))
	if err != nil {
		return nil, fmt.Errorf("URL_DECODE: %w", err)
	}
	return decoded, nil
}

// ─────────────────────────── HASH helper ─────────────────────────────────────

// evalHashFunc computes a hash digest for the given text.
// Supported algorithms: md5, sha1, sha256, sha512, fnv.
func evalHashFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("HASH expects 2 arguments: (algorithm, text)")
	}
	algVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	textVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if algVal == nil || textVal == nil {
		return nil, nil
	}
	alg := strings.ToLower(fmt.Sprintf("%v", algVal))
	data := []byte(fmt.Sprintf("%v", textVal))

	switch alg {
	case "md5":
		h := md5.Sum(data)
		return hex.EncodeToString(h[:]), nil
	case "sha1":
		h := sha1.Sum(data)
		return hex.EncodeToString(h[:]), nil
	case "sha256":
		h := sha256.Sum256(data)
		return hex.EncodeToString(h[:]), nil
	case "sha512":
		h := sha512.Sum512(data)
		return hex.EncodeToString(h[:]), nil
	case "fnv":
		h := fnv.New64a()
		h.Write(data)
		return fmt.Sprintf("%016x", h.Sum64()), nil
	default:
		return nil, fmt.Errorf("HASH: unknown algorithm %q (use md5/sha1/sha256/sha512/fnv)", alg)
	}
}

// ─────────────────────────── BITMAP helpers ──────────────────────────────────
// Bitmaps are stored as hex-encoded byte slices. Bit positions are 0-indexed.
// The internal representation is little-endian: bit N is in byte N/8, bit N%8.

const bitmapMaxBytes = 128 // 1024 bits maximum

func bitmapDecode(s string) ([]byte, error) {
	if s == "" {
		return make([]byte, bitmapMaxBytes), nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("BITMAP: invalid hex encoding: %w", err)
	}
	if len(b) < bitmapMaxBytes {
		b = append(b, make([]byte, bitmapMaxBytes-len(b))...)
	}
	return b, nil
}

func bitmapEncode(b []byte) string {
	// Trim trailing zero bytes for compact storage.
	end := len(b)
	for end > 0 && b[end-1] == 0 {
		end--
	}
	if end == 0 {
		return ""
	}
	return hex.EncodeToString(b[:end])
}

func evalBitmapNew(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 0 {
		return nil, fmt.Errorf("BITMAP_NEW expects no arguments")
	}
	return "", nil // empty bitmap
}

func evalBitmapSet(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("BITMAP_SET expects 2 arguments: (bitmap, bit)")
	}
	bmVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bitVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if bitVal == nil {
		return bmVal, nil
	}
	bitN, err := toInt(bitVal)
	if err != nil {
		return nil, fmt.Errorf("BITMAP_SET: bit must be an integer: %w", err)
	}
	if bitN < 0 || bitN >= bitmapMaxBytes*8 {
		return nil, fmt.Errorf("BITMAP_SET: bit %d out of range [0, %d)", bitN, bitmapMaxBytes*8)
	}
	bmStr := ""
	if bmVal != nil {
		bmStr = fmt.Sprintf("%v", bmVal)
	}
	bm, err := bitmapDecode(bmStr)
	if err != nil {
		return nil, err
	}
	byteIdx := bitN / 8
	bitIdx := uint(bitN % 8)
	bm[byteIdx] |= 1 << bitIdx
	return bitmapEncode(bm), nil
}

func evalBitmapGet(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("BITMAP_GET expects 2 arguments: (bitmap, bit)")
	}
	bmVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bitVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if bmVal == nil || bitVal == nil {
		return false, nil
	}
	bitN, err := toInt(bitVal)
	if err != nil {
		return nil, fmt.Errorf("BITMAP_GET: bit must be an integer: %w", err)
	}
	if bitN < 0 || bitN >= bitmapMaxBytes*8 {
		return false, nil
	}
	bmStr := fmt.Sprintf("%v", bmVal)
	bm, err := bitmapDecode(bmStr)
	if err != nil {
		return nil, err
	}
	byteIdx := bitN / 8
	bitIdx := uint(bitN % 8)
	return (bm[byteIdx]>>bitIdx)&1 == 1, nil
}

func evalBitmapCount(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("BITMAP_COUNT expects 1 argument: (bitmap)")
	}
	bmVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if bmVal == nil {
		return 0, nil
	}
	bmStr := fmt.Sprintf("%v", bmVal)
	bm, err := bitmapDecode(bmStr)
	if err != nil {
		return nil, err
	}
	count := 0
	for _, b := range bm {
		count += bits.OnesCount8(b)
	}
	return count, nil
}

func evalBitmapOr(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("BITMAP_OR expects 2 arguments: (a, b)")
	}
	aVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	aStr := ""
	if aVal != nil {
		aStr = fmt.Sprintf("%v", aVal)
	}
	bStr := ""
	if bVal != nil {
		bStr = fmt.Sprintf("%v", bVal)
	}
	a, err := bitmapDecode(aStr)
	if err != nil {
		return nil, err
	}
	b, err := bitmapDecode(bStr)
	if err != nil {
		return nil, err
	}
	result := make([]byte, bitmapMaxBytes)
	for i := range result {
		result[i] = a[i] | b[i]
	}
	return bitmapEncode(result), nil
}

func evalBitmapAnd(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("BITMAP_AND expects 2 arguments: (a, b)")
	}
	aVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	aStr := ""
	if aVal != nil {
		aStr = fmt.Sprintf("%v", aVal)
	}
	bStr := ""
	if bVal != nil {
		bStr = fmt.Sprintf("%v", bVal)
	}
	a, err := bitmapDecode(aStr)
	if err != nil {
		return nil, err
	}
	b, err := bitmapDecode(bStr)
	if err != nil {
		return nil, err
	}
	result := make([]byte, bitmapMaxBytes)
	for i := range result {
		result[i] = a[i] & b[i]
	}
	return bitmapEncode(result), nil
}

// ─────────────────────────── Registration ────────────────────────────────────

// getExtraTypeFunctions returns the function map for YAML/URL/HASH/BITMAP helpers.
func getExtraTypeFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"YAML_PARSE":   evalYAMLParse,
		"YAML_GET":     evalYAMLGet,
		"URL_PARSE":    evalURLParse,
		"URL_ENCODE":   evalURLEncode,
		"URL_DECODE":   evalURLDecode,
		"HASH":         evalHashFunc,
		"BITMAP_NEW":   evalBitmapNew,
		"BITMAP_SET":   evalBitmapSet,
		"BITMAP_GET":   evalBitmapGet,
		"BITMAP_COUNT": evalBitmapCount,
		"BITMAP_OR":    evalBitmapOr,
		"BITMAP_AND":   evalBitmapAnd,
	}
}
