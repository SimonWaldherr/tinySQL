package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WatermarkValue is a JSON-friendly, type-preserving representation of a
// watermark column's value. JSON numbers/strings alone can't tell a
// time.Time from a plain string, so we carry the concrete Go type
// alongside the textual form and reconstruct it on demand.
type WatermarkValue struct {
	Kind string `json:"kind"` // "time" | "int" | "float" | "string"
	Text string `json:"text"`
}

// newWatermarkValue builds a WatermarkValue from an arbitrary Go value,
// recording enough type information to reconstruct it later via Value().
func newWatermarkValue(v any) (WatermarkValue, error) {
	switch t := v.(type) {
	case time.Time:
		return WatermarkValue{Kind: "time", Text: t.Format(time.RFC3339Nano)}, nil
	case int:
		return WatermarkValue{Kind: "int", Text: strconv.FormatInt(int64(t), 10)}, nil
	case int8:
		return WatermarkValue{Kind: "int", Text: strconv.FormatInt(int64(t), 10)}, nil
	case int16:
		return WatermarkValue{Kind: "int", Text: strconv.FormatInt(int64(t), 10)}, nil
	case int32:
		return WatermarkValue{Kind: "int", Text: strconv.FormatInt(int64(t), 10)}, nil
	case int64:
		return WatermarkValue{Kind: "int", Text: strconv.FormatInt(t, 10)}, nil
	case uint:
		return WatermarkValue{Kind: "int", Text: strconv.FormatUint(uint64(t), 10)}, nil
	case uint8:
		return WatermarkValue{Kind: "int", Text: strconv.FormatUint(uint64(t), 10)}, nil
	case uint16:
		return WatermarkValue{Kind: "int", Text: strconv.FormatUint(uint64(t), 10)}, nil
	case uint32:
		return WatermarkValue{Kind: "int", Text: strconv.FormatUint(uint64(t), 10)}, nil
	case uint64:
		return WatermarkValue{Kind: "int", Text: strconv.FormatUint(t, 10)}, nil
	case float32:
		return WatermarkValue{Kind: "float", Text: strconv.FormatFloat(float64(t), 'g', -1, 64)}, nil
	case float64:
		return WatermarkValue{Kind: "float", Text: strconv.FormatFloat(t, 'g', -1, 64)}, nil
	case string:
		return WatermarkValue{Kind: "string", Text: t}, nil
	case fmt.Stringer:
		return WatermarkValue{Kind: "string", Text: t.String()}, nil
	case nil:
		return WatermarkValue{}, fmt.Errorf("cannot build watermark from nil value")
	default:
		return WatermarkValue{Kind: "string", Text: fmt.Sprintf("%v", t)}, nil
	}
}

// Value reconstructs the typed Go value represented by w, based on its Kind.
func (w WatermarkValue) Value() (any, error) {
	switch w.Kind {
	case "time":
		t, err := time.Parse(time.RFC3339Nano, w.Text)
		if err != nil {
			return nil, fmt.Errorf("parse watermark time %q: %w", w.Text, err)
		}
		return t, nil
	case "int":
		n, err := strconv.ParseInt(w.Text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse watermark int %q: %w", w.Text, err)
		}
		return n, nil
	case "float":
		f, err := strconv.ParseFloat(w.Text, 64)
		if err != nil {
			return nil, fmt.Errorf("parse watermark float %q: %w", w.Text, err)
		}
		return f, nil
	case "string", "":
		return w.Text, nil
	default:
		return w.Text, nil
	}
}

// TableSyncState is the persisted sync state for one (source, target, table)
// pipeline: the last-seen key set, an optional watermark for incremental
// pulls, and (only when no watermark column is configured) a per-row content
// hash used to detect updates/deletes without a watermark.
type TableSyncState struct {
	Keys      []string          `json:"keys"`
	Watermark *WatermarkValue   `json:"watermark,omitempty"`
	RowHashes map[string]string `json:"row_hashes,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// defaultStateFilePath returns a stable path under the user's cache
// directory for the sync state of one pipeline, derived from sourceID,
// targetID, table and keyCols so distinct pipelines never collide.
func defaultStateFilePath(sourceID, targetID, table string, keyCols []string) string {
	sortedKeys := append([]string(nil), keyCols...)
	sort.Strings(sortedKeys)

	h := sha256.New()
	h.Write([]byte(sourceID))
	h.Write([]byte{0})
	h.Write([]byte(targetID))
	h.Write([]byte{0})
	h.Write([]byte(table))
	h.Write([]byte{0})
	h.Write([]byte(strings.Join(sortedKeys, ",")))
	sum := hex.EncodeToString(h.Sum(nil))[:8]

	cacheDir, err := os.UserCacheDir()
	if err != nil || cacheDir == "" {
		cacheDir = os.TempDir()
	}
	return filepath.Join(cacheDir, "tinysql-migrate", sum+".json")
}

// loadSyncState reads the sync state at path. A missing file is not an
// error: it returns a zero-value state, representing "no prior sync".
func loadSyncState(path string) (*TableSyncState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &TableSyncState{}, nil
		}
		return nil, fmt.Errorf("read sync state %s: %w", path, err)
	}
	var state TableSyncState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse sync state %s: %w", path, err)
	}
	return &state, nil
}

// saveSyncState writes state to path atomically: it marshals to JSON,
// writes to a sibling ".tmp" file, then renames it over path so readers
// never observe a partially-written file.
func saveSyncState(path string, state *TableSyncState) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create sync state dir %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sync state: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write sync state tmp file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename sync state tmp file %s to %s: %w", tmpPath, path, err)
	}
	return nil
}

// canonicalKeyPart converts a single key-column value into a stable string
// form so the same logical value always canonicalizes the same way
// regardless of the driver-specific Go type it arrived as.
func canonicalKeyPart(v any) string {
	switch t := v.(type) {
	case nil:
		return "\x00NULL"
	case string:
		return t
	case []byte:
		return string(t)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 64)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprintf("%v", t)
	}
}

// keyPartSeparator joins canonicalized key parts into a single row key.
// 0x1f (unit separator) is chosen because it's not a character any
// canonicalized column value is expected to contain.
const keyPartSeparator = "\x1f"

// computeRowKey joins the canonicalized forms of keyVals into a single
// stable row-key string usable as a map key.
func computeRowKey(keyVals []any) string {
	parts := make([]string, len(keyVals))
	for i, v := range keyVals {
		parts[i] = canonicalKeyPart(v)
	}
	return strings.Join(parts, keyPartSeparator)
}

// rowContentHash computes a SHA-256 hex digest over the canonicalized
// values of an entire row, used to detect content changes when no
// watermark column is available to do it more cheaply.
func rowContentHash(row []any) string {
	h := sha256.New()
	for i, v := range row {
		if i > 0 {
			h.Write([]byte(keyPartSeparator))
		}
		h.Write([]byte(canonicalKeyPart(v)))
	}
	return hex.EncodeToString(h.Sum(nil))
}
