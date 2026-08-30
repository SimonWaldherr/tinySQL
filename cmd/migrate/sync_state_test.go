package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWatermarkValueRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   any
	}{
		{"time", time.Date(2026, 8, 7, 12, 34, 56, 789000000, time.UTC)},
		{"int", int64(42)},
		{"float", float64(3.14159)},
		{"string", "hello-watermark"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wv, err := newWatermarkValue(c.in)
			if err != nil {
				t.Fatalf("newWatermarkValue(%v): %v", c.in, err)
			}

			data, err := json.Marshal(wv)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			var decoded WatermarkValue
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}

			got, err := decoded.Value()
			if err != nil {
				t.Fatalf("decoded.Value(): %v", err)
			}

			switch want := c.in.(type) {
			case time.Time:
				gotTime, ok := got.(time.Time)
				if !ok {
					t.Fatalf("got type %T, want time.Time", got)
				}
				if !gotTime.Equal(want) {
					t.Fatalf("time round-trip mismatch: got %v, want %v", gotTime, want)
				}
			case int64:
				gotInt, ok := got.(int64)
				if !ok {
					t.Fatalf("got type %T, want int64", got)
				}
				if gotInt != want {
					t.Fatalf("int round-trip mismatch: got %v, want %v", gotInt, want)
				}
			case float64:
				gotFloat, ok := got.(float64)
				if !ok {
					t.Fatalf("got type %T, want float64", got)
				}
				if gotFloat != want {
					t.Fatalf("float round-trip mismatch: got %v, want %v", gotFloat, want)
				}
			case string:
				gotStr, ok := got.(string)
				if !ok {
					t.Fatalf("got type %T, want string", got)
				}
				if gotStr != want {
					t.Fatalf("string round-trip mismatch: got %q, want %q", gotStr, want)
				}
			default:
				t.Fatalf("unhandled case type %T", want)
			}
		})
	}
}

func TestLoadSyncStateMissingFileReturnsZeroState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.json")

	state, err := loadSyncState(path)
	if err != nil {
		t.Fatalf("loadSyncState on missing file returned error: %v", err)
	}
	if state == nil {
		t.Fatal("loadSyncState returned nil state")
	}
	if len(state.Keys) != 0 {
		t.Errorf("expected empty Keys, got %v", state.Keys)
	}
	if state.Watermark != nil {
		t.Errorf("expected nil Watermark, got %v", state.Watermark)
	}
	if len(state.RowHashes) != 0 {
		t.Errorf("expected empty RowHashes, got %v", state.RowHashes)
	}
	if !state.UpdatedAt.IsZero() {
		t.Errorf("expected zero UpdatedAt, got %v", state.UpdatedAt)
	}
}

func TestSaveAndLoadSyncStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "state.json")

	wm, err := newWatermarkValue(int64(1723027200))
	if err != nil {
		t.Fatalf("newWatermarkValue: %v", err)
	}

	want := &TableSyncState{
		Keys:      []string{"1", "2", "3"},
		Watermark: &wm,
		RowHashes: map[string]string{"1": "abc123", "2": "def456"},
		UpdatedAt: time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC),
	}

	if err := saveSyncState(path, want); err != nil {
		t.Fatalf("saveSyncState: %v", err)
	}

	got, err := loadSyncState(path)
	if err != nil {
		t.Fatalf("loadSyncState: %v", err)
	}

	if len(got.Keys) != len(want.Keys) {
		t.Fatalf("Keys length mismatch: got %v, want %v", got.Keys, want.Keys)
	}
	for i := range want.Keys {
		if got.Keys[i] != want.Keys[i] {
			t.Errorf("Keys[%d]: got %q, want %q", i, got.Keys[i], want.Keys[i])
		}
	}

	if got.Watermark == nil {
		t.Fatal("expected non-nil Watermark")
	}
	if got.Watermark.Kind != want.Watermark.Kind || got.Watermark.Text != want.Watermark.Text {
		t.Errorf("Watermark mismatch: got %+v, want %+v", got.Watermark, want.Watermark)
	}

	if len(got.RowHashes) != len(want.RowHashes) {
		t.Fatalf("RowHashes length mismatch: got %v, want %v", got.RowHashes, want.RowHashes)
	}
	for k, v := range want.RowHashes {
		if got.RowHashes[k] != v {
			t.Errorf("RowHashes[%q]: got %q, want %q", k, got.RowHashes[k], v)
		}
	}

	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}
}

func TestSaveSyncStateIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state := &TableSyncState{Keys: []string{"a"}, UpdatedAt: time.Now().UTC()}

	if err := saveSyncState(path, state); err != nil {
		t.Fatalf("saveSyncState: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected final path to exist after save: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected .tmp file to be gone after save, stat err = %v", err)
	}

	// Save again to make sure the tmp file doesn't linger on a second write either.
	state2 := &TableSyncState{Keys: []string{"a", "b"}, UpdatedAt: time.Now().UTC()}
	if err := saveSyncState(path, state2); err != nil {
		t.Fatalf("second saveSyncState: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected .tmp file to be gone after second save, stat err = %v", err)
	}

	got, err := loadSyncState(path)
	if err != nil {
		t.Fatalf("loadSyncState after second save: %v", err)
	}
	if len(got.Keys) != 2 {
		t.Fatalf("expected state from second save to win, got Keys=%v", got.Keys)
	}
}

func TestSaveSyncStateConcurrentWritersLeaveValidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	states := []*TableSyncState{
		{Keys: []string{"a"}, UpdatedAt: time.Now().UTC()},
		{Keys: []string{"b", "c"}, UpdatedAt: time.Now().UTC()},
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(states))
	for _, state := range states {
		wg.Add(1)
		go func(state *TableSyncState) {
			defer wg.Done()
			errs <- saveSyncState(path, state)
		}(state)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("saveSyncState: %v", err)
		}
	}

	got, err := loadSyncState(path)
	if err != nil {
		t.Fatalf("loadSyncState: %v", err)
	}
	if len(got.Keys) != 1 && len(got.Keys) != 2 {
		t.Fatalf("concurrent result is not either complete state: %+v", got)
	}
	matchesFirst := len(got.Keys) == 1 && got.Keys[0] == "a"
	matchesSecond := len(got.Keys) == 2 && got.Keys[0] == "b" && got.Keys[1] == "c"
	if !matchesFirst && !matchesSecond {
		t.Fatalf("concurrent result mixed states: %+v", got)
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, "state.json.tmp*"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("temporary files left behind: %v", leftovers)
	}
}

func TestLoadSyncStateRejectsTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"keys":[]} {"keys":["unexpected"]}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := loadSyncState(path); err == nil {
		t.Fatal("loadSyncState accepted a second JSON value")
	}
}

func TestDefaultStateFilePathIsStableAndDistinguishing(t *testing.T) {
	p1 := defaultStateFilePath("src1", "tgt1", "orders", []string{"id"})
	p2 := defaultStateFilePath("src1", "tgt1", "orders", []string{"id"})
	if p1 != p2 {
		t.Fatalf("defaultStateFilePath not stable: %q vs %q", p1, p2)
	}

	variants := [][4]any{
		{"src2", "tgt1", "orders", []string{"id"}},
		{"src1", "tgt2", "orders", []string{"id"}},
		{"src1", "tgt1", "customers", []string{"id"}},
		{"src1", "tgt1", "orders", []string{"id", "region"}},
	}
	seen := map[string]bool{p1: true}
	for _, v := range variants {
		p := defaultStateFilePath(v[0].(string), v[1].(string), v[2].(string), v[3].([]string))
		if seen[p] {
			t.Errorf("collision: variant %v produced already-seen path %q", v, p)
		}
		seen[p] = true
	}

	if filepath.Base(filepath.Dir(p1)) != "tinysql-migrate" {
		t.Errorf("expected parent dir 'tinysql-migrate', got %q", filepath.Dir(p1))
	}
	base := filepath.Base(p1)
	if filepath.Ext(base) != ".json" {
		t.Errorf("expected .json extension, got %q", base)
	}
	hexPart := base[:len(base)-len(".json")]
	if len(hexPart) != 8 {
		t.Errorf("expected 8-char hex prefix, got %q (len %d)", hexPart, len(hexPart))
	}
}

func TestComputeRowKeyAndRowContentHash(t *testing.T) {
	k1 := computeRowKey([]any{int64(1), "alice"})
	k2 := computeRowKey([]any{int64(1), "alice"})
	if k1 != k2 {
		t.Fatalf("computeRowKey not deterministic: %q vs %q", k1, k2)
	}

	k3 := computeRowKey([]any{int64(2), "alice"})
	if k1 == k3 {
		t.Fatalf("computeRowKey collided for different keys: %q", k1)
	}

	h1 := rowContentHash([]any{int64(1), "alice", 30})
	h2 := rowContentHash([]any{int64(1), "alice", 30})
	if h1 != h2 {
		t.Fatalf("rowContentHash not deterministic: %q vs %q", h1, h2)
	}
	h3 := rowContentHash([]any{int64(1), "alice", 31})
	if h1 == h3 {
		t.Fatalf("rowContentHash collided for different content: %q", h1)
	}
	if len(h1) != 64 {
		t.Errorf("expected 64-char hex sha256 digest, got %d chars: %q", len(h1), h1)
	}
}
