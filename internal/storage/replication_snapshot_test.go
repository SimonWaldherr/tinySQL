package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDecodedTableOwnershipKeepsNormalization(t *testing.T) {
	for _, rows := range [][][]any{nil, {}, {nil, {}, {1, `{"key":2}`, []any{1.0, 2.0}, "discard"}}} {
		dt := diskTable{Name: "owned", Cols: []diskColumn{{Name: "id", Type: IntType}, {Name: "payload", Type: JsonType}, {Name: "embedding", Type: VectorType}}, Rows: rows}
		var encoded bytes.Buffer
		if err := gob.NewEncoder(&encoded).Encode(dt); err != nil {
			t.Fatal(err)
		}
		var decoded diskTable
		if err := gob.NewDecoder(&encoded).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		want := diskToTable(decoded)
		got := diskToTableOwned(decoded)
		if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Cols, want.Cols) {
			t.Fatalf("owned normalization: %#v, want %#v", got.Rows, want.Rows)
		}
		if len(got.Rows) > 2 {
			if &got.Rows[2][0] != &decoded.Rows[2][0] || cap(got.Rows[2]) != len(got.Rows[2]) {
				t.Fatal("decoded rows were copied or retained excess append capacity")
			}
			if got.Rows[2][3] != nil {
				t.Fatal("extra-schema value was retained")
			}
		}
	}
}

func replicationFixture(t testing.TB) *DB {
	t.Helper()
	db, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: filepath.Join(t.TempDir(), "primary")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Put("default", loadBenchmarkTable()); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestReplicationSnapshotRoundTripAndCompletion(t *testing.T) {
	db := replicationFixture(t)
	if err := db.Catalog().RegisterView("main", "streamed_view", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	typed := NewTable("typed", []Column{{Name: "blob", Type: BlobType}, {Name: "json", Type: JsonType}, {Name: "vector", Type: VectorType}}, false)
	typed.Rows = [][]any{{[]byte{1, 2, 3}, map[string]any{"value": 2.0}, []float64{1, 2, 3}}}
	if err := db.Put("other_tenant", typed); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	watermark, epoch, err := WriteReplicationSnapshot(t.Context(), db, db.AdvancedWAL(), &encoded)
	if err != nil || epoch == 0 {
		t.Fatal(watermark, epoch, err)
	}
	data := bytes.Clone(encoded.Bytes())
	loaded, err := LoadReplicationSnapshot(t.Context(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	legacyData, err := SaveToBytes(db)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := LoadFromBytes(legacyData)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	typedLoaded, err := loaded.Get("other_tenant", "typed")
	typedLegacy, legacyErr := legacy.Get("other_tenant", "typed")
	if err != nil || legacyErr != nil || !reflect.DeepEqual(typedLoaded.Rows, typedLegacy.Rows) || !reflect.DeepEqual(catalogToDisk(loaded.Catalog()), catalogToDisk(legacy.Catalog())) {
		t.Fatal("streamed types/catalog differ from legacy restore", err, legacyErr)
	}
	want, _ := db.Get("default", "load_rows")
	got, _ := loaded.Get("default", "load_rows")
	if !reflect.DeepEqual(got.Rows, want.Rows) || cap(got.Rows[0]) != len(got.Rows[0]) {
		t.Fatal("streamed rows differ")
	}
	got.Rows[0][0] = -1
	if want.Rows[0][0] != 0 {
		t.Fatal("restored rows alias the source")
	}
	for _, length := range []int{0, 1, len(data) / 2, len(data) - 1} {
		if partial, err := LoadReplicationSnapshot(t.Context(), bytes.NewReader(data[:length])); err == nil || partial != nil {
			t.Fatal("accepted an incomplete snapshot", length, partial, err)
		}
	}
	if partial, err := LoadReplicationSnapshot(t.Context(), io.MultiReader(bytes.NewReader(data), bytes.NewReader([]byte{1, 2, 3}))); err == nil || partial != nil {
		t.Fatal("accepted trailing data", partial, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := LoadReplicationSnapshot(ctx, bytes.NewReader(data)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled load", err)
	}
}

func TestReplicationSnapshotRejectsTruncatedLargeRowCount(t *testing.T) {
	var encoded bytes.Buffer
	enc := gob.NewEncoder(&encoded)
	if err := enc.Encode(replicationSnapshotHeader{Version: 1, Tables: 1}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(replicationTableHeader{Table: diskTable{Name: "truncated"}, Rows: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if partial, err := LoadReplicationSnapshot(t.Context(), &encoded); err == nil || partial != nil {
		t.Fatal("accepted missing rows", partial, err)
	}
}

func TestReplicationSnapshotReleasesLocksBeforeOutput(t *testing.T) {
	db := replicationFixture(t)
	mutable := NewTable("mutable", []Column{{Name: "blob", Type: BlobType, HasDefault: true, DefaultValue: []byte{7}}, {Name: "vector", Type: VectorType}}, false)
	mutable.Rows = [][]any{{[]byte{1}, []float64{2}}}
	if err := db.Put("default", mutable); err != nil {
		t.Fatal(err)
	}
	w := newBlockingCheckpointWriter()
	done := make(chan error, 1)
	go func() { _, _, err := WriteReplicationSnapshot(t.Context(), db, db.AdvancedWAL(), w); done <- err }()
	select {
	case <-w.started:
	case <-time.After(3 * time.Second):
		close(w.release)
		t.Fatal("snapshot output not reached")
	}
	mutated := make(chan struct{})
	go func() {
		db.LockContentForWrite()
		table, _ := db.Get("default", "load_rows")
		table.Rows[0][0] = -1
		mutable.Rows[0][0].([]byte)[0] = 9
		mutable.Rows[0][1].([]float64)[0] = 9
		mutable.Cols[0].DefaultValue.([]byte)[0] = 9
		db.UnlockContentForWrite()
		close(mutated)
	}()
	select {
	case <-mutated:
	case <-time.After(time.Second):
		close(w.release)
		<-done
		t.Fatal("network output held the database content lock")
	}
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadReplicationSnapshot(t.Context(), &w.buf)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	table, _ := loaded.Get("default", "load_rows")
	if table.Rows[0][0] != 0 {
		t.Fatal("post-capture mutation leaked into the snapshot")
	}
	image, err := loaded.Get("default", "mutable")
	if err != nil || image.Rows[0][0].([]byte)[0] != 1 || image.Rows[0][1].([]float64)[0] != 2 || image.Cols[0].DefaultValue.([]byte)[0] != 7 {
		t.Fatal("mutable values changed after capture", image, err)
	}
}

func BenchmarkStreamedReplicationSnapshot(b *testing.B) {
	db := replicationFixture(b)
	b.ReportAllocs()
	for b.Loop() {
		r, w := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, _, err := WriteReplicationSnapshot(b.Context(), db, db.AdvancedWAL(), w)
			w.CloseWithError(err)
			done <- err
		}()
		loaded, err := LoadReplicationSnapshot(b.Context(), r)
		r.CloseWithError(err)
		if writeErr := <-done; err != nil || writeErr != nil || loaded == nil {
			b.Fatal(err, writeErr)
		}
	}
}
