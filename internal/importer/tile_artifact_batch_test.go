//go:build !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Every callback borrows the same buffer. The importer must copy payloads
// queued across callbacks and persist cells before reusing its batch.
type reusedTileArtifactSource struct {
	sizes       []int
	duplicateAt int
	failAt      int
	scanErr     error
	afterTile   func(int)
}

func (s reusedTileArtifactSource) Info(context.Context) (TileArtifactInfo, error) {
	info := TileArtifactInfo{Name: "reused.tiles", TileCount: int64(len(s.sizes)), Metadata: map[string]string{"format": "pbf", "name": "reused"}}
	for _, size := range s.sizes {
		info.TileBytes += int64(size)
		info.MaxTileBytes = max(info.MaxTileBytes, int64(size))
	}
	return info, nil
}

func (s reusedTileArtifactSource) ScanTiles(_ context.Context, emit func(TileArtifactTile) error) error {
	var size int
	for _, n := range s.sizes {
		size = max(size, n)
	}
	buffer := make([]byte, size)
	for i, n := range s.sizes {
		if s.scanErr != nil && i == s.failAt {
			return s.scanErr
		}
		for j := range buffer[:n] {
			buffer[j] = byte(i % 251)
		}
		key := i
		if s.duplicateAt > 0 && i == s.duplicateAt {
			key = 0
		}
		if err := emit(TileArtifactTile{Z: 8, X: key / 256, Y: key % 256, Data: buffer[:n]}); err != nil {
			return err
		}
		// Includes the final partial batch: mutate immediately after callback.
		for j := range buffer {
			buffer[j] = 255
		}
		if s.afterTile != nil {
			s.afterTile(i)
		}
	}
	return nil
}

func TestTileArtifactBatchReusePreservesPayloads(t *testing.T) {
	for _, batchSize := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(batchSize), func(t *testing.T) {
			source := reusedTileArtifactSource{sizes: []int{0, 1, 300, 8193, 2}}
			target := filepath.Join(t.TempDir(), "dataset.tinysql")
			var published MBTilesProgress
			result, err := ImportTileArtifact(context.Background(), source, target, &MBTilesArtifactOptions{
				BatchSize: batchSize, MaxMemoryBytes: 16 << 20,
				Progress: func(p MBTilesProgress) {
					if p.Phase == "published" {
						published = p
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Manifest.Schema != MBTilesSchemaFlat || published.RowsRead != 5 || published.RowsWritten != 5 {
				t.Fatalf("schema/progress changed: %s, %+v", result.Manifest.Schema, published)
			}
			reader, err := OpenMBTilesReader(context.Background(), target, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			digest := sha256.New()
			for i, size := range source.sizes {
				want := bytes.Repeat([]byte{byte(i % 251)}, size)
				got, found, err := reader.LookupTile(context.Background(), 8, 0, i)
				if err != nil || !found || !bytes.Equal(got, want) {
					t.Fatalf("tile %d: found=%v bytes=%d, want %d, err=%v", i, found, len(got), size, err)
				}
				hashTile(digest, 8, 0, i, want)
			}
			if got := result.Manifest.IndexConfig["tile_digest_sha256"]; got != hex.EncodeToString(digest.Sum(nil)) {
				t.Fatalf("tile digest changed: %v", got)
			}
		})
	}
}

func TestTileArtifactBatchFailureDoesNotPublish(t *testing.T) {
	for _, mode := range []string{"source-error", "cancel", "duplicate-in-batch", "duplicate-after-batch"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := reusedTileArtifactSource{sizes: []int{8, 8, 8, 8, 8}}
			var wantErr error
			switch mode {
			case "source-error":
				wantErr = errors.New("source failed")
				source.scanErr, source.failAt = wantErr, 3
			case "cancel":
				wantErr = context.Canceled
				source.afterTile = func(i int) {
					if i == 1 {
						cancel()
					}
				}
			case "duplicate-in-batch":
				source.duplicateAt = 1
			case "duplicate-after-batch":
				source.duplicateAt = 3
			}
			parent := t.TempDir()
			target := filepath.Join(parent, "dataset.tinysql")
			if _, err := ImportTileArtifact(ctx, source, target, &MBTilesArtifactOptions{BatchSize: 2, MaxMemoryBytes: 16 << 20}); err == nil || (wantErr != nil && !errors.Is(err, wantErr)) {
				t.Fatalf("import error = %v, want %v", err, wantErr)
			}
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed import left files: %v, %v", entries, err)
			}
		})
	}
}

func BenchmarkImportTileArtifactBatch(b *testing.B) {
	sizes := make([]int, 1025)
	for i := range sizes {
		sizes[i] = 64
	}
	source := reusedTileArtifactSource{sizes: sizes}
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := filepath.Join(root, fmt.Sprint(i))
		if _, err := ImportTileArtifact(b.Context(), source, target, &MBTilesArtifactOptions{BatchSize: 128, MaxMemoryBytes: 16 << 20}); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(target); err != nil {
			b.Fatal(err)
		}
	}
}
