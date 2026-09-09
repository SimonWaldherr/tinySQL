package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var cpuBenchmarkModes = []storage.StorageMode{storage.ModeMemory, storage.ModeDisk, storage.ModeHybrid, storage.ModeIndex, storage.ModeJSON, storage.ModePagedIndex}

func cpuModeFixture(tb testing.TB, mode storage.StorageMode) (*storage.DB, storage.StorageConfig) {
	tb.Helper()
	cfg := storage.DefaultStorageConfig(mode)
	cfg.Path = filepath.Join(tb.TempDir(), "cpu.db")
	db, err := storage.OpenDB(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("cpu_items", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "label", Type: storage.TextType}}, false)
	for i := 0; i < 128; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("item-%d", i)})
	}
	table.Version++
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db, cfg
}

// Run with -cpu=1,36. Sequential means query latency; parallel means aggregate
// throughput of independent readers. Warm caches and parsed SQL are intentional.
func BenchmarkStorageModeCPU(b *testing.B) {
	for _, mode := range cpuBenchmarkModes {
		b.Run(mode.String(), func(b *testing.B) {
			db, _ := cpuModeFixture(b, mode)
			stmt := mustParse("SELECT id, label FROM cpu_items WHERE id >= 120 LIMIT 4")
			check := func() error {
				rs, err := Execute(b.Context(), db, "default", stmt)
				if err != nil {
					return err
				}
				if len(rs.Rows) != 4 || valueText(rs.Rows[0]["id"]) != "120" {
					return fmt.Errorf("unexpected rows: %v", rs.Rows)
				}
				return nil
			}
			if err := check(); err != nil {
				b.Fatal(err)
			}
			b.Run("sequential", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := check(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := check(); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		})
	}
}

func TestStorageModesCPUReopen(t *testing.T) {
	for _, mode := range cpuBenchmarkModes {
		t.Run(mode.String(), func(t *testing.T) {
			db, cfg := cpuModeFixture(t, mode)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			rs := execSQL(t, reopened, "SELECT id, label FROM cpu_items WHERE id >= 120 ORDER BY id DESC LIMIT 4")
			if len(rs.Rows) != 4 || valueText(rs.Rows[0]["id"]) != "127" {
				t.Fatal(rs)
			}
		})
	}
}
