package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

// benchTable creates a Table with nRows rows of (id INT, name STRING, score FLOAT64).
func benchTable(name string, nRows int) *Table {
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
		{Name: "score", Type: Float64Type},
	}
	t := NewTable(name, cols, false)
	t.Rows = make([][]any, nRows)
	for i := range t.Rows {
		t.Rows[i] = []any{i, fmt.Sprintf("user_%d", i), float64(i) * 1.1}
	}
	t.Version = 1
	return t
}

// tempDir creates a temporary directory that is removed after the benchmark.
func tempDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "bench_storage_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ───────────────────────────────────────────────────────────────────────────
// Backend factory
// ───────────────────────────────────────────────────────────────────────────

type backendFactory struct {
	name    string
	newFunc func(b *testing.B) StorageBackend
}

func allBackends(b *testing.B) []backendFactory {
	b.Helper()
	return []backendFactory{
		{
			name: "Memory",
			newFunc: func(b *testing.B) StorageBackend {
				b.Helper()
				return NewMemoryBackend("")
			},
		},
		{
			name: "Disk",
			newFunc: func(b *testing.B) StorageBackend {
				b.Helper()
				dir := tempDir(b)
				be, err := NewDiskBackend(dir, false)
				if err != nil {
					b.Fatal(err)
				}
				return be
			},
		},
		{
			name: "DiskGzip",
			newFunc: func(b *testing.B) StorageBackend {
				b.Helper()
				dir := tempDir(b)
				be, err := NewDiskBackend(dir, true)
				if err != nil {
					b.Fatal(err)
				}
				return be
			},
		},
		{
			name: "Hybrid",
			newFunc: func(b *testing.B) StorageBackend {
				b.Helper()
				dir := tempDir(b)
				be, err := NewHybridBackend(dir, 256*1024*1024, false, ModeHybrid)
				if err != nil {
					b.Fatal(err)
				}
				return be
			},
		},
		{
			name: "Index",
			newFunc: func(b *testing.B) StorageBackend {
				b.Helper()
				dir := tempDir(b)
				be, err := NewHybridBackend(dir, 64*1024*1024, false, ModeIndex)
				if err != nil {
					b.Fatal(err)
				}
				return be
			},
		},
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: SaveTable
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkSaveTable(b *testing.B) {
	rowCounts := []int{10, 100, 1000, 10_000}

	for _, rc := range rowCounts {
		for _, factory := range allBackends(b) {
			name := fmt.Sprintf("%s/rows=%d", factory.name, rc)
			b.Run(name, func(b *testing.B) {
				be := factory.newFunc(b)
				defer be.Close()

				tbl := benchTable("bench", rc)
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					if err := be.SaveTable("default", tbl); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: LoadTable (cold – table only exists on backend)
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkLoadTable(b *testing.B) {
	rowCounts := []int{10, 100, 1000, 10_000}

	for _, rc := range rowCounts {
		for _, factory := range allBackends(b) {
			// MemoryBackend.LoadTable is a no-op (always nil) by design.
			if factory.name == "Memory" {
				continue
			}
			name := fmt.Sprintf("%s/rows=%d", factory.name, rc)
			b.Run(name, func(b *testing.B) {
				be := factory.newFunc(b)
				defer be.Close()

				tbl := benchTable("bench", rc)
				if err := be.SaveTable("default", tbl); err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					t, err := be.LoadTable("default", "bench")
					if err != nil {
						b.Fatal(err)
					}
					if t == nil {
						b.Fatal("LoadTable returned nil")
					}
				}
			})
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: SaveTable + LoadTable roundtrip
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkRoundTrip(b *testing.B) {
	rowCounts := []int{100, 1000}

	for _, rc := range rowCounts {
		for _, factory := range allBackends(b) {
			if factory.name == "Memory" {
				continue // MemoryBackend.LoadTable always returns nil
			}
			name := fmt.Sprintf("%s/rows=%d", factory.name, rc)
			b.Run(name, func(b *testing.B) {
				be := factory.newFunc(b)
				defer be.Close()

				tbl := benchTable("bench", rc)
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					if err := be.SaveTable("default", tbl); err != nil {
						b.Fatal(err)
					}
					t, err := be.LoadTable("default", "bench")
					if err != nil {
						b.Fatal(err)
					}
					if t == nil {
						b.Fatal("LoadTable returned nil after save")
					}
				}
			})
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: DeleteTable
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkDeleteTable(b *testing.B) {
	for _, factory := range allBackends(b) {
		b.Run(factory.name, func(b *testing.B) {
			be := factory.newFunc(b)
			defer be.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tbl := benchTable(fmt.Sprintf("t%d", i), 50)
				_ = be.SaveTable("default", tbl)
				if err := be.DeleteTable("default", tbl.Name); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: ListTableNames with many tables
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkListTableNames(b *testing.B) {
	tableCounts := []int{10, 100, 500}

	for _, tc := range tableCounts {
		for _, factory := range allBackends(b) {
			if factory.name == "Memory" {
				continue // MemoryBackend.ListTableNames always returns nil
			}
			name := fmt.Sprintf("%s/tables=%d", factory.name, tc)
			b.Run(name, func(b *testing.B) {
				be := factory.newFunc(b)
				defer be.Close()

				for i := 0; i < tc; i++ {
					tbl := benchTable(fmt.Sprintf("table_%d", i), 5)
					if err := be.SaveTable("default", tbl); err != nil {
						b.Fatal(err)
					}
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					names, err := be.ListTableNames("default")
					if err != nil {
						b.Fatal(err)
					}
					if len(names) != tc {
						b.Fatalf("expected %d tables, got %d", tc, len(names))
					}
				}
			})
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: TableExists
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkTableExists(b *testing.B) {
	for _, factory := range allBackends(b) {
		// MemoryBackend.TableExists always returns false by design.
		if factory.name != "Memory" {
			b.Run(factory.name+"/hit", func(b *testing.B) {
				be := factory.newFunc(b)
				defer be.Close()

				tbl := benchTable("target", 100)
				_ = be.SaveTable("default", tbl)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					if !be.TableExists("default", "target") {
						b.Fatal("expected table to exist")
					}
				}
			})
		}

		b.Run(factory.name+"/miss", func(b *testing.B) {
			be := factory.newFunc(b)
			defer be.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if be.TableExists("default", "nonexistent") {
					b.Fatal("expected table to not exist")
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: Sync
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkSync(b *testing.B) {
	for _, factory := range allBackends(b) {
		b.Run(factory.name, func(b *testing.B) {
			be := factory.newFunc(b)
			defer be.Close()

			// Populate some data so Sync has something to work with.
			for i := 0; i < 10; i++ {
				tbl := benchTable(fmt.Sprintf("t%d", i), 100)
				_ = be.SaveTable("default", tbl)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := be.Sync(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: Concurrent reads
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkConcurrentLoad(b *testing.B) {
	for _, factory := range allBackends(b) {
		if factory.name == "Memory" {
			continue // MemoryBackend.LoadTable always returns nil
		}
		b.Run(factory.name, func(b *testing.B) {
			be := factory.newFunc(b)
			defer be.Close()

			tbl := benchTable("shared", 1000)
			if err := be.SaveTable("default", tbl); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(4)

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					t, err := be.LoadTable("default", "shared")
					if err != nil {
						b.Fatal(err)
					}
					if t == nil {
						b.Fatal("nil table")
					}
				}
			})
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: End-to-end via OpenDB (integration-level)
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkOpenDB_SaveLoad(b *testing.B) {
	modes := []struct {
		name string
		mode StorageMode
	}{
		{"Memory", ModeMemory},
		{"Disk", ModeDisk},
		{"Hybrid", ModeHybrid},
		{"Index", ModeIndex},
	}

	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			dir := tempDir(b)
			path := filepath.Join(dir, "bench.db")

			cfg := DefaultStorageConfig(m.mode)
			cfg.Path = path

			db, err := OpenDB(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tblName := fmt.Sprintf("bench_%d", i)
				tbl := benchTable(tblName, 500)
				if err := db.Put("default", tbl); err != nil {
					b.Fatal(err)
				}
				got, err := db.Get("default", tblName)
				if err != nil {
					b.Fatal(err)
				}
				if got == nil {
					b.Fatal("nil table from DB")
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: PageBackend via pager sub-package
//
// PageBackend is tested separately because it does not implement
// StorageBackend (it uses pager.TableData instead of *storage.Table).
// This sub-benchmark is placed here for convenient side-by-side comparison
// but delegates to the pager package's own benchmark file.
// See internal/storage/pager/backend_benchmark_test.go for pager-specific
// benchmarks.
// ───────────────────────────────────────────────────────────────────────────
