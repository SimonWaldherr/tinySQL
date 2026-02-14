package pager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

func benchTableData(name string, nRows int) *TableData {
	cols := []ColumnInfo{
		{Name: "id", Type: 0},     // IntType
		{Name: "name", Type: 13},  // StringType
		{Name: "score", Type: 11}, // Float64Type
	}
	rows := make([][]any, nRows)
	for i := range rows {
		rows[i] = []any{float64(i), fmt.Sprintf("user_%d", i), float64(i) * 1.1}
	}
	return &TableData{
		Name:    name,
		Columns: cols,
		Rows:    rows,
		Version: 1,
	}
}

func pagerTempDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "bench_pager_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newBenchPageBackend(b *testing.B) *PageBackend {
	b.Helper()
	dir := pagerTempDir(b)
	pb, err := NewPageBackend(PageBackendConfig{
		Path: filepath.Join(dir, "bench.db"),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { pb.Close() })
	return pb
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: SaveTable
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_SaveTable(b *testing.B) {
	rowCounts := []int{10, 100, 1000, 10_000}

	for _, rc := range rowCounts {
		b.Run(fmt.Sprintf("rows=%d", rc), func(b *testing.B) {
			pb := newBenchPageBackend(b)
			td := benchTableData("bench", rc)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := pb.SaveTable("default", td); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: LoadTable
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_LoadTable(b *testing.B) {
	rowCounts := []int{10, 100, 1000, 10_000}

	for _, rc := range rowCounts {
		b.Run(fmt.Sprintf("rows=%d", rc), func(b *testing.B) {
			pb := newBenchPageBackend(b)
			td := benchTableData("bench", rc)

			if err := pb.SaveTable("default", td); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				got, err := pb.LoadTable("default", "bench")
				if err != nil {
					b.Fatal(err)
				}
				if got == nil {
					b.Fatal("LoadTable returned nil")
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: RoundTrip (Save + Load)
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_RoundTrip(b *testing.B) {
	rowCounts := []int{100, 1000}

	for _, rc := range rowCounts {
		b.Run(fmt.Sprintf("rows=%d", rc), func(b *testing.B) {
			pb := newBenchPageBackend(b)
			td := benchTableData("bench", rc)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := pb.SaveTable("default", td); err != nil {
					b.Fatal(err)
				}
				got, err := pb.LoadTable("default", "bench")
				if err != nil {
					b.Fatal(err)
				}
				if got == nil {
					b.Fatal("LoadTable returned nil after save")
				}
			}
		})
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: DeleteTable
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_DeleteTable(b *testing.B) {
	pb := newBenchPageBackend(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		td := benchTableData(fmt.Sprintf("t%d", i), 50)
		_ = pb.SaveTable("default", td)
		if err := pb.DeleteTable("default", td.Name); err != nil {
			b.Fatal(err)
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: ListTableNames
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_ListTableNames(b *testing.B) {
	tableCounts := []int{10, 100}

	for _, tc := range tableCounts {
		b.Run(fmt.Sprintf("tables=%d", tc), func(b *testing.B) {
			pb := newBenchPageBackend(b)

			for i := 0; i < tc; i++ {
				td := benchTableData(fmt.Sprintf("table_%d", i), 5)
				if err := pb.SaveTable("default", td); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				names, err := pb.ListTableNames("default")
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

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: Concurrent loads
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_ConcurrentLoad(b *testing.B) {
	pb := newBenchPageBackend(b)

	td := benchTableData("shared", 1000)
	if err := pb.SaveTable("default", td); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(4)

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			got, err := pb.LoadTable("default", "shared")
			if err != nil {
				b.Fatal(err)
			}
			if got == nil {
				b.Fatal("nil table")
			}
		}
	})
}

// ───────────────────────────────────────────────────────────────────────────
// Benchmark: Sync (checkpoint)
// ───────────────────────────────────────────────────────────────────────────

func BenchmarkPageBackend_Sync(b *testing.B) {
	pb := newBenchPageBackend(b)

	for i := 0; i < 10; i++ {
		td := benchTableData(fmt.Sprintf("t%d", i), 100)
		_ = pb.SaveTable("default", td)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := pb.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
