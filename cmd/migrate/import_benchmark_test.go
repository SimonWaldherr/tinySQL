package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func benchmarkMigrateCSVData(n int) []byte {
	var b strings.Builder
	b.WriteString("id,name,active,score\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "%d,user_%04d,%t,%.1f\n", i, i, i%2 == 0, float64(i%100)+0.5)
	}
	return []byte(b.String())
}

func benchmarkMigrateYAMLData(n int) []byte {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "- id: %d\n  name: user_%04d\n  active: %t\n  score: %.1f\n",
			i, i, i%2 == 0, float64(i%100)+0.5)
	}
	return []byte(b.String())
}

func benchmarkMigrateXMLData(n int) []byte {
	var b strings.Builder
	b.WriteString("<root>")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, `<record id="%d" name="user_%04d" active="%t" score="%.1f"/>`,
			i, i, i%2 == 0, float64(i%100)+0.5)
	}
	b.WriteString("</root>")
	return []byte(b.String())
}

func BenchmarkMigrateImportFileToTinySQL(b *testing.B) {
	dir := b.TempDir()
	files := []struct {
		name string
		data []byte
	}{
		{name: "csv", data: benchmarkMigrateCSVData(500)},
		{name: "yaml", data: benchmarkMigrateYAMLData(500)},
		{name: "xml", data: benchmarkMigrateXMLData(500)},
	}

	for _, file := range files {
		path := filepath.Join(dir, "data."+file.name)
		if err := os.WriteFile(path, file.data, 0600); err != nil {
			b.Fatalf("write %s: %v", file.name, err)
		}

		b.Run(file.name, func(b *testing.B) {
			b.SetBytes(int64(len(file.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				db := tinysql.NewDB()
				if err := importFileToTinySQL(db, context.Background(), "default", path, "bench_data", true, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
