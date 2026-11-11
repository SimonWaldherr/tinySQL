package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkTrimLength(b *testing.B) {
	for _, tc := range []struct{ name, text, cut string }{
		{"Changed", "xxα🙂xx", "x"},
		{"Unchanged", "α🙂", "x"},
		{"EmptyCutset", "xxα🙂xx", ""},
		{"LongResult", "xx" + strings.Repeat("a", 300) + "xx", "x"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("trim_lengths", []storage.Column{{Name: "body", Type: storage.TextType}, {Name: "cut", Type: storage.TextType}}, false)
			for range 50000 {
				table.Rows = append(table.Rows, []any{tc.text, tc.cut})
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			runBench(b, db, "SELECT SUM(LENGTH(TRIM(body,cut))) FROM trim_lengths")
		})
	}
}
