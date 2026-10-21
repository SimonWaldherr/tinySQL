package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var stringCountSink int

func BenchmarkStringCharCount(b *testing.B) {
	for _, tc := range []struct{ name, text string }{
		{"short_ascii", "sample text"},
		{"long_ascii", strings.Repeat("a", 16384)},
		{"unicode_tail", strings.Repeat("a", 16384) + "漢🙂"},
		{"unicode_head", "漢🙂" + strings.Repeat("a", 16384)},
		{"unicode", strings.Repeat("漢🙂", 2048)},
		{"invalid_tail", strings.Repeat("a", 16384) + "\xff\xc2"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.text)))
			b.ReportAllocs()
			for b.Loop() {
				stringCountSink = stringCharCount(tc.text)
			}
		})
	}
}

func BenchmarkLengthScan(b *testing.B) {
	for _, suffix := range []string{"ascii", "unicode_tail"} {
		b.Run(suffix, func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("length_rows", []storage.Column{{Name: "note", Type: storage.TextType}}, false)
			text := strings.Repeat("a", 16384)
			if suffix == "unicode_tail" {
				text += "漢🙂"
			}
			table.Rows = make([][]any, 10000)
			for i := range table.Rows {
				table.Rows[i] = []any{text}
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			runBench(b, db, "SELECT COUNT(*) FROM length_rows WHERE LENGTH(note) > 100")
		})
	}
}
