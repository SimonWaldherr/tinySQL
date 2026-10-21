package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkTrimEdges(b *testing.B) {
	for _, tc := range []struct{ name, text string }{
		{"short_ascii", "  sample text  "},
		{"long_ascii", "  " + strings.Repeat("sample text ", 3000) + "  "},
		{"long_unicode", "\u2003 " + strings.Repeat("漢🙂text ", 3000) + " \u00a0"},
		{"untrimmed", strings.Repeat("sample", 5000)},
	} {
		for _, name := range []string{"LTRIM", "RTRIM"} {
			b.Run(tc.name+"/"+name, func(b *testing.B) {
				call := &FuncCall{Name: name, Args: []Expr{&Literal{Val: tc.text}}}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := evalFuncCall(ExecEnv{}, call, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTrimFilterScan(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("trim_rows", []storage.Column{{Name: "note", Type: storage.TextType}}, false)
	text := "  " + strings.Repeat("sample text ", 3000) + "  "
	table.Rows = make([][]any, 10000)
	for i := range table.Rows {
		table.Rows[i] = []any{text}
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"LTRIM", "RTRIM"} {
		b.Run(name, func(b *testing.B) {
			runBench(b, db, "SELECT COUNT(*) FROM trim_rows WHERE "+name+"(note) = 'missing'")
		})
	}
}
