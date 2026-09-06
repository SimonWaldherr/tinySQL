package engine

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRAGContextExtremeWindows(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	for _, tc := range []struct {
		name                  string
		center, before, after int
		want                  []int
	}{
		{"upper overflow", maxInt, 1, 1, []int{maxInt - 1, maxInt}},
		{"lower overflow", minInt, 1, 1, []int{minInt, minInt + 1}},
		{"wide window", 0, maxInt, maxInt, []int{minInt + 1, 0, maxInt - 1, maxInt}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []Row{}
			raw := [][]any{}
			for _, n := range []int{maxInt, minInt, 0, maxInt - 1, minInt + 1} {
				rows = append(rows, Row{"doc": "a", "chunk": n})
				raw = append(raw, []any{"a", n})
			}
			for _, source := range []ragSource{
				{cols: []string{"doc", "chunk"}, rows: rows},
				{cols: []string{"doc", "chunk"}, rawRows: raw, columnIdx: map[string]int{"doc": 0, "chunk": 1}, tableSource: true},
			} {
				index := ragBuildContextIndex(source, "doc", "chunk")
				for _, matches := range [][]ragContextRow{
					index.find("a", tc.center, tc.before, tc.after),
					ragFindContextRows(source, "doc", "chunk", "a", tc.center, tc.before, tc.after),
				} {
					got := make([]int, len(matches))
					for i, match := range matches {
						got[i] = match.chunkIndex
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("got %v, want %v", got, tc.want)
					}
				}
			}
		})
	}
}

func TestRAGContextWideWindowSQL(t *testing.T) {
	db, ctx := ragCacheSetup(t)
	maxInt := int(^uint(0) >> 1)
	for _, query := range []string{
		fmt.Sprintf(`SELECT * FROM RAG_CONTEXT('chunks', 'doc_id', 'chunk_index', 'doc-1', 1, %d, %d)`, maxInt, maxInt),
		fmt.Sprintf(`WITH hits AS (SELECT * FROM chunks WHERE doc_id = 'doc-1' AND chunk_index = 1) SELECT * FROM RAG_CONTEXT_FROM('chunks', 'doc_id', 'chunk_index', 'hits', 'doc_id', 'chunk_index', %d, %d)`, maxInt, maxInt),
	} {
		rs := mustExec(t, db, ctx, query)
		if len(rs.Rows) != 3 {
			t.Fatalf("got %d rows, want 3", len(rs.Rows))
		}
		for i, row := range rs.Rows {
			if row["doc_id"] != "doc-1" || row["_context_offset"] != i-1 {
				t.Fatalf("unexpected context row %v", row)
			}
		}
	}
}

func BenchmarkRAGContextWideWindow(b *testing.B) {
	source := ragSource{cols: []string{"doc", "chunk"}, rows: []Row{
		{"doc": "a", "chunk": 0}, {"doc": "a", "chunk": 1}, {"doc": "a", "chunk": 2},
	}}
	for _, width := range []int{2, 1_000_000} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := ragFindContextRows(source, "doc", "chunk", "a", 1, width, width); len(got) != 3 {
					b.Fatal(len(got))
				}
			}
		})
	}
}
