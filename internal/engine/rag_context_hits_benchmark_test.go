package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A RAG_CONTEXT_FROM hit can carry text, embeddings and diagnostic columns,
// although expansion only needs its document, chunk and retrieval rank.
func BenchmarkRAGContextFromHitColumns(b *testing.B) {
	for _, kind := range []string{"table", "cte"} {
		for _, hitCount := range []int{6, 64} {
			b.Run(fmt.Sprintf("%s/hits=%d", kind, hitCount), func(b *testing.B) {
				db := storage.NewDB()
				b.Cleanup(func() { _ = db.Close() })
				chunks := storage.NewTable("context_chunks", []storage.Column{
					{Name: "doc", Type: storage.TextType},
					{Name: "chunk", Type: storage.IntType},
					{Name: "body", Type: storage.TextType},
				}, false)
				for i := 0; i < 128; i++ {
					chunks.Rows = append(chunks.Rows, []any{fmt.Sprintf("doc-%d", i/8), i % 8, "neighbor text"})
				}
				hitCols := []storage.Column{{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType}, {Name: "_rrf_rank", Type: storage.IntType}}
				for i := 0; i < 24; i++ {
					hitCols = append(hitCols, storage.Column{Name: fmt.Sprintf("metadata_%d", i), Type: storage.TextType})
				}
				hits := storage.NewTable("context_hits", hitCols, false)
				for i := 0; i < hitCount; i++ {
					raw := []any{fmt.Sprintf("doc-%d", i%16), 3, hitCount - i}
					for range 24 {
						raw = append(raw, "retrieval metadata")
					}
					hits.Rows = append(hits.Rows, raw)
				}
				for _, table := range []*storage.Table{chunks, hits} {
					if err := db.Put("default", table); err != nil {
						b.Fatal(err)
					}
				}
				env := ExecEnv{ctx: b.Context(), db: db, tenant: "default"}
				if kind == "cte" {
					source := ragSourceFromTable("default", hits)
					rows := make([]Row, source.len())
					for i := range rows {
						rows[i] = source.outputRow(i)
					}
					env.ctes = map[string]*ResultSet{"context_hits": {Cols: source.cols, Rows: rows}}
				}
				args := []Expr{&Literal{Val: "context_chunks"}, &Literal{Val: "doc"}, &Literal{Val: "chunk"}, &Literal{Val: "context_hits"}, &Literal{Val: "doc"}, &Literal{Val: "chunk"}, &Literal{Val: 1}, &Literal{Val: 1}}
				function := &RAGContextFromTableFunc{}
				if _, err := function.Execute(b.Context(), args, env, nil); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := function.Execute(b.Context(), args, env, nil)
					if err != nil || len(result.Rows) != min(hitCount, 16)*3 {
						b.Fatalf("context rows = %v, error = %v", result, err)
					}
				}
			})
		}
	}
}
