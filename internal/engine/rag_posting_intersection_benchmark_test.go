package engine

import (
	"fmt"
	"testing"
)

func BenchmarkRAGFilteredPostingSelectivity(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, _ := db.Get("default", "rag_chunks")
	for _, stride := range []int{1, 4, 100, 10000} {
		b.Run(fmt.Sprintf("stride%d", stride), func(b *testing.B) {
			filter := &ragRowFilter{}
			for i := 0; i < len(table.Rows); i += stride {
				filter.rows = append(filter.rows, i)
			}
			for _, query := range []string{"term0", "term7 OR term23 OR term180 OR needle42"} {
				b.Run(query, func(b *testing.B) {
					if _, err := ragFTSSearchCandidatesFiltered(b.Context(), "default", table, query, 24, []int{5}, filter); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					for b.Loop() {
						if _, err := ragFTSSearchCandidatesFiltered(b.Context(), "default", table, query, 24, []int{5}, filter); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
