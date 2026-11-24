package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Measure the first lookup after the importer touches a bookkeeping column on
// 3% of the rows. Restore the old cache entry outside the timed section.
func BenchmarkRAGContextIndexAfterMetadataUpdate(b *testing.B) {
	for _, rows := range []int{12_000, 120_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			table := storage.NewTable("updated_context", []storage.Column{
				{Name: "doc", Type: storage.TextType},
				{Name: "chunk", Type: storage.IntType},
				{Name: "loaded_at", Type: storage.IntType},
			}, false)
			table.Rows = make([][]any, rows)
			for i := range table.Rows {
				table.Rows[i] = []any{fmt.Sprintf("doc-%d", i/12), i % 12, 0}
			}
			source := ragSourceFromTable("default", table)
			key := ragContextCacheKey(source, "doc", "chunk")
			getRAGContextIndex(source, "doc", "chunk")
			ragContextIndexCacheMu.RLock()
			oldEntry := ragContextIndexCache[key]
			ragContextIndexCacheMu.RUnlock()
			b.Cleanup(func() { purgeRAGContextCachesFor("default", table.Name) })

			for row := range rows * 3 / 100 {
				table.Rows[row][2] = 1
				table.MarkRowUpdatedCols(row, storage.ColumnBit(2))
			}
			table.Version++
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ragContextIndexCacheMu.Lock()
				ragContextIndexCache[key] = oldEntry
				ragContextIndexCacheMu.Unlock()
				b.StartTimer()
				index := getRAGContextIndex(source, "doc", "chunk")
				if len(index.find("doc-0", 1, 1, 1)) != 3 {
					b.Fatal("expected three neighboring chunks")
				}
			}
		})
	}
}
