package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Measure the first context lookup after a small ingestion batch. Resetting
// the prior cache entry outside the timer keeps the corpus size fixed and
// excludes ingestion itself from the serving-path measurement.
func BenchmarkRAGContextIndexAfterAppend(b *testing.B) {
	for _, count := range []int{1200, 12000} {
		b.Run(fmt.Sprintf("rows=%d", count), func(b *testing.B) {
			table := storage.NewTable("append_context", []storage.Column{
				{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType},
			}, false)
			for i := range count {
				table.Rows = append(table.Rows, []any{fmt.Sprintf("doc-%d", i/12), i % 12})
			}
			table.Version++
			base := ragSourceFromTable("default", table)
			getRAGContextIndex(base, "doc", "chunk")
			key := ragContextCacheKey(base, "doc", "chunk")
			ragContextIndexCacheMu.RLock()
			entry := ragContextIndexCache[key]
			ragContextIndexCacheMu.RUnlock()
			b.Cleanup(func() { purgeRAGContextCachesFor("default", table.Name) })
			for i := range 12 {
				table.Rows = append(table.Rows, []any{"doc-0", 12 + i})
			}
			table.Version++
			source := ragSourceFromTable("default", table)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ragContextIndexCacheMu.Lock()
				ragContextIndexCache[key] = entry
				ragContextIndexCacheMu.Unlock()
				b.StartTimer()
				index := getRAGContextIndex(source, "doc", "chunk")
				if len(index.find("doc-0", 12, 0, 11)) != 12 {
					b.Fatal("appended neighbors missing")
				}
			}
		})
	}
}
