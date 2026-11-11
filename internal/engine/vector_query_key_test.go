package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecQueryKeyPreservesBitStream(t *testing.T) {
	table := storage.NewTable("key_vectors", nil, false)
	table.Version = 7
	values := []float64{0, math.Copysign(0, -1), 1, -1, math.SmallestNonzeroFloat64, math.MaxFloat64,
		math.Inf(-1), math.Inf(1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002)}
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 33, 64, 65, 768, 1536} {
		query := make([]float64, dims)
		bytes := make([]byte, dims*8)
		for i := range query {
			query[i] = values[i%len(values)]
			binary.LittleEndian.PutUint64(bytes[i*8:], math.Float64bits(query[i]))
		}
		want := vecQueryCacheKey{tenant: "tenant", table: table.Name, source: table, column: "Embedding",
			version: table.Version, metric: "cosine", index: "hnsw", k: 20, query: sha256.Sum256(bytes)}
		args := vecSearchArgs{queryVec: query, metric: "cosine", indexMode: "hnsw", k: 20}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				if got := vecQueryKey("tenant", table, "Embedding", args); got != want {
					t.Errorf("dims=%d: key differs: %v / %v", dims, got, want)
				}
			})
		}
		wg.Wait()
		for i, v := range query {
			if math.Float64bits(v) != math.Float64bits(values[i%len(values)]) {
				t.Fatal("hashing changed the source vector")
			}
		}
	}
}

var vecQueryKeySink vecQueryCacheKey

func BenchmarkVecQueryKeyBatches(b *testing.B) {
	table := storage.NewTable("key_vectors", nil, false)
	for _, dims := range []int{32, 768, 1536} {
		b.Run(fmt.Sprint(dims), func(b *testing.B) {
			query := make([]float64, dims)
			for i := range query {
				query[i] = float64(i) / 17
			}
			args := vecSearchArgs{queryVec: query, metric: "cosine", indexMode: "hnsw", k: 20}
			b.ReportAllocs()
			for b.Loop() {
				vecQueryKeySink = vecQueryKey("tenant", table, "embedding", args)
			}
		})
	}
}
