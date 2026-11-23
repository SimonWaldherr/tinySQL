package engine

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkIVFSparseHeapReservation(b *testing.B) {
	const rows, stride = 65536, 128
	table := storage.NewTable("ivf_sparse_heap", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i += stride {
		table.Rows[i] = []any{[]float64{1, float64(i/stride%11 - 5), float64(i/stride%7 - 3)}}
	}
	cache := buildVecColumnCache(table, 0, true)
	idx, err := buildVecIVFIndex(b.Context(), table, "cosine", 3, cache)
	if err != nil {
		b.Fatal(err)
	}
	query := []float64{1, 0, 0}
	for _, tc := range []struct {
		name  string
		query []float64
		k     int
		want  int
	}{
		{"Top20", query, 20, 20},
		{"All", query, rows, rows / stride},
		{"Unscorable", []float64{0, 0, 0}, rows, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			norm := vectorL2Norm(tc.query)
			b.ReportAllocs()
			for b.Loop() {
				got, err := idx.search(b.Context(), tc.query, norm, tc.k, cache)
				if err != nil || len(got) != tc.want {
					b.Fatalf("got %d candidates: %v", len(got), err)
				}
			}
		})
	}
}

func BenchmarkIVFSearchSegmented(b *testing.B) {
	const (
		count = 32768
		dims  = 64
		lists = 32
	)
	rows := make([][]any, count)
	idx := &vecIVFIndex{metric: "cosine", centroids: make([][]float64, lists), lists: make([][]int, lists)}
	for row := range rows {
		vec := make([]float64, dims)
		for d := range vec {
			vec[d] = math.Sin(float64(row)*0.3 + float64(d)*0.2)
		}
		rows[row] = []any{vec}
		list := row % lists
		idx.lists[list] = append(idx.lists[list], row)
		if row < lists {
			idx.centroids[list] = vec
		}
	}
	idx.centroidNorms = centroidNorms(idx.centroids)
	query := rows[197][0].([]float64)
	queryNorm := vectorL2Norm(query)
	for _, parts := range []int{1, 8} {
		b.Run(fmt.Sprintf("segments=%d", parts), func(b *testing.B) {
			cache := vecSearchColumnCacheEntry{rows: count, normsReady: true}
			for i := range parts {
				start, end := i*count/parts, (i+1)*count/parts
				cache.segments = append(cache.segments, buildVecColumnSegment(rows[start:end], 0, true, start))
			}
			b.ReportAllocs()
			for b.Loop() {
				got, err := idx.search(b.Context(), query, queryNorm, 20, cache)
				if err != nil || len(got) != 20 {
					b.Fatalf("got %d candidates: %v", len(got), err)
				}
			}
		})
	}
}

func BenchmarkIVFSearchScratch(b *testing.B) {
	for _, nlist := range []int{4, 32, 64, 65, 129} {
		for _, metric := range []string{"cosine", "l2"} {
			b.Run(fmt.Sprintf("lists%d/%s", nlist, metric), func(b *testing.B) {
				idx, cache := ivfSearchScratchFixture(nlist, metric)
				query := []float64{1, 2}
				queryNorm := vectorL2Norm(query)
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					got, err := idx.search(ctx, query, queryNorm, 10, cache)
					if err != nil {
						b.Fatal(err)
					}
					runtime.KeepAlive(got)
				}
			})
		}
	}
}
