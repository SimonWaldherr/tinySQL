package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func vectorHeapTestRows(n int) []vecScoredRow {
	rng := rand.New(rand.NewSource(42))
	rows := make([]vecScoredRow, n)
	for i := range rows {
		rows[i] = vecScoredRow{rowIdx: i, distance: float64(rng.Intn(17) - 8)}
	}
	if n >= 4 {
		rows[0].distance = math.Inf(-1)
		rows[1].distance = math.Inf(1)
		rows[2].distance = math.Copysign(0, -1)
		rows[3].distance = 0
	}
	rng.Shuffle(n, func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	return rows
}

func TestIVFCentroidFrontierPreservesProbeOrderAndFallback(t *testing.T) {
	table := storage.NewTable("frontier", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	for _, x := range []float64{100, 0, 2, 1} {
		table.Rows = append(table.Rows, []any{[]float64{x}})
	}
	cache := buildVecColumnCache(table, 0, false)
	idx := &vecIVFIndex{
		metric:        "l2",
		centroids:     [][]float64{{math.NaN()}, {0}, {2}, {-2}, {4}, {math.Inf(1)}},
		centroidNorms: make([]float64, 6),
		lists:         [][]int{nil, nil, {0}, {1}, {2}, {3}},
	}
	for _, tc := range []struct {
		k    int
		want []int
	}{
		// Two probes: the empty nearest list and the lower-ID tied centroid.
		{1, []int{0}},
		// Continue beyond the budget only while fewer than k rows were found.
		{2, []int{1, 0}},
		{3, []int{1, 2, 0}},
		{4, []int{1, 3, 2, 0}},
		{99, []int{1, 3, 2, 0}},
	} {
		got, err := idx.search(context.Background(), []float64{0}, 0, tc.k, cache)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int
		for _, row := range got {
			ids = append(ids, row.rowIdx)
		}
		if !reflect.DeepEqual(ids, tc.want) {
			t.Errorf("k=%d: row IDs=%v, want %v", tc.k, ids, tc.want)
		}
	}
}

func TestVectorHeapsMatchSortedCandidates(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 8, 31, 256, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			input := vectorHeapTestRows(n)
			want := append([]vecScoredRow(nil), input...)
			sort.Slice(want, func(i, j int) bool {
				if want[i].distance == want[j].distance {
					return want[i].rowIdx < want[j].rowIdx
				}
				return want[i].distance < want[j].distance
			})
			frontier := append(vecMinScoredHeap(nil), input...)
			for i := len(frontier)/2 - 1; i >= 0; i-- {
				vecMinHeapDown(frontier, i)
			}
			for i, expected := range want {
				if got := vecMinHeapPop(&frontier); got != expected {
					t.Fatalf("frontier[%d]=%v, want %v", i, got, expected)
				}
			}
			for _, k := range []int{0, 1, 2, 20, n, n + 10} {
				h := newScoredHeap(k, n)
				for _, row := range input {
					pushTopK(h, row.rowIdx, row.distance, k)
				}
				pushTopK(h, -1, math.NaN(), k)
				got := topKFromHeap(h, k)
				expected := want[:min(k, n)]
				if len(got) != len(expected) || (len(got) > 0 && !reflect.DeepEqual(got, expected)) {
					t.Fatalf("k=%d: top-k=%v, want %v", k, got, expected)
				}
			}
		})
	}
}

func TestNearestCentroidMatchesGenericRanking(t *testing.T) {
	centroids := [][]float64{
		{math.NaN(), 0}, {1, 0}, {-1, 0}, {0, 0}, {1},
		{math.Inf(1), 0}, {math.MaxFloat64, math.MaxFloat64},
		{math.SmallestNonzeroFloat64, 0},
	}
	norms := centroidNorms(centroids)
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot", "invalid"} {
		for _, query := range [][]float64{{1, 1}, {0, 0}, {1e300, -1e300}, {1e-300, 0}} {
			norm := vectorL2Norm(query)
			want, best := 0, math.MaxFloat64
			for i, centroid := range centroids {
				distance, ok := search.VectorRankingDistance(metric, centroid, query, norms[i], norm)
				if ok && distance < best {
					want, best = i, distance
				}
			}
			if got := nearestCentroid(metric, query, norm, centroids, norms); got != want {
				t.Errorf("metric=%s query=%v: centroid=%d, want %d", metric, query, got, want)
			}
		}
	}
}

var vectorIndexBenchmarkSink *vecIVFIndex

func BenchmarkIVFBuild(b *testing.B) {
	for _, dims := range []int{8, 64} {
		db := makeVecSearchBenchmarkTable(4096, dims)
		table, err := db.Get("default", "vec_docs")
		if err != nil {
			b.Fatal(err)
		}
		cache := buildVecColumnCache(table, 1, true)
		for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
			b.Run(fmt.Sprintf("dims%d/%s", dims, metric), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					idx, err := buildVecIVFIndex(context.Background(), table, metric, dims, cache)
					if err != nil {
						b.Fatal(err)
					}
					vectorIndexBenchmarkSink = idx
				}
			})
		}
	}
}
