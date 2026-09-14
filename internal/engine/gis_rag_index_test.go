package engine

import (
	"context"
	"fmt"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

func TestRetrievalIntersectionDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(8721))
	for trial := 0; trial < 500; trial++ {
		var a, b []int32
		var allowed []int
		want := []int32{}
		for row := -10; row < 20000; row++ {
			inA, inB := rng.Intn(1+trial%137) == 0, rng.Intn(1+trial%3) == 0
			if inA {
				a = append(a, int32(row))
			}
			if inB {
				b = append(b, int32(row))
				allowed = append(allowed, row)
			}
			if inA && inB {
				want = append(want, int32(row))
			}
		}
		aBefore, bBefore := slices.Clone(a), slices.Clone(b)
		if got := ftsIntersect(a, b); !slices.Equal(got, want) {
			t.Fatalf("FTS trial %d", trial)
		}
		if got := ftsIntersect(b, a); !slices.Equal(got, want) {
			t.Fatalf("FTS reverse trial %d", trial)
		}
		if got := ragIntersectFTSCandidates(ftsCandidates{rows: a}, allowed); !slices.Equal(got, want) {
			t.Fatalf("RAG trial %d", trial)
		}
		if got := ragFilteredFTSDocFrequency(a, allowed); got != len(want) {
			t.Fatalf("frequency %d want %d", got, len(want))
		}
		reverse := make([]int, len(a))
		for i, v := range a {
			reverse[i] = int(v)
		}
		if got := ragIntersectFTSCandidates(ftsCandidates{rows: b}, reverse); !slices.Equal(got, want) {
			t.Fatalf("RAG reverse trial %d", trial)
		}
		if got := ragFilteredFTSDocFrequency(b, reverse); got != len(want) {
			t.Fatalf("reverse frequency %d want %d", got, len(want))
		}
		if !slices.Equal(a, aBefore) || !slices.Equal(b, bBefore) {
			t.Fatal("modified shared postings")
		}
	}
	for _, a := range [][]int32{nil, {0}, {1, 2, 3}} {
		if len(ftsIntersect(a, nil)) != 0 || ragFilteredFTSDocFrequency(a, nil) != 0 {
			t.Fatal("empty intersection")
		}
	}
}

func BenchmarkGISRAGIndex(b *testing.B) {
	ctx := context.Background()
	for _, polygons := range []bool{false, true} {
		table := geoSpeedTable(b, polygons)
		idx, err := getGeoGridIndex(ctx, "default", table, 1)
		if err != nil {
			b.Fatal(err)
		}
		for _, tc := range []struct {
			name string
			box  []float64
		}{
			{"local", []float64{4, 4, 5, 5}}, {"strip", []float64{4, 0, 4.01, 9.8}}, {"regional", []float64{2, 2, 18, 8}},
		} {
			b.Run(fmt.Sprintf("spatial/polygons=%t/%s", polygons, tc.name), func(b *testing.B) {
				opts := &ragSpatialFilterOptions{GeometryColumn: "geom", BBox: tc.box}
				b.ReportAllocs()
				for b.Loop() {
					rows, err := ragRowsForSpatialFilter(ctx, "default", table, opts)
					if err != nil || len(rows) == 0 {
						b.Fatalf("rows=%d err=%v", len(rows), err)
					}
				}
			})
		}
		b.Run(fmt.Sprintf("build/polygons=%t", polygons), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := buildGeoGridIndex(ctx, table, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = idx
	}
	for _, stride := range []int{1, 4, 100, 10000} {
		all := make([]int32, 20000)
		allowed := []int{}
		for i := range all {
			all[i] = int32(i)
			if i%stride == 0 {
				allowed = append(allowed, i)
			}
		}
		b.Run(fmt.Sprintf("intersection/stride%d", stride), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if rows := ragIntersectFTSCandidates(ftsCandidates{rows: all}, allowed); len(rows) != len(allowed) {
					b.Fatal("missing rows")
				}
			}
		})
		b.Run(fmt.Sprintf("frequency/stride%d", stride), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if n := ragFilteredFTSDocFrequency(all, allowed); n != len(allowed) {
					b.Fatal("wrong frequency")
				}
			}
		})
	}
}

func BenchmarkGISRAGConjunction(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	query := "term0 AND needle42"
	if hits, err := ftsSearchCandidates(b.Context(), "default", table, query, 24, []int{5}); err != nil || len(hits) != 3 {
		b.Fatalf("hits=%d err=%v", len(hits), err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if hits, err := ftsSearchCandidates(b.Context(), "default", table, query, 24, []int{5}); err != nil || len(hits) != 3 {
			b.Fatalf("hits=%d err=%v", len(hits), err)
		}
	}
}

func BenchmarkGISRAGFilteredPlan(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	query := "term0 AND term7"
	for _, stride := range []int{100, 10000} {
		filter := &ragRowFilter{}
		for i := 0; i < len(table.Rows); i += stride {
			filter.rows = append(filter.rows, i)
		}
		expected, err := ragFTSSearchCandidatesFiltered(b.Context(), "default", table, query, 24, []int{5}, filter)
		if err != nil || len(expected) == 0 {
			b.Fatal("bad fixture", err)
		}
		key := ragFilteredFTSQueryCacheKey{table: table, version: table.Version, cols: ftsColsCacheKey([]int{5}), query: query, filter: filter}
		for _, cold := range []bool{false, true} {
			b.Run(fmt.Sprintf("stride%d/planMiss=%t", stride, cold), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if cold {
						ragFilteredFTSQueryCacheMu.Lock()
						delete(ragFilteredFTSQueryCache, key)
						ragFilteredFTSQueryCacheMu.Unlock()
					}
					hits, err := ragFTSSearchCandidatesFiltered(b.Context(), "default", table, query, 24, []int{5}, filter)
					if err != nil || len(hits) != len(expected) {
						b.Fatal("wrong hits", err)
					}
				}
			})
		}
	}
}

func BenchmarkGISRAGGeoSQL(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Put("default", geoSpeedTable(b, false)); err != nil {
		b.Fatal(err)
	}
	runRAGBench(b, db, `SELECT id FROM GEO_SEARCH('geo_speed','geom','bbox',4,0,4.01,9.8)`, 99)
}

func TestGISOrderedGridDifferential(t *testing.T) {
	for _, polygons := range []bool{false, true} {
		idx, err := buildGeoGridIndex(t.Context(), geoSpeedTable(t, polygons), 1)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(7123))
		for trial := 0; trial < 250; trial++ {
			x1, x2, y1, y2 := rng.Float64()*25-2, rng.Float64()*25-2, rng.Float64()*15-2, rng.Float64()*15-2
			loX, hiX, loY, hiY := min(x1, x2), max(x1, x2), min(y1, y2), max(y1, y2)
			// Compare against the original cell semantics, including false positives.
			want := map[int32]bool{}
			if hiX >= idx.bounds.MinX && loX <= idx.bounds.MaxX && hiY >= idx.bounds.MinY && loY <= idx.bounds.MaxY {
				for _, row := range idx.overflow {
					want[row] = true
				}
				for cell, rows := range idx.cells {
					if float64(cell.X) < math.Floor(max(loX, idx.bounds.MinX)/idx.cellSizeLon) || float64(cell.X) > math.Floor(min(hiX, idx.bounds.MaxX)/idx.cellSizeLon) || float64(cell.Y) < math.Floor(max(loY, idx.bounds.MinY)/idx.cellSizeLat) || float64(cell.Y) > math.Floor(min(hiY, idx.bounds.MaxY)/idx.cellSizeLat) {
						continue
					}
					for _, row := range rows {
						want[row] = true
					}
				}
			}
			got := idx.candidatesBBox(x1, y1, x2, y2)
			if len(got) != len(want) {
				t.Fatalf("polygon=%v trial=%d got=%d want=%d", polygons, trial, len(got), len(want))
			}
			for _, row := range got {
				if !want[row] {
					t.Fatalf("unexpected or duplicate row %d", row)
				}
				delete(want, row)
			}
		}
	}
}

func TestSparseFilteredRankingDifferential(t *testing.T) {
	table := storage.NewTable("sparse_ranking", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 2000; i++ {
		body := "alpha beta gamma"
		if i%97 == 0 {
			body = "alpha alpha"
		}
		table.Rows = append(table.Rows, []any{body})
	}
	filter := &ragRowFilter{rows: []int{0, 97, 501, 1500, 1999}}
	for _, query := range []string{"alpha", "alpha AND beta", "alpha OR beta", `"alpha beta"`, "NOT beta", "al*", "absent"} {
		want := referenceAuthorizedFTS(t, table, filter, query, 10, []int{0})
		got, err := ragFTSSearchCandidatesFiltered(t.Context(), "default", table, query, 10, []int{0}, filter)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("query=%s got=%v want=%v err=%v", query, got, want, err)
		}
	}
}
