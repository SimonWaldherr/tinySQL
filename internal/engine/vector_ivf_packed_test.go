package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func legacyIVFValidRows(cache vecSearchColumnCacheEntry, dims int) []int {
	rows := make([]int, 0, cache.rowCount())
	for row := 0; row < cache.rowCount(); row++ {
		if cache.validAt(row) && len(cache.vector(row)) == dims {
			rows = append(rows, row)
		}
	}
	return rows
}

// Keep the former training and append-grown list construction as an
// independent reference for centroid bits, membership and row order.
func legacyIVFBuild(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecIVFIndex, error) {
	rows := legacyIVFValidRows(cache, dims)
	nlist := chooseIVFListCount(len(rows))
	idx := &vecIVFIndex{table: table, version: table.Version, metric: metric, dims: dims}
	if len(rows) == 0 || nlist == 0 {
		return idx, nil
	}
	idx.centroids = make([][]float64, nlist)
	for c := range idx.centroids {
		idx.centroids[c] = append([]float64(nil), cache.vector(rows[c*len(rows)/nlist])...)
	}
	sums, counts := make([]float64, nlist*dims), make([]int, nlist)
	needNorm := metricNeedsNorms(metric)
	for range vecIVFKMeansIters {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		idx.centroidNorms = centroidNormsFor(metric, idx.centroids)
		clear(sums)
		clear(counts)
		for i, row := range rows {
			if i&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			vector, norm, _ := cache.resolveRow(row, needNorm)
			c := nearestCentroid(metric, vector, norm, idx.centroids, idx.centroidNorms)
			counts[c]++
			search.VectorAccumulate(sums[c*dims:(c+1)*dims], vector)
		}
		for c := range idx.centroids {
			if counts[c] == 0 {
				continue
			}
			inv := 1 / float64(counts[c])
			for d := range idx.centroids[c] {
				idx.centroids[c][d] = sums[c*dims+d] * inv
			}
		}
	}
	idx.centroidNorms = centroidNorms(idx.centroids)
	idx.lists = make([][]int, nlist)
	for i, row := range rows {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		vector, norm, _ := cache.resolveRow(row, needNorm)
		c := nearestCentroid(metric, vector, norm, idx.centroids, idx.centroidNorms)
		idx.lists[c] = append(idx.lists[c], row)
	}
	return idx, nil
}

func ivfPackedFixture(rows, valid, dims int) (*storage.Table, vecSearchColumnCacheEntry) {
	table := storage.NewTable("ivf_packed", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, rows)
	for i := range valid {
		vector := make([]float64, dims)
		for d := range vector {
			vector[d] = math.Sin(float64(i/2+1)*0.17 + float64(d)*0.2)
		}
		table.Rows[i*(rows/valid)] = []any{vector}
	}
	return table, buildVecColumnCache(table, 0, true)
}

func TestIVFValidRowsMatchesLegacyLayouts(t *testing.T) {
	_, full := ivfPackedFixture(257, 64, 3)
	for _, parts := range []int{1, 8} {
		cache := full
		cache.segments = nil
		for part := range parts {
			from, to := part*cache.rows/parts, (part+1)*cache.rows/parts
			segment := full.segments[0]
			cache.segments = append(cache.segments, vecColumnSegment{
				start: from, vectors: segment.vectors[from:to], valid: segment.valid[from:to], norms: segment.norms[from:to],
			})
		}
		for _, layout := range []string{"direct", "overrides", "gap", "overlap", "missing", "extra-validity", "missing-norms"} {
			t.Run(fmt.Sprintf("segments%d/%s", parts, layout), func(t *testing.T) {
				view := cache
				view.segments = append([]vecColumnSegment(nil), cache.segments...)
				switch layout {
				case "overrides":
					view.overrides = map[int]vecColumnOverride{0: {}, 1: {vector: []float64{1, 2, 3}, valid: true}}
				case "gap":
					view.segments = view.segments[1:]
				case "overlap":
					view.segments = append(view.segments, vecColumnSegment{start: 250, vectors: [][]float64{{1, 2, 3}}, valid: []bool{true}})
				case "missing":
					view.segments = nil
				case "extra-validity":
					view.segments[0].valid = append(append([]bool(nil), view.segments[0].valid...), true)
				case "missing-norms":
					view.segments[0].norms = nil
				}
				for _, dims := range []int{0, 2, 3, 4} {
					want := legacyIVFValidRows(view, dims)
					got := validVectorRows(view, dims)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("dims%d: rows=%v, want %v", dims, got, want)
					}
					if view.directScanSegments(0, view.rows) && cap(got) != len(got) {
						t.Fatalf("direct collection retained capacity %d for %d valid rows", cap(got), len(got))
					}
				}
			})
		}
	}
}

func TestIVFPackedBuildMatchesLegacy(t *testing.T) {
	for _, valid := range []int{0, 1, 64, 512} {
		for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
			t.Run(fmt.Sprintf("valid%d/%s", valid, metric), func(t *testing.T) {
				table, cache := ivfPackedFixture(512, valid, 8)
				if valid > 32 {
					// Keep mixed dimensions, zero vectors and nonfinite values in
					// the source just as an embedding migration can produce them.
					table.Rows[0] = []any{[]float64{1}}
					table.Rows[8] = []any{make([]float64, 8)}
					table.Rows[16] = []any{[]float64{math.NaN(), 1, 2, 3, 4, 5, 6, 7}}
					table.Rows[24] = []any{[]float64{math.Inf(1), 1, 2, 3, 4, 5, 6, 7}}
					cache = buildVecColumnCache(table, 0, true)
				}
				got, err := buildVecIVFIndex(t.Context(), table, metric, 8, cache)
				if err != nil {
					t.Fatal(err)
				}
				want, err := legacyIVFBuild(t.Context(), table, metric, 8, cache)
				if err != nil || !reflect.DeepEqual(got.lists, want.lists) || len(got.centroids) != len(want.centroids) {
					t.Fatal("partition membership/order changed", err)
				}
				for c := range got.centroids {
					for d, value := range got.centroids[c] {
						if math.Float64bits(value) != math.Float64bits(want.centroids[c][d]) {
							t.Fatalf("centroid %d dimension %d changed bits", c, d)
						}
					}
					if math.Float64bits(got.centroidNorms[c]) != math.Float64bits(want.centroidNorms[c]) || cap(got.lists[c]) != len(got.lists[c]) {
						t.Fatal("centroid norm changed or inverted list retains growth slack")
					}
				}
				for _, query := range [][]float64{make([]float64, 8), {1, 2, 3, 4, 5, 6, 7, 8}, {math.NaN(), 0, 0, 0, 0, 0, 0, 0}} {
					for _, k := range []int{1, 20, 1000} {
						rows, err := got.search(t.Context(), query, vectorL2Norm(query), k, cache)
						reference, refErr := want.search(t.Context(), query, vectorL2Norm(query), k, cache)
						if fmt.Sprint(err) != fmt.Sprint(refErr) || !reflect.DeepEqual(rows, reference) {
							t.Fatalf("k%d query%v: packed search differs", k, query)
						}
					}
				}
				before := slices.Clone(got.lists)
				for c := range got.lists {
					before[c] = slices.Clone(got.lists[c])
				}
				for c := range got.lists {
					_ = append(got.lists[c], -1)
				}
				if !reflect.DeepEqual(before, got.lists) {
					t.Fatal("appending one list overwrote another list's backing storage")
				}
			})
		}
	}
}

func TestIVFPackedBuildCancellation(t *testing.T) {
	table, cache := ivfPackedFixture(128, 128, 3)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if idx, err := buildVecIVFIndex(ctx, table, "cosine", 3, cache); idx != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build returned %v, %v", idx, err)
	}
}
