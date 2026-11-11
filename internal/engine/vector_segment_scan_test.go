package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func referenceVecSegmentScan(ctx context.Context, rows [][]any, start, end, dims, k int, cache vecSearchColumnCacheEntry, distance vecDistanceFunc, needNorm bool) (vecScoredHeap, error) {
	heap := newScoredHeap(k, end-start)
	for row := start; row < end; row++ {
		if row&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		vector, norm, valid := cache.resolveRow(row, needNorm)
		if !valid || len(vector) != dims {
			continue
		}
		dist, ok := distance(vector, norm)
		if ok {
			pushTopK(heap, row, dist, k)
		}
	}
	return *heap, nil
}

func segmentScanCache(rows [][]any, boundaries []int, includeNorms bool) vecSearchColumnCacheEntry {
	cache := vecSearchColumnCacheEntry{rows: len(rows), normsReady: includeNorms}
	start := 0
	for _, end := range boundaries {
		cache.segments = append(cache.segments, buildVecColumnSegment(rows[start:end], 0, includeNorms, start))
		start = end
	}
	return cache
}

func TestVecSegmentScanMatchesResolver(t *testing.T) {
	rows := [][]any{
		{[]float64{1, 2}}, {nil}, {}, {"not a vector"}, {[]float64{1}},
		{[]float64{0, 0}}, {[]float64{1, 2}}, {[]float64{math.NaN(), 1}},
		{[]float64{math.Inf(1), 2}}, {[]float64(nil)}, {[]float64{math.SmallestNonzeroFloat64, 0}},
		{[]float64{-1, -2}}, {[]float64{1, 2}},
	}
	for _, normsReady := range []bool{false, true} {
		for _, boundaries := range [][]int{{len(rows)}, {2, 5, 5, 8, len(rows)}} {
			base := segmentScanCache(rows, boundaries, normsReady)
			cases := map[string]vecSearchColumnCacheEntry{"direct": base}
			withGap := base
			withGap.segments = []vecColumnSegment{
				buildVecColumnSegment(rows[:3], 0, normsReady, 0),
				buildVecColumnSegment(rows[6:], 0, normsReady, 6),
			}
			cases["gap"] = withGap
			withOverlap := base
			withOverlap.segments = []vecColumnSegment{
				buildVecColumnSegment(rows[:8], 0, normsReady, 0),
				buildVecColumnSegment(rows[6:], 0, normsReady, 6),
			}
			cases["overlap"] = withOverlap
			withOverrides := base
			withOverrides.overrides = map[int]vecColumnOverride{
				0: {vector: []float64{-2, 1}, norm: math.Sqrt(5), valid: true},
				6: {},
			}
			cases["overrides"] = withOverrides
			for name, cache := range cases {
				if got := cache.directScanSegments(0, len(rows)); got != (name == "direct") {
					t.Fatalf("%s direct scan eligibility = %t", name, got)
				}
				for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
					for _, query := range [][]float64{{1, 2}, nil} {
						distance := buildVecDistanceFunc(metric, query, vectorL2Norm(query))
						for _, bounds := range [][2]int{{0, len(rows)}, {1, len(rows) - 1}, {2, 2}, {4, 9}, {len(rows), len(rows)}} {
							for _, k := range []int{1, 3, 20} {
								got, err := vecSearchTopKRange(t.Context(), rows, bounds[0], bounds[1], len(query), k, cache, distance, metric == "cosine")
								want, wantErr := referenceVecSegmentScan(t.Context(), rows, bounds[0], bounds[1], len(query), k, cache, distance, metric == "cosine")
								gotRows, wantRows := topKFromHeap(&got, k), topKFromHeap(&want, k)
								if err != nil || wantErr != nil || len(gotRows) != len(wantRows) {
									t.Fatalf("norms=%t segments=%d case=%s metric=%s dims=%d bounds=%v k=%d: got %v/%v, want %v/%v", normsReady, len(boundaries), name, metric, len(query), bounds, k, gotRows, err, wantRows, wantErr)
								}
								for i := range gotRows {
									if gotRows[i].rowIdx != wantRows[i].rowIdx || math.Float64bits(gotRows[i].distance) != math.Float64bits(wantRows[i].distance) {
										t.Fatalf("norms=%t segments=%d case=%s metric=%s dims=%d bounds=%v k=%d: got %v, want %v", normsReady, len(boundaries), name, metric, len(query), bounds, k, gotRows, wantRows)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestVecSegmentScanPreservesCancellationCadence(t *testing.T) {
	rows := make([][]any, 3073)
	for i := range rows {
		rows[i] = []any{[]float64{float64(i)}}
	}
	for _, gap := range []bool{false, true} {
		cache := segmentScanCache(rows, []int{500, 900, 2300, len(rows)}, false)
		if gap {
			cache.segments = append(cache.segments[:1:1], cache.segments[2:]...)
		}
		var counts [2]int
		for mode := range counts {
			ctx, cancel := context.WithCancel(t.Context())
			distance := func(vector []float64, _ float64) (float64, bool) {
				counts[mode]++
				if counts[mode] == 5 {
					cancel()
				}
				return vector[0], true
			}
			var err error
			if mode == 0 {
				_, err = referenceVecSegmentScan(ctx, rows, 37, len(rows), 1, 20, cache, distance, false)
			} else {
				_, err = vecSearchTopKRange(ctx, rows, 37, len(rows), 1, 20, cache, distance, false)
			}
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("gap=%t mode=%d: expected cancellation, got %v", gap, mode, err)
			}
		}
		if counts[0] != counts[1] {
			t.Fatalf("gap=%t distance calls before cancellation: %v", gap, counts)
		}
	}
}

func TestVecSegmentScanAppendAndUpdateSQL(t *testing.T) {
	db := setupTestDB()
	const name = "segment_scan_docs"
	t.Cleanup(func() { purgeVectorCachesFor("default", name) })
	execSQL(t, db, `CREATE TABLE segment_scan_docs (id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO segment_scan_docs VALUES (0, '[4,0]'), (1, '[3,0]'), (2, '[2,0]'), (3, '[1,0]')`)
	table, err := db.Get("default", name)
	if err != nil {
		t.Fatal(err)
	}
	getVecColumnCache("default", table, 1, false)
	execSQL(t, db, `INSERT INTO segment_scan_docs VALUES (4, '[0,0]'), (5, '[5,0]'), (6, NULL)`)
	query := `SELECT id, _vec_distance FROM VEC_SEARCH('segment_scan_docs', 'embedding', '[0,0]', 4, 'l2', 'flat')`
	check := func(want []int) {
		result := execSQL(t, db, query)
		if len(result.Rows) != len(want) {
			t.Fatalf("result ids = %v, want %v", result.Rows, want)
		}
		for i, id := range want {
			if fmt.Sprint(result.Rows[i]["id"]) != fmt.Sprint(id) {
				t.Fatalf("result ids = %v, want %v", result.Rows, want)
			}
		}
	}
	check([]int{4, 3, 2, 1})
	table, _ = db.Get("default", name)
	cache := getVecColumnCache("default", table, 1, false)
	if len(cache.segments) < 2 || !cache.directScanSegments(0, len(table.Rows)) {
		t.Fatal("append-only SQL did not exercise segmented scan")
	}
	execSQL(t, db, `UPDATE segment_scan_docs SET embedding = '[0.5,0]' WHERE id = 0`)
	check([]int{4, 0, 3, 2})
	table, _ = db.Get("default", name)
	cache = getVecColumnCache("default", table, 1, false)
	if len(cache.overrides) == 0 || cache.directScanSegments(0, len(table.Rows)) {
		t.Fatal("UPDATE did not preserve the override lookup path")
	}
}
