package engine

import (
	"context"
	"reflect"
	"testing"
)

func generalHashJoinRows(n, keyCount, offset int, alias string) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{alias + ".id": i%keyCount + offset, alias + ".payload": i}
	}
	return rows
}

func BenchmarkGeneralHashJoinCompact(b *testing.B) {
	const n = 32768
	for _, fixture := range []struct {
		name             string
		keyCount, offset int
		nullBuild        bool
	}{
		{"unique_no_match", n, n, false},
		{"duplicates_no_match", 256, n, false},
		{"unique_matches", n, 0, false},
		{"null_build", n, n, true},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			left := generalHashJoinRows(n+1, n+1, fixture.offset, "l")
			right := generalHashJoinRows(n, fixture.keyCount, 0, "r")
			if fixture.nullBuild {
				for _, row := range right {
					row["r.id"] = nil
				}
			}
			h := HashJoinOptimizer{env: ExecEnv{ctx: b.Context()}}
			condition := &JoinCondition{LeftColumn: "l.id", RightColumn: "r.id", IsEquiJoin: true}
			for _, layout := range []string{"row_slices", "compact_positions"} {
				b.Run(layout, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var rows []Row
						var err error
						if layout == "row_slices" {
							rows, err = benchmarkGeneralHashJoinRowSlices(&h, left, right)
						} else {
							rows, err = h.processHashJoin(left, right, condition, OptimizedJoinTypeInner)
						}
						if err != nil {
							b.Fatal(err)
						}
						if fixture.offset != 0 && len(rows) != 0 || fixture.offset == 0 && len(rows) != n {
							b.Fatalf("unexpected result cardinality: %d", len(rows))
						}
					}
				})
			}
		})
	}
}

// Retain the former bucket layout only as a benchmark control. The fixtures
// above always build the smaller right relation and use an INNER equality.
func benchmarkGeneralHashJoinRowSlices(h *HashJoinOptimizer, left, right []Row) ([]Row, error) {
	buckets := make(map[any][]Row)
	for _, row := range right {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}
		if key := h.getJoinKey(row, "r.id", "r.id"); key != nil {
			buckets[key] = append(buckets[key], row)
		}
	}
	result := make([]Row, 0, len(right))
	for _, row := range left {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}
		if key := h.getJoinKey(row, "l.id", "l.id"); key != nil {
			for _, matching := range buckets[key] {
				result = append(result, mergeRows(row, matching))
			}
		}
	}
	return result, nil
}

func TestCompactHashJoinPreservesDuplicateOrderAndNulls(t *testing.T) {
	for _, sizes := range [][2]int{{17, 9}, {9, 17}, {17, 17}} {
		left := generalHashJoinRows(sizes[0], 5, 0, "l")
		right := generalHashJoinRows(sizes[1], 4, 0, "r")
		left[0]["l.id"], right[0]["r.id"] = nil, nil
		for _, row := range left {
			row["shared"] = "left"
		}
		for _, row := range right {
			row["shared"] = "right"
		}
		for _, kind := range []OptimizedJoinType{OptimizedJoinTypeInner, OptimizedJoinTypeLeft} {
			for _, reversed := range []bool{false, true} {
				on := "l.id=r.id"
				if reversed {
					on = "r.id=l.id"
				}
				condition := mustParse("SELECT * FROM l JOIN r ON " + on).(*Select).Joins[0].On
				h := HashJoinOptimizer{env: ExecEnv{ctx: t.Context()}}
				got, err := h.ProcessOptimizedJoin(left, right, condition, kind)
				if err != nil {
					t.Fatal(err)
				}
				var want []Row
				if kind == OptimizedJoinTypeLeft || len(left) > len(right) {
					for _, l := range left {
						matched := false
						for _, r := range right {
							if l["l.id"] != nil && l["l.id"] == r["r.id"] {
								want = append(want, mergeRows(l, r))
								matched = true
							}
						}
						if !matched && kind == OptimizedJoinTypeLeft {
							want = append(want, h.addNullsForUnmatchedJoin(l, right[0]))
						}
					}
				} else {
					for _, r := range right {
						for _, l := range left {
							if l["l.id"] != nil && l["l.id"] == r["r.id"] {
								want = append(want, mergeRows(l, r))
							}
						}
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("sizes=%v kind=%d reversed=%v: duplicate order or outer-row values changed", sizes, kind, reversed)
				}
			}
		}
	}
}

func TestCompactHashJoinEmptyAndCanceled(t *testing.T) {
	left := generalHashJoinRows(4, 4, 0, "l")
	condition := &JoinCondition{LeftColumn: "l.id", RightColumn: "r.id", IsEquiJoin: true}
	h := HashJoinOptimizer{env: ExecEnv{ctx: t.Context()}}
	got, err := h.processHashJoin(left, nil, condition, OptimizedJoinTypeLeft)
	if err != nil || !reflect.DeepEqual(got, left) {
		t.Fatal(got, err)
	}
	got[0]["l.payload"] = "private"
	if left[0]["l.payload"] != 0 {
		t.Fatal("unmatched output aliases the source row")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	h.env.ctx = ctx
	if _, err := h.processHashJoin(left, generalHashJoinRows(4, 4, 0, "r"), condition, OptimizedJoinTypeInner); err != context.Canceled {
		t.Fatalf("got %v, want cancellation", err)
	}
}

func TestCompactHashJoinAllNullBuild(t *testing.T) {
	left := generalHashJoinRows(17, 5, 0, "l")
	right := generalHashJoinRows(9, 4, 0, "r")
	for _, row := range right {
		row["r.id"] = nil
	}
	h := HashJoinOptimizer{env: ExecEnv{ctx: t.Context()}}
	condition := &JoinCondition{LeftColumn: "l.id", RightColumn: "r.id", IsEquiJoin: true}
	for _, kind := range []OptimizedJoinType{OptimizedJoinTypeInner, OptimizedJoinTypeLeft} {
		got, err := h.processHashJoin(left, right, condition, kind)
		if err != nil {
			t.Fatal(err)
		}
		if kind == OptimizedJoinTypeInner && len(got) != 0 || kind == OptimizedJoinTypeLeft && len(got) != len(left) {
			t.Fatalf("kind=%d: unexpected result cardinality %d", kind, len(got))
		}
		for i, row := range got {
			if row["l.payload"] != i || row["r.id"] != nil || row["r.payload"] != nil {
				t.Fatalf("NULL key matched a build row: %v", row)
			}
		}
	}
}

func TestCompactHashJoinResidualFallback(t *testing.T) {
	left := generalHashJoinRows(23, 5, 0, "l")
	right := generalHashJoinRows(17, 4, 0, "r")
	condition := mustParse(`SELECT * FROM l JOIN r ON l.id=r.id AND l.payload>r.payload`).(*Select).Joins[0].On
	h := HashJoinOptimizer{env: ExecEnv{ctx: t.Context()}}
	got, err := h.ProcessOptimizedJoin(left, right, condition, OptimizedJoinTypeLeft)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range got {
		if row["r.id"] != nil && (row["l.id"] != row["r.id"] || row["l.payload"].(int) <= row["r.payload"].(int)) {
			t.Fatalf("residual predicate was skipped: %v", row)
		}
	}
	if len(got) <= len(left) {
		t.Fatalf("duplicate-match fixture did not produce multiple matches: %d", len(got))
	}
}
