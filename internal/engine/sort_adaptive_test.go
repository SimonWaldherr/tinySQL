package engine

import (
	"fmt"
	"reflect"
	"testing"
)

// Large pages exercise the point where maintaining a bounded heap costs more
// than sorting the complete input. Fixtures are built outside the timed loop.
func BenchmarkMaterializedOrderLimitFraction(b *testing.B) {
	rows := make([]Row, 20000)
	for i := range rows {
		rows[i] = Row{"id": i, "grp": i % 31, "val": (i * 7919) % len(rows)}
	}
	for _, order := range [][]OrderItem{
		{{Col: "val"}},
		{{Col: "grp"}, {Col: "val", Desc: true}},
	} {
		for _, n := range []int{20, 2000, 10000, 15000, 15001, 18000, 18001, 19999} {
			b.Run(fmt.Sprintf("%d-columns/%d", len(order), n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if got := applySortOrderWithLimit(order, rows, &n, nil); len(got) != n {
						b.Fatal(len(got))
					}
				}
			})
		}
	}
}

func TestLargeBoundedOrderMatchesStableSort(t *testing.T) {
	const size = 129
	rows := make([]Row, size)
	for i := range rows {
		var value any = (i * 97) % 19
		if i%11 == 0 {
			value = nil
		}
		rows[i] = Row{"id": i, "grp": i % 7, "val": value}
	}
	for _, order := range [][]OrderItem{
		{{Col: "VAL"}},
		{{Col: "val", Desc: true}},
		{{Col: "GRP"}, {Col: "val", Desc: true}},
		{{Col: "grp", Desc: true}, {Col: "val"}},
	} {
		want := applySortOrder(order, append([]Row(nil), rows...))
		cutover := size - size/10
		for _, n := range []int{1, 2, size/2 - 1, size / 2, size/2 + 1, cutover - 1, cutover, cutover + 1, size - 1, size, size + 1} {
			for _, offset := range []int{0, 1, size / 2, size + 1} {
				got := applySortOrderWithLimit(order, rows, &n, &offset)
				if !reflect.DeepEqual(got, want[:min(size, n+offset)]) {
					t.Fatalf("order=%v limit=%d offset=%d: got %v", order, n, offset, got)
				}
				// The limited sort must not reorder its caller's input slice.
				for i, row := range rows {
					if row["id"] != i {
						t.Fatalf("input row %d moved to %v", i, row["id"])
					}
				}
			}
		}
	}
}

func TestBoundedOrderHeapInitializationAcrossKeyChunks(t *testing.T) {
	rows := make([]Row, 2*rawKeyArenaChunkRows+17)
	for i := range rows {
		var value any = (i * 97) % 127
		if i%13 == 0 {
			value = nil
		}
		rows[i] = Row{"id": i, "grp": i % 47, "val": value}
	}
	order := []OrderItem{{Col: "grp", Desc: true}, {Col: "val"}}
	want := applySortOrder(order, append([]Row(nil), rows...))
	// Filling the heap must preserve every key slice across arena chunks;
	// subsequent replacements must only reuse discarded candidates' keys.
	n, offset := rawKeyArenaChunkRows+1, 17
	got := applySortOrderWithLimit(order, rows, &n, &offset)
	if !reflect.DeepEqual(got, want[:n+offset]) {
		t.Fatal("heap initialization across key chunks changed retained rows")
	}
}
