package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
)

func BenchmarkAggregateCompositeLookup(b *testing.B) {
	for _, cardinality := range []int{350, 20000} {
		b.Run(fmt.Sprint(cardinality), func(b *testing.B) {
			rows := make([][]any, 20000)
			for i := range rows {
				group := i % cardinality
				rows[i] = []any{fmt.Sprintf("group-%d", group%50), fmt.Sprintf("sub-%d", group/50)}
			}
			projs := []simpleAggregateProjection{{kind: aggCount}}
			cols := []int{0, 1}
			b.ReportAllocs()
			for b.Loop() {
				groups := newAggregateGroups(projs, len(cols))
				for _, row := range rows {
					groups.state(row, cols).counts[0]++
				}
				if len(groups.order) != cardinality {
					b.Fatalf("got %d groups, want %d", len(groups.order), cardinality)
				}
			}
		})
	}
}

func TestCompositeGroupLookupMatchesEncodedKeys(t *testing.T) {
	values := []any{
		"", "a", "b", "a\x1fb", "b\x1fc", "a;b", "S1:a;", "N;", "1",
		1, int64(1), float64(1), nil, true, false,
		float64(0), math.Copysign(0, -1), math.NaN(),
		math.Float64frombits(0x7ff8000000000001), math.Inf(1),
		[]byte("a"), big.NewRat(1, 3),
	}
	projs := []simpleAggregateProjection{{kind: aggCount}}
	for _, cols := range [][]int{{0, 1}, {1, 0}, {2, 0}, {0, 0}} {
		t.Run(fmt.Sprint(cols), func(t *testing.T) {
			got := newAggregateGroups(projs, len(cols))
			want := newAggregateGroups(projs, len(cols))
			for range 3 {
				for _, left := range values {
					for _, right := range values {
						raw := []any{left, right, "constant"}
						got.state(raw, cols).counts[0]++
						want.encodedState(raw, cols).counts[0]++
					}
				}
			}
			if len(got.order) != len(want.order) {
				t.Fatalf("got %d groups, want %d", len(got.order), len(want.order))
			}
			encode := func(values []any) string {
				var key []byte
				for _, v := range values {
					key = writeFmtKeyPart(key, v)
				}
				return string(key)
			}
			for i, state := range got.order {
				if encode(state.groupValues) != encode(want.order[i].groupValues) || !reflect.DeepEqual(state.counts, want.order[i].counts) {
					t.Fatalf("group %d: changed equivalence, encounter order or aggregate values", i)
				}
			}
		})
	}
}
