package storage

import (
	"fmt"
	"reflect"
	"testing"
)

func coldRangeFixture(n int, floating bool) *SecondaryIndex {
	idx := &SecondaryIndex{Name: "range", Columns: []string{"series", "time"}}
	for i := 0; i < n; i++ {
		var v any = i - n/2
		if floating {
			v = float64(i-n/2) / 100
		}
		idx.Entries = append(idx.Entries, IndexEntry{Key: CanonicalIndexKey([]any{"a", v}), RowIDs: []int{i}})
	}
	return idx
}

func TestColdRangeAndPrefixMatchHydrated(t *testing.T) {
	for _, floating := range []bool{false, true} {
		idx := coldRangeFixture(1000, floating)
		// Duplicate timestamps/coordinates are legal and all matching rows survive.
		idx.Entries[500].RowIDs = append(idx.Entries[500].RowIDs, 1001)
		warm := idx.clone()
		warm.hydrate()
		table := &Table{}
		value := func(n int) any {
			if floating {
				return float64(n) / 100
			}
			return n
		}
		for _, prefix := range []string{"a", "missing"} {
			for _, lo := range []IndexRangeBound{{Absent: true}, {Value: value(-20), Inclusive: true}, {Value: value(0)}} {
				for _, hi := range []IndexRangeBound{{Absent: true}, {Value: value(20), Inclusive: true}, {Value: value(0)}, {Value: value(-40)}} {
					if lo.Absent && hi.Absent {
						continue
					}
					got, err := table.LookupSecondaryIndexRange(idx, []any{prefix}, lo, hi)
					want, werr := table.LookupSecondaryIndexRange(warm, []any{prefix}, lo, hi)
					if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(err, werr) {
						t.Fatalf("range mismatch: %v %v / %v %v", got, err, want, werr)
					}
				}
			}
			got, err := table.LookupSecondaryIndexPrefix(idx, []any{prefix})
			want, werr := table.LookupSecondaryIndexPrefix(warm, []any{prefix})
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(err, werr) {
				t.Fatal("prefix mismatch")
			}
		}
		if idx.Len() != 1000 {
			t.Fatal("key count mismatch")
		}
		if idx.fast != nil {
			t.Fatal("read rebuilt runtime index")
		}
		// Once a mutation creates the live structure, stale persisted entries must
		// never hide newly inserted log events or measurements.
		idx.hydrate().Insert(CanonicalIndexKey([]any{"a", value(501)}), 1002)
		got, err := table.LookupSecondaryIndexRange(idx, []any{"a"}, IndexRangeBound{Value: value(501), Inclusive: true}, IndexRangeBound{Absent: true})
		if err != nil || !reflect.DeepEqual(got, []int{1002}) {
			t.Fatalf("mutation missing: %v %v", got, err)
		}
	}
}

func BenchmarkColdIndexRange(b *testing.B) {
	fixture := coldRangeFixture(20000, false)
	for _, hydrate := range []bool{true, false} {
		b.Run(fmt.Sprintf("hydrate%v", hydrate), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				idx := &SecondaryIndex{Columns: fixture.Columns, Entries: fixture.Entries}
				if hydrate {
					idx.hydrate()
				}
				got, err := (&Table{}).LookupSecondaryIndexRange(idx, []any{"a"}, IndexRangeBound{Value: 0, Inclusive: true}, IndexRangeBound{Value: 9, Inclusive: true})
				if err != nil || len(got) != 10 {
					b.Fatal(got, err)
				}
			}
		})
	}
}
