package storage

import (
	"reflect"
	"strings"
	"testing"
)

func TestRangeSeekLongPrefixAndOwnedResults(t *testing.T) {
	for _, prefix := range []string{"a", strings.Repeat("長", 100)} {
		for _, warm := range []bool{false, true} {
			idx := &SecondaryIndex{Columns: []string{"prefix", "n"}}
			for i := -2; i <= 2; i++ {
				idx.Entries = append(idx.Entries, IndexEntry{Key: CanonicalIndexKey([]any{prefix, i}), RowIDs: []int{2 - i}})
			}
			if warm {
				idx.hydrate()
			}
			for _, inclusive := range []bool{false, true} {
				lo := IndexRangeBound{Value: -1, Inclusive: inclusive}
				hi := IndexRangeBound{Value: 1, Inclusive: inclusive}
				want := []int{2}
				if inclusive {
					want = []int{1, 2, 3}
				}
				rows, err := (&Table{}).LookupSecondaryIndexRange(idx, []any{prefix}, lo, hi)
				if err != nil || !reflect.DeepEqual(rows, want) {
					t.Fatalf("range: %v %v", rows, err)
				}
				rows[0] = 999
				rows, err = (&Table{}).LookupSecondaryIndexRange(idx, []any{prefix}, lo, hi)
				if err != nil || !reflect.DeepEqual(rows, want) {
					t.Fatal("result aliases index")
				}
			}
		}
	}
}
