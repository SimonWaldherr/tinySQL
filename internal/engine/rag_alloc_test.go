package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGNormalizeRowIDsOwnership(t *testing.T) {
	for _, input := range [][]int{nil, {-3, -1}, {0, 0, 1, 3, 3, 9, 12}, {9, 3, 0, 3, -1, 12}, {12, 13}} {
		original := append([]int(nil), input...)
		got := ragNormalizeRowIDs(10, input)
		var want []int
		for id := 0; id < 10; id++ {
			for _, v := range input {
				if v == id {
					want = append(want, id)
					break
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("input=%v: got %v, want %v", input, got, want)
		}
		if len(got) > 0 {
			got[0] = 99
		}
		if !reflect.DeepEqual(input, original) {
			t.Fatal("result aliases input")
		}
	}
}

func TestRAGAllowedIDBucketsMatchExactScan(t *testing.T) {
	table := &storage.Table{Cols: []storage.Column{{Name: "id", Type: storage.IntType}}, Rows: [][]any{
		{int(1)}, {int64(2)}, {float64(2)}, {"2"}, {int64(9007199254740993)},
		{float64(9007199254740992)}, {true}, {nil}, {},
	}}
	for _, values := range [][]any{{1, int64(2)}, {"2", "missing"}, {float64(2)}, {int64(9007199254740993)}, {1, float64(2), "2", true}, {nil}} {
		got, err := ragRowsForAllowedIDs(table, "id", 0, values)
		if err != nil {
			t.Fatal(err)
		}
		want := ragRowsForAllowedIDsScan(table, 0, values, []int{})
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("values=%v: got %v, want %v", values, got, want)
		}
	}
}

func BenchmarkRAGMapSliceAllocations(b *testing.B) {
	const n = 10000
	rows := make([]int, n)
	values := make([]any, n)
	table := &storage.Table{Cols: []storage.Column{{Name: "id", Type: storage.IntType}}, Rows: make([][]any, n)}
	for i := range n {
		rows[i] = i
		values[i] = i
		table.Rows[i] = []any{i}
	}
	b.Run("SortedRows", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if len(ragNormalizeRowIDs(n, rows)) != n {
				b.Fatal("lost rows")
			}
		}
	})
	b.Run("IntegerIDs", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := ragRowsForAllowedIDs(table, "id", 0, values)
			if err != nil || len(got) != n {
				b.Fatal(err)
			}
		}
	})
}
