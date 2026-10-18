package engine

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkMaterializedSingleOrderLargePage(b *testing.B) {
	const size = 20000
	for _, pattern := range []string{"sorted", "reversed", "shuffled"} {
		rows := make([]Row, size)
		for i := range rows {
			value := i
			switch pattern {
			case "reversed":
				value = size - i
			case "shuffled":
				value = (i * 7919) % size
			}
			rows[i] = Row{"id": i, "value": value}
		}
		b.Run(pattern, func(b *testing.B) {
			limit, offset := size-100, 50
			b.ReportAllocs()
			for b.Loop() {
				got := applySortOrderWithLimit([]OrderItem{{Col: "value"}}, rows, &limit, &offset)
				if len(got) != limit+offset {
					b.Fatalf("retained %d rows", len(got))
				}
			}
		})
	}
}

func TestSingleOrderLargePagesMatchStableReference(t *testing.T) {
	for _, size := range []int{2, 9, 10, 11, 129, 4097} {
		rows := make([]Row, size)
		for i := range rows {
			var value any = (i * 97) % 13
			if i%11 == 0 {
				value = nil
			}
			rows[i] = Row{"id": i, "value": value}
		}
		for _, desc := range []bool{false, true} {
			want := append([]Row(nil), rows...)
			sort.SliceStable(want, func(i, j int) bool {
				return compareOrderedValue(want[i]["value"], want[j]["value"], desc) < 0
			})
			cutover := size - size/10
			for _, keep := range []int{1, cutover - 1, cutover, cutover + 1, size - 1, size, size + 1} {
				if keep < 1 {
					continue
				}
				for _, offset := range []int{0, keep / 2} {
					limit := keep - offset
					got := applySortOrderWithLimit([]OrderItem{{Col: "VALUE", Desc: desc}}, rows, &limit, &offset)
					if !reflect.DeepEqual(got, want[:min(size, keep)]) {
						t.Fatalf("size=%d desc=%v limit=%d offset=%d: changed stable order", size, desc, limit, offset)
					}
					for i, row := range rows {
						if row["id"] != i {
							t.Fatalf("input row %d moved", i)
						}
					}
				}
			}
		}
	}
}

func TestMaterializedSingleOrderLargePageSQL(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE large_page (id INT, value INT)")
	table, err := db.Get("default", "large_page")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 129; i++ {
		var value any = (i * 97) % 13
		if i%11 == 0 {
			value = nil
		}
		table.Rows = append(table.Rows, []any{i, value})
	}
	table.Version++
	// A subquery forces the materialized sorting path. Compare the complete
	// ordered output to pages with and without an OFFSET, including tied NULLs.
	for _, direction := range []string{"ASC", "DESC"} {
		base := "SELECT id, value FROM (SELECT id, value FROM large_page) AS src ORDER BY value " + direction
		want := execSQL(t, db, base)
		for _, bounds := range [][2]int{{117, 0}, {116, 1}, {120, 5}, {128, 0}, {129, 0}, {200, 0}} {
			limit, offset := bounds[0], bounds[1]
			got := execSQL(t, db, fmt.Sprintf("%s LIMIT %d OFFSET %d", base, limit, offset))
			if !reflect.DeepEqual(got.Rows, want.Rows[offset:min(len(want.Rows), limit+offset)]) {
				t.Fatalf("%s LIMIT %d OFFSET %d changed page", direction, limit, offset)
			}
		}
	}
}
