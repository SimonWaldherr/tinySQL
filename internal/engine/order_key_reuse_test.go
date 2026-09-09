package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkOrderByMultiColumnLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	runBench(b, db, `SELECT id, grp, sub, val FROM t ORDER BY grp, sub, val DESC LIMIT 20 OFFSET 5`)
}

func BenchmarkMaterializedOrderKeys(b *testing.B) {
	rows := make([]Row, 20000)
	for i := range rows {
		rows[i] = Row{"id": i, "grp": fmt.Sprintf("group-%d", i%50), "val": float64(i)}
	}
	order := []OrderItem{{Col: "grp"}, {Col: "val", Desc: true}}
	for _, n := range []int{20, 20000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := applySortOrderWithLimit(order, rows, &n, nil); len(got) != n {
					b.Fatal(len(got))
				}
			}
		})
	}
}

func TestBoundedOrderKeysMatchFullSort(t *testing.T) {
	for _, size := range []int{3, 63, 1000} {
		rows := make([]Row, size)
		for i := range rows {
			var group any = fmt.Sprintf("group-%d", i%7)
			if i%11 == 0 {
				group = nil
			}
			rows[i] = Row{"id": i, "grp": group, "val": i % 13}
		}
		for _, order := range [][]OrderItem{
			{{Col: "grp"}, {Col: "val", Desc: true}},
			{{Col: "grp", Desc: true}, {Col: "val"}},
			{{Col: "val"}},
		} {
			want := applySortOrder(order, append([]Row(nil), rows...))
			for _, n := range []int{1, 2, 20, size} {
				for _, offset := range []int{0, 5} {
					got := applySortOrderWithLimit(order, rows, &n, &offset)
					count := min(size, n+offset)
					if !reflect.DeepEqual(got, want[:count]) {
						t.Fatalf("size=%d limit=%d offset=%d order=%v", size, n, offset, order)
					}
				}
			}
		}
	}
}

func TestRawBoundedOrderKeysMatchFullSort(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE key_reuse (id INT, grp TEXT, val INT)")
	table, err := db.Get("default", "key_reuse")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		var group any = fmt.Sprintf("group-%d", i%7)
		if i%11 == 0 {
			group = nil
		}
		table.Rows = append(table.Rows, []any{i, group, i % 13})
	}
	table.Version++
	for _, order := range []string{"grp, val DESC, id", "grp DESC, val, id DESC", "glen, shifted DESC, id"} {
		query := "SELECT id, grp, val, LENGTH(grp) AS glen, val + 1 AS shifted FROM key_reuse ORDER BY " + order
		want := execSQL(t, db, query)
		for _, bounds := range [][2]int{{1, 0}, {20, 5}, {4097, 0}, {20, 4995}, {20, 5001}} {
			n, offset := bounds[0], bounds[1]
			got := execSQL(t, db, fmt.Sprintf("%s LIMIT %d OFFSET %d", query, n, offset))
			start, end := min(offset, len(want.Rows)), min(offset+n, len(want.Rows))
			if len(got.Rows) != end-start {
				t.Fatalf("%s %v: count %d", order, bounds, len(got.Rows))
			}
			for i, row := range got.Rows {
				if !reflect.DeepEqual(row, want.Rows[start+i]) {
					t.Fatalf("%s %v row %d: got %v want %v", order, bounds, i, row, want.Rows[start+i])
				}
			}
		}
	}
}
