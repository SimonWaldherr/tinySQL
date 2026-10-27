package engine

import (
	"fmt"
	"reflect"
	"sort"
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

func TestRawOrderMixedNumericNullAndStableTies(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE order_mixed (id INT, grp TEXT, val INT)")
	table, err := db.Get("default", "order_mixed")
	if err != nil {
		t.Fatal(err)
	}
	// Storage users can supply different numeric types for the same column.
	// Equal keys must retain their input order, including NULL group keys.
	table.Rows = [][]any{
		{0, "b", float64(2)}, {1, "a", int64(1)}, {2, nil, int(0)},
		{3, "a", float64(1)}, {4, "a", int(2)}, {5, nil, float64(0)},
		{6, "b", int64(2)}, {7, "a", nil}, {8, "b", int(1)},
		{9, "a", float64(2)},
	}
	table.Version++
	want := []int{4, 9, 1, 3, 7, 0, 6, 8, 2, 5}
	assertIDs := func(sql string, expected []int) {
		t.Helper()
		rs := execSQL(t, db, sql)
		ids := make([]int, 0, len(rs.Rows))
		for _, row := range rs.Rows {
			ids = append(ids, row["id"].(int))
		}
		if !reflect.DeepEqual(ids, expected) {
			t.Fatalf("%s: ids=%v, want %v", sql, ids, expected)
		}
	}
	base := "SELECT id, grp, val FROM order_mixed ORDER BY grp ASC, val DESC"
	assertIDs(base, want)

	// Add an explicit final key so bounded pages have a unique SQL ordering.
	ordered := base + ", id ASC"
	for _, page := range [][2]int{{2, 3}, {4, 2}, {3, 7}, {3, 12}, {50, 0}} {
		limit, offset := page[0], page[1]
		start, end := min(offset, len(want)), min(offset+limit, len(want))
		assertIDs(fmt.Sprintf("%s LIMIT %d OFFSET %d", ordered, limit, offset), want[start:end])
	}
}

func TestRawFullOrderAndTopNWithRepeatedMixedKeys(t *testing.T) {
	const size = 5000
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE order_many (id INT, grp TEXT, val INT)")
	table, err := db.Get("default", "order_many")
	if err != nil {
		t.Fatal(err)
	}
	type expectedRow struct {
		id                   int
		group                string
		value                int
		groupNull, valueNull bool
	}
	reference := make([]expectedRow, size)
	groups := []string{"delta", "alpha", "charlie", "bravo"}
	table.Rows = make([][]any, size)
	for i := range size {
		r := expectedRow{id: i, group: groups[(i*17)%len(groups)], value: (i * 13) % 17, groupNull: i%11 == 0, valueNull: i%19 == 0}
		reference[i] = r
		var group any = r.group
		if r.groupNull {
			group = nil
		}
		var value any = r.value
		switch i % 3 {
		case 1:
			value = int64(r.value)
		case 2:
			value = float64(r.value)
		}
		if r.valueNull {
			value = nil
		}
		table.Rows[i] = []any{i, group, value}
	}
	table.Version++
	// The reference uses the fixture's typed fields rather than SQL's sort
	// implementation, retaining input order for rows with equal keys.
	sort.SliceStable(reference, func(i, j int) bool {
		a, b := reference[i], reference[j]
		if a.groupNull != b.groupNull {
			return !a.groupNull
		}
		if !a.groupNull && a.group != b.group {
			return a.group < b.group
		}
		if a.valueNull != b.valueNull {
			return !a.valueNull
		}
		if !a.valueNull && a.value != b.value {
			return a.value > b.value
		}
		return false
	})
	check := func(query string, offset, limit int) {
		t.Helper()
		got := execSQL(t, db, query)
		start, end := min(offset, size), min(offset+limit, size)
		if len(got.Rows) != end-start {
			t.Fatalf("%s: got %d rows, want %d", query, len(got.Rows), end-start)
		}
		for i, row := range got.Rows {
			if id := row["id"]; id != reference[start+i].id {
				t.Fatalf("%s: row %d id=%v, want %d", query, i, id, reference[start+i].id)
			}
		}
	}
	base := "SELECT id, grp, val FROM order_many ORDER BY grp ASC, val DESC"
	check(base, 0, size)
	// IDs follow input order, so the final key yields the same reference
	// ordering while making the bounded Top-N pages unambiguous.
	for _, page := range [][2]int{{20, 5}, {100, 50}, {20, 4990}, {20, 5005}} {
		limit, offset := page[0], page[1]
		check(fmt.Sprintf("%s, id ASC LIMIT %d OFFSET %d", base, limit, offset), offset, limit)
	}
}
