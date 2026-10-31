package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExplicitNullOrderPaths(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE null_order (id INT, v INT)`)
	execSQL(t, db, `INSERT INTO null_order VALUES (1,NULL),(2,2),(3,1),(4,NULL),(5,2)`)
	for _, tc := range []struct {
		order string
		ids   []int
	}{
		{"ASC", []int{3, 2, 5, 1, 4}},
		{"DESC", []int{2, 5, 3, 1, 4}},
		{"ASC NULLS FIRST", []int{1, 4, 3, 2, 5}},
		{"ASC NULLS LAST", []int{3, 2, 5, 1, 4}},
		{"DESC NULLS FIRST", []int{1, 4, 2, 5, 3}},
		{"DESC NULLS LAST", []int{2, 5, 3, 1, 4}},
	} {
		for _, source := range []string{"null_order", "(SELECT id, v FROM null_order) AS source"} {
			for _, paging := range []string{"", " LIMIT 2", " LIMIT 2 OFFSET 1", " LIMIT 4"} {
				sql := "SELECT id, v FROM " + source + " ORDER BY v " + tc.order + paging
				want := tc.ids
				switch paging {
				case " LIMIT 2":
					want = want[:2]
				case " LIMIT 2 OFFSET 1":
					want = want[1:3]
				case " LIMIT 4":
					want = want[:4]
				}
				rs := execSQL(t, db, sql)
				ids := make([]int, len(rs.Rows))
				for i, r := range rs.Rows {
					ids[i] = r["id"].(int)
				}
				if !reflect.DeepEqual(ids, want) {
					t.Fatalf("%s: %v want %v", sql, ids, want)
				}
			}
		}
	}
	for _, sql := range []string{
		`SELECT DISTINCT v FROM null_order ORDER BY v NULLS LAST`,
		`SELECT v FROM null_order UNION SELECT v FROM null_order ORDER BY v NULLS LAST`,
		`SELECT v,COUNT(*) AS n FROM null_order GROUP BY v ORDER BY v NULLS LAST`,
	} {
		rs := execSQL(t, db, sql)
		if len(rs.Rows) != 3 || rs.Rows[0]["v"] != 1 || rs.Rows[1]["v"] != 2 || rs.Rows[2]["v"] != nil {
			t.Fatalf("%s: %v", sql, rs.Rows)
		}
	}
	// Different NULL placement must produce different cached window shapes.
	rs := execSQL(t, db, `SELECT id, ROW_NUMBER() OVER (ORDER BY v NULLS FIRST) AS first_rank,
		ROW_NUMBER() OVER (ORDER BY v NULLS LAST) AS last_rank FROM null_order ORDER BY id`)
	wantFirst := []int{1, 4, 3, 2, 5}
	wantLast := []int{4, 2, 1, 5, 3}
	for i, r := range rs.Rows {
		if r["first_rank"] != wantFirst[i] || r["last_rank"] != wantLast[i] {
			t.Fatalf("window row %d: %v", i, r)
		}
	}
}

func TestExplicitNullOrderMultiKey(t *testing.T) {
	rows := []Row{{"a": nil, "b": 1}, {"a": 1, "b": nil}, {"a": nil, "b": nil}, {"a": 1, "b": 2}}
	order := []OrderItem{{Col: "a", Desc: true, Nulls: NullsLast}, {Col: "b", Nulls: NullsFirst}}
	want := []Row{rows[1], rows[3], rows[2], rows[0]}
	for _, limit := range []int{2, 4} {
		got := applySortOrderWithLimit(order, append([]Row(nil), rows...), &limit, nil)
		if !reflect.DeepEqual(got, want[:limit]) {
			t.Fatalf("limit %d: %v want %v", limit, got, want[:limit])
		}
	}
}

func TestNullOrderSyntaxErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT 1 AS v ORDER BY v NULLS`, `SELECT 1 AS v ORDER BY v NULLS MIDDLE`,
		`SELECT ROW_NUMBER() OVER (ORDER BY v NULLS) FROM t`,
	} {
		if _, err := NewParser(sql).ParseStatement(); err == nil {
			t.Fatalf("accepted %s", sql)
		}
	}
}
