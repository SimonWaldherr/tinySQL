package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const boundedRecursiveCTEQuery = `
	WITH RECURSIVE c(n,title) AS (
		SELECT 0 AS seed, 'root' AS anchor_title
		UNION ALL
		SELECT f.id AS next_n, f.label AS next_title
		FROM (SELECT n FROM c WHERE n=0) seed
		JOIN cte_fanout f ON 1=1
		%s
	)
	SELECT n,title FROM c`

func setupBoundedRecursiveCTE(tb testing.TB, count int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("cte_fanout", []storage.Column{
		{Name: "id", Type: storage.IntType}, {Name: "label", Type: storage.TextType},
	}, false)
	for id := 1; id <= count; id++ {
		var label any = fmt.Sprintf("label%d", id)
		if id == 2 {
			label = nil
		} else if id == 3 {
			label = "α🙂"
		}
		table.Rows = append(table.Rows, []any{id, label})
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db
}

func TestRecursiveCTEBoundedFanoutAlignment(t *testing.T) {
	db := setupBoundedRecursiveCTE(t, 6)
	for _, tc := range []struct {
		tail string
		ids  []int
	}{
		{"LIMIT 2", []int{0, 1}},
		{"LIMIT 3 OFFSET 2", []int{2, 3, 4}},
		{"LIMIT 0", nil},
		{"LIMIT 20", []int{0, 1, 2, 3, 4, 5, 6}},
		{"", []int{0, 1, 2, 3, 4, 5, 6}},
	} {
		t.Run(tc.tail, func(t *testing.T) {
			rs := execSQL(t, db, fmt.Sprintf(boundedRecursiveCTEQuery, tc.tail))
			if len(rs.Cols) != 2 || rs.Cols[0] != "n" || rs.Cols[1] != "title" || len(rs.Rows) != len(tc.ids) {
				t.Fatalf("result = %v / %v, want ids %v", rs.Cols, rs.Rows, tc.ids)
			}
			for i, id := range tc.ids {
				if got := expectAsInt(t, rs.Rows[i]["n"]); got != id {
					t.Fatalf("row %d id = %v, want %d", i, got, id)
				}
				var label any = fmt.Sprintf("label%d", id)
				switch id {
				case 0:
					label = "root"
				case 2:
					label = nil
				case 3:
					label = "α🙂"
				}
				if rs.Rows[i]["title"] != label {
					t.Fatalf("row %d title = %v, want %v", i, rs.Rows[i]["title"], label)
				}
			}
		})
	}
}

func TestRecursiveCTEBoundedAlignmentKeepsErrors(t *testing.T) {
	db := setupBoundedRecursiveCTE(t, 6)
	for _, tc := range []struct {
		name, sql, diagnostic string
	}{
		{
			name: "projection-after-work-limit",
			sql: strings.Replace(fmt.Sprintf(boundedRecursiveCTEQuery, "LIMIT 2"),
				"f.id AS next_n", "CASE WHEN f.id=6 THEN 1/0 ELSE f.id END AS next_n", 1),
			diagnostic: "division by zero",
		},
		{
			name: "schema-before-truncation",
			sql: `WITH RECURSIVE c(n) AS (
				SELECT 0 AS seed UNION ALL
				SELECT f.id AS next_n, f.label AS next_title
				FROM (SELECT n FROM c WHERE n=0) seed JOIN cte_fanout f ON 1=1
				LIMIT 2
			) SELECT n FROM c`,
			diagnostic: "has 1 columns in anchor and 2 in recursive term",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Execute(t.Context(), db, "default", mustParse(tc.sql))
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("got %v, want %q", err, tc.diagnostic)
			}
		})
	}
}
