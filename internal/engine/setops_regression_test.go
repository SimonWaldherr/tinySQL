package engine

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSetOperationsAlignRightColumnsByPosition(t *testing.T) {
	db := storage.NewDB()
	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{
			name: "union-all",
			sql:  `SELECT 1 AS a UNION ALL SELECT 2 AS b`,
			want: []any{1, 2},
		},
		{
			name: "union-distinct",
			sql:  `SELECT 1 AS a UNION SELECT 1 AS b UNION SELECT 2 AS c`,
			want: []any{1, 2},
		},
		{
			name: "except",
			sql:  `SELECT 1 AS a UNION ALL SELECT 2 AS a EXCEPT SELECT 2 AS b`,
			want: []any{1},
		},
		{
			name: "intersect",
			sql:  `SELECT 1 AS a UNION ALL SELECT 2 AS a INTERSECT SELECT 2 AS b`,
			want: []any{2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := executeSetOpsSQL(t, db, tc.sql)
			if !reflect.DeepEqual(rs.Cols, []string{"a"}) {
				t.Fatalf("output columns = %v, want [a]", rs.Cols)
			}
			got := setOpsColumnValues(t, rs, "a")
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("values = %#v, want %#v", got, tc.want)
			}
		})
	}

	rs := executeSetOpsSQL(t, db, `
		SELECT 1 AS id, 'left' AS label
		UNION ALL
		SELECT 2 AS other_id, 'right' AS other_label`)
	if !reflect.DeepEqual(rs.Cols, []string{"id", "label"}) {
		t.Fatalf("multi-column output columns = %v, want [id label]", rs.Cols)
	}
	wantRows := []Row{
		{"id": 1, "label": "left"},
		{"id": 2, "label": "right"},
	}
	if !reflect.DeepEqual(rs.Rows, wantRows) {
		t.Fatalf("multi-column rows = %#v, want %#v", rs.Rows, wantRows)
	}
}

func TestCompoundSelectAppliesTrailingOrderLimitAndOffset(t *testing.T) {
	db := storage.NewDB()
	rs := executeSetOpsSQL(t, db, `
		SELECT 3 AS a
		UNION ALL SELECT 1 AS b
		UNION ALL SELECT 2 AS c
		ORDER BY b
		LIMIT 2 OFFSET 1`)
	if !reflect.DeepEqual(rs.Cols, []string{"a"}) {
		t.Fatalf("output columns = %v, want [a]", rs.Cols)
	}
	if got, want := setOpsColumnValues(t, rs, "a"), []any{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered/paged values = %#v, want %#v", got, want)
	}
}

func TestSetOperationsUseSQLiteSetEquality(t *testing.T) {
	db := storage.NewDB()
	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{
			name: "except-is-distinct",
			sql:  `SELECT 1 AS value UNION ALL SELECT 1 AS ignored_value EXCEPT SELECT 2 AS other_value`,
			want: []any{1},
		},
		{
			name: "union-matches-integer-and-real",
			sql:  `SELECT 1 AS value UNION SELECT 1.0 AS ignored_value`,
			want: []any{1},
		},
		{
			name: "except-matches-integer-and-real",
			sql:  `SELECT 1 AS value EXCEPT SELECT 1.0 AS ignored_value`,
			want: []any{},
		},
		{
			name: "intersect-matches-integer-and-real",
			sql:  `SELECT 1 AS value INTERSECT SELECT 1.0 AS ignored_value`,
			want: []any{1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := executeSetOpsSQL(t, db, tc.sql)
			if got := setOpsColumnValues(t, rs, "value"); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("values = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseCompoundSelectAttachesTailToOuterSelect(t *testing.T) {
	stmt, err := NewParser(`
		SELECT 3 AS a
		UNION ALL SELECT 1 AS b
		UNION SELECT 2 AS c
		ORDER BY a DESC
		LIMIT 2 OFFSET 1`).ParseStatement()
	if err != nil {
		t.Fatalf("parse compound SELECT: %v", err)
	}
	sel, ok := stmt.(*Select)
	if !ok {
		t.Fatalf("statement type = %T, want *Select", stmt)
	}
	if sel.Union == nil || sel.Union.Next == nil {
		t.Fatalf("expected a flat two-clause union chain, got %#v", sel.Union)
	}
	if len(sel.OrderBy) != 1 || !sel.OrderBy[0].Desc || sel.OrderBy[0].Col != "a" {
		t.Fatalf("outer ORDER BY = %#v, want a DESC", sel.OrderBy)
	}
	if sel.Limit == nil || *sel.Limit != 2 || sel.Offset == nil || *sel.Offset != 1 {
		t.Fatalf("outer LIMIT/OFFSET = %v/%v, want 2/1", sel.Limit, sel.Offset)
	}
	for clause := sel.Union; clause != nil; clause = clause.Next {
		if len(clause.Right.OrderBy) != 0 || clause.Right.Limit != nil || clause.Right.Offset != nil {
			t.Fatalf("right-hand term retained compound tail: %#v", clause.Right)
		}
	}
}

func BenchmarkDistinctSetOperationNumericRows(b *testing.B) {
	const (
		rowCount      = 10_000
		distinctCount = 500
	)
	rows := make([]Row, rowCount)
	for i := range rows {
		value := any(i % distinctCount)
		if i%2 != 0 {
			value = float64(i % distinctCount)
		}
		rows[i] = Row{"value": value}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := distinctSetOperationRows(rows, []string{"value"})
		if len(result) != distinctCount {
			b.Fatalf("result rows = %d, want %d", len(result), distinctCount)
		}
	}
}

func executeSetOpsSQL(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return rs
}

func setOpsColumnValues(t *testing.T, rs *ResultSet, col string) []any {
	t.Helper()
	values := make([]any, len(rs.Rows))
	for i, row := range rs.Rows {
		value, ok := row[strings.ToLower(col)]
		if !ok {
			t.Fatalf("row %d missing output column %q: %#v", i, col, row)
		}
		values[i] = value
	}
	return values
}
