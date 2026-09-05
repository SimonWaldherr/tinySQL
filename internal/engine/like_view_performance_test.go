package engine

import (
	"context"
	"strings"
	"testing"
)

func BenchmarkLikeLiteralSegments(b *testing.B) {
	match := compileLikeStringMatcher("%needle%middle%tail", false)
	text := strings.Repeat("n", 256) + "needle" + strings.Repeat("m", 256) + "middle-tail"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !match(text) {
			b.Fatal("expected match")
		}
	}
}

func BenchmarkMaterializedViewRefreshRows(b *testing.B) {
	db := setupPerfTable(b, 20000)
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE MATERIALIZED VIEW copy_view AS SELECT id AS ID, note AS Note, val AS Value FROM t WITH NO DATA`)); err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`REFRESH MATERIALIZED VIEW copy_view`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkViewResultRows(b *testing.B) {
	rs := &ResultSet{Cols: []string{"ID", "Note", "Value"}, Rows: make([]Row, 20000)}
	for i := range rs.Rows {
		rs.Rows[i] = Row{"id": i, "note": "example", "value": i}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(rowsFromResultSet(rs, "view_alias")) != len(rs.Rows) {
			b.Fatal("missing rows")
		}
	}
}
