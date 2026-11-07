package engine

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkCTEWideBinding(b *testing.B) {
	rs := &ResultSet{Cols: make([]string, 128), Rows: make([]Row, 20)}
	projections := make([]string, len(rs.Cols))
	for i := range rs.Cols {
		rs.Cols[i] = fmt.Sprintf("C%03d", i)
		projections[i] = "q." + rs.Cols[i]
	}
	for i := range rs.Rows {
		rs.Rows[i] = make(Row, len(rs.Cols))
		for j, column := range rs.Cols {
			rs.Rows[i][strings.ToLower(column)] = i + j
		}
	}
	stmt := mustParse("SELECT " + strings.Join(projections, ",") + " FROM c q WHERE q.C000>=0").(*Select)
	env := ExecEnv{ctx: b.Context(), ctes: map[string]*ResultSet{"c": rs}}
	b.ReportAllocs()
	for b.Loop() {
		result, ok, err := executeSimpleCTESelectFastPath(env, stmt)
		if err != nil || !ok || len(result.Rows) != 20 {
			b.Fatalf("CTE binding: %v / %v", ok, err)
		}
	}
}

func BenchmarkDeleteReturningScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	table, _ := db.Get("default", "t")
	original := table.Rows
	stmt := mustParse("DELETE FROM t WHERE id = 10000 RETURNING id")
	b.ReportAllocs()
	for b.Loop() {
		rs, err := Execute(b.Context(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 1 {
			b.Fatalf("delete: %v / %v", rs, err)
		}
		table.Rows = original
	}
}

func BenchmarkCTEQualifiedFilter(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	runBench(b, db, "WITH c AS (SELECT id, grp, val FROM t) SELECT q.id, q.grp FROM c q WHERE q.id >= 10000 AND q.id < 10100 LIMIT 20")
}

func BenchmarkInsertSelectedRows(b *testing.B) {
	db := setupPerfTable(b, 10000)
	defer db.Close()
	if _, err := Execute(b.Context(), db, "default", mustParse("CREATE TABLE selected_rows (id INT, val FLOAT)")); err != nil {
		b.Fatal(err)
	}
	table, _ := db.Get("default", "selected_rows")
	stmt := mustParse("INSERT INTO selected_rows SELECT id, val FROM t")
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		if len(table.Rows) != 10000 {
			b.Fatal("rows lost")
		}
		table.Rows = nil
	}
}

func BenchmarkCreateSelectedRows(b *testing.B) {
	for _, temporary := range []bool{false, true} {
		b.Run(fmt.Sprintf("temp=%t", temporary), func(b *testing.B) {
			db := setupPerfTable(b, 10000)
			defer db.Close()
			prefix := "CREATE TABLE"
			if temporary {
				prefix = "CREATE TEMP TABLE"
			}
			stmt := mustParse(prefix + " stage AS SELECT id, val FROM t")
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
				if err := db.Drop("default", "stage"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
