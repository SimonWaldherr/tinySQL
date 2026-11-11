package engine

import "testing"

func BenchmarkRawOrderValues(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	for _, query := range []struct {
		name, sql string
		rows      int
	}{
		{"ArithmeticSmall", "SELECT id,id+val*2 AS score FROM t ORDER BY score DESC LIMIT 20", 20},
		{"ArithmeticDeep", "SELECT id,id+val*2 AS score FROM t ORDER BY score DESC LIMIT 20 OFFSET 19980", 20},
		{"ArithmeticFull", "SELECT id,id+val*2 AS score FROM t ORDER BY score DESC", 20000},
		{"CaseSmall", "SELECT id,CASE WHEN val>15000 THEN val ELSE id END AS score FROM t ORDER BY score DESC LIMIT 20", 20},
		{"NullSmall", "SELECT id,COALESCE(NULLIF(val,0),id) AS score FROM t ORDER BY score DESC LIMIT 20", 20},
		{"MultiSmall", "SELECT id,id+val*2 AS score,LOWER(grp) AS name FROM t ORDER BY score DESC,name LIMIT 20", 20},
		{"DirectControl", "SELECT id,val FROM t ORDER BY val DESC LIMIT 20", 20},
	} {
		b.Run(query.name, func(b *testing.B) {
			statement := mustParse(query.sql)
			b.ReportAllocs()
			for b.Loop() {
				result, err := Execute(b.Context(), db, "default", statement)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != query.rows {
					b.Fatal("unexpected result length", len(result.Rows))
				}
			}
		})
	}
}
