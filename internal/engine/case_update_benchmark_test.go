package engine

import "testing"

func BenchmarkConditionalUpdate(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	statement := mustParse("UPDATE t SET val = CASE WHEN id > 10000 THEN val + 1 ELSE val - 1 END")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := Execute(b.Context(), db, "default", statement)
		if err != nil {
			b.Fatal(err)
		}
		if result.Rows[0]["updated"] != 20000 {
			b.Fatal("unexpected matched row count", result.Rows)
		}
	}
}
