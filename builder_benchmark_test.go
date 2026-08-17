package tinysql

import "testing"

var benchmarkBuilderSQL string

func BenchmarkBuilderToSQLNested(b *testing.B) {
	stmt := Select(Col("u.id"), Upper(Col("u.name")), Count(Col("o.id"))).
		FromAs("users", "u").
		LeftJoinAs("orders", "o", Eq(Col("u.id"), Col("o.user_id"))).
		Where(And(
			Gt(Col("u.age"), Val(18)),
			Or(Eq(Col("u.active"), Val(true)), IsNotNull(Col("o.id"))),
		)).
		GroupBy("u.id", "u.name").
		Having(Gt(Count(Col("o.id")), Val(0))).
		OrderByDesc("u.id").
		Limit(50).
		Offset(10).
		Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkBuilderSQL = ToSQL(stmt)
	}
}
