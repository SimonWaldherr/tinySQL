package tinyorm

import (
	"testing"
)

var benchmarkBoundSQL string

func BenchmarkDescribeModelCached(b *testing.B) {
	if _, err := describeModel(testUser{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta, err := describeModel(testUser{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBoundSQL = meta.selectList()
	}
}

func BenchmarkBindNamedStruct(b *testing.B) {
	params := testUser{ID: 42, Name: "Ada Lovelace", Age: 36, Score: 9.5, Active: true}
	const query = "SELECT * FROM users WHERE id = :id AND name = @name AND active = :active"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bound, err := BindNamed(query, params)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBoundSQL = bound
	}
}
