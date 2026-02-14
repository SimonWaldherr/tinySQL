package driver

import (
	"database/sql/driver"
	"testing"
)

func nvArg(v any) driver.NamedValue { return driver.NamedValue{Value: v} }

func BenchmarkSqlLiteral_Int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = sqlLiteral(123456789)
	}
}

func BenchmarkSqlLiteral_String(b *testing.B) {
	s := "This is a test string with 'single quotes' and more content to escape"
	for i := 0; i < b.N; i++ {
		_ = sqlLiteral(s)
	}
}

func BenchmarkBindPlaceholders_Sequential(b *testing.B) {
	q := "INSERT INTO t (a,b,c,d,e,f,g,h,i,j) VALUES (?,?,?,?,?,?,?,?,?,?)"
	args := make([]driver.NamedValue, 10)
	for i := 0; i < 10; i++ {
		args[i] = nvArg(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindPlaceholders(q, args)
	}
}

func BenchmarkBindPlaceholders_Numbered(b *testing.B) {
	q := "INSERT INTO t (a,b,c,d,e,f,g,h,i,j) VALUES ($10,$9,$8,$7,$6,$5,$4,$3,$2,$1)"
	args := make([]driver.NamedValue, 10)
	for i := 0; i < 10; i++ {
		args[i] = nvArg(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindPlaceholders(q, args)
	}
}
