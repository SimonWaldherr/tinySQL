package driver

import (
	"database/sql/driver"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func nvArg(v any) driver.NamedValue { return driver.NamedValue{Value: v} }

func BenchmarkSqlLiteral_Int(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sqlLiteral(123456789)
	}
}

func BenchmarkSqlLiteral_String(b *testing.B) {
	s := "This is a test string with 'single quotes' and more content to escape"
	b.ReportAllocs()
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
	b.ReportAllocs()
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindPlaceholders(q, args)
	}
}

func BenchmarkBindPlaceholders_MixedAndQuoted(b *testing.B) {
	q := "SELECT '?', ?, $2, :3, 'It''s ?', ?, $1, :2"
	args := []driver.NamedValue{
		nvArg("alpha"),
		nvArg(123),
		nvArg("beta"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindPlaceholders(q, args)
	}
}

func BenchmarkBindPlaceholders_LargeArity(b *testing.B) {
	const n = 100
	q := "INSERT INTO t VALUES ("
	args := make([]driver.NamedValue, n)
	for i := range n {
		if i > 0 {
			q += ","
		}
		q += "$" + strconv.Itoa(i+1)
		args[i] = nvArg(i)
	}
	q += ")"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bindPlaceholders(q, args)
	}
}

func BenchmarkRowsNext(b *testing.B) {
	const rowCount = 1024
	rs := &engine.ResultSet{
		Cols: []string{"ID", "NAME", "SCORE", "ACTIVE"},
		Rows: make([]engine.Row, rowCount),
	}
	for i := range rowCount {
		rs.Rows[i] = engine.Row{
			"id":     i,
			"name":   "user_" + strconv.Itoa(i),
			"score":  float64(i) * 1.25,
			"active": i%2 == 0,
		}
	}
	dest := make([]driver.Value, len(rs.Cols))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := &rows{rs: rs}
		for {
			if err := r.Next(dest); err != nil {
				break
			}
		}
	}
}
