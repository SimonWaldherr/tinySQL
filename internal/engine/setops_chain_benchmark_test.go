package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Growing UNION results must not be rehashed once for every additional term.
// The repeated-values case also guards the common small-result workload.
func BenchmarkUnionDistinctChain(b *testing.B) {
	const rowsPerTerm = 128
	for _, termCount := range []int{2, 16, 64} {
		b.Run(fmt.Sprintf("unique/terms=%d", termCount), func(b *testing.B) {
			benchmarkUnionDistinctChain(b, rowsPerTerm, termCount, false)
		})
	}
	b.Run("repeated/terms=64", func(b *testing.B) {
		benchmarkUnionDistinctChain(b, rowsPerTerm, 64, true)
	})
}

func benchmarkUnionDistinctChain(b *testing.B, rowsPerTerm, termCount int, repeated bool) {
	db := setupPerfTable(b, rowsPerTerm)
	b.Cleanup(func() { _ = db.Close() })
	terms := make([]string, termCount)
	for i := range terms {
		offset := i * rowsPerTerm
		if repeated {
			offset = 0
		}
		terms[i] = fmt.Sprintf("SELECT id + %d AS value FROM t", offset)
	}
	stmt := mustParse(strings.Join(terms, " UNION "))
	wantRows := rowsPerTerm * termCount
	if repeated {
		wantRows = rowsPerTerm
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != wantRows {
			b.Fatalf("row count = %d, want %d", len(rs.Rows), wantRows)
		}
	}
}
