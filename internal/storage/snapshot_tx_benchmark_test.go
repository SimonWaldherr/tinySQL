package storage

import (
	"fmt"
	"testing"
)

// BenchmarkSnapshotForTx isolates the per-BEGIN cost of a transaction's private
// shadow from everything the SQL driver adds around it.
func BenchmarkSnapshotForTx(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			db := NewDB()
			t := NewTable("bench", []Column{
				{Name: "id", Type: IntType, Constraint: PrimaryKey},
				{Name: "name", Type: TextType},
				{Name: "score", Type: Float64Type},
				{Name: "bucket", Type: IntType},
			}, false)
			for i := 0; i < n; i++ {
				t.Rows = append(t.Rows, []any{i, "row", float64(i), i % 64})
			}
			if err := db.Put("default", t); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base, shadow := db.SnapshotForTx()
				_, _ = base, shadow
			}
		})
	}
}

// Compare the two shadow strategies used by single-connection embeddings.
// Both receive the same immutable scalar rows; neither mutates the source.
func BenchmarkWriteTxShadow(b *testing.B) {
	db := NewDB()
	table := NewTable("bench", []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: TextType},
		{Name: "score", Type: Float64Type},
	}, false)
	for i := 0; i < 10_000; i++ {
		table.Rows = append(table.Rows, []any{i, "row", float64(i)})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		clone func() *DB
	}{
		{"DeepClone", db.DeepClone},
		{"SnapshotForWriteTx", db.SnapshotForWriteTx},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tc.clone()
			}
		})
	}
}
