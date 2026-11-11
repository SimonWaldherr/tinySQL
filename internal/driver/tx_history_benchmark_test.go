package driver

import (
	"fmt"
	"testing"
)

// Keep a populated UPDATE history and warm row/index twins before measuring
// one-row transactions. Short runs otherwise miss the history-copy cost.
func BenchmarkTxWarmUpdateHistory(b *testing.B) {
	for _, history := range []int{8, 2048} {
		b.Run(fmt.Sprint(history), func(b *testing.B) {
			const rows = 1000
			db := openTxBenchDB(b, rows)
			ctx := b.Context()
			step := 0
			update := func() {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				// Initial scores are integers; the fractional part ensures even
				// the first warm-up pass changes the row rather than doing a no-op.
				result, err := tx.ExecContext(ctx, "UPDATE bench SET score=? WHERE id=?", float64(step)+0.5, step%rows)
				if err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				if count, err := result.RowsAffected(); err != nil || count != 1 {
					_ = tx.Rollback()
					b.Fatalf("updated %d rows: %v", count, err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
				step++
			}
			for range history {
				update()
			}
			b.ReportAllocs()
			for b.Loop() {
				update()
			}
		})
	}
}
