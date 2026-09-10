package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkSubscriptionPointUpdate(b *testing.B) {
	for _, full := range []bool{true, false} {
		b.Run(fmt.Sprintf("full=%t", full), func(b *testing.B) {
			db := setupPerfTable(b, 20000)
			defer db.Close()
			table, _ := db.Get("default", "t")
			for i := range table.Rows {
				table.Rows[i][0] = i
			}
			table.Cols[0].Constraint = storage.PrimaryKey
			state := &querySubscriptionState{db: db, tenant: "default", query: mustParse("SELECT id, val FROM t").(*Select), rows: make(map[int]Row)}
			if _, err := state.refresh(b.Context()); err != nil {
				b.Fatal(err)
			}
			update := mustParse("UPDATE t SET val = val + 1 WHERE id = 10000")
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", update); err != nil {
					b.Fatal(err)
				}
				if full {
					state.structural = -1
				}
				delta, err := state.refresh(b.Context())
				if err != nil || delta == nil || len(delta.Added) != 1 {
					b.Fatalf("delta: %v %v", delta, err)
				}
				if !full && delta.ScannedRows != 1 {
					b.Fatalf("scanned %d", delta.ScannedRows)
				}
			}
		})
	}
}

func BenchmarkSubscriptionGeneralUnchanged(b *testing.B) {
	db := streamTestDB(b, 20000)
	defer db.Close()
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse("SELECT id + 1 AS next_id FROM stream_rows").(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if delta, err := state.refresh(b.Context()); err != nil || delta != nil {
			b.Fatalf("delta: %v, %v", delta, err)
		}
	}
}
