package engine

import "testing"

// A dashboard's source rows may change fields that do not affect its aggregate.
func BenchmarkSubscriptionAggregateUnrelatedUpdate(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp`).(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		next := append([]any(nil), table.Rows[10000]...)
		next[4] = "updated"
		table.Rows[10000] = next
		table.MarkRowUpdated(10000)
		table.Version++
		b.StartTimer()
		if delta, err := state.refresh(b.Context()); err != nil || delta != nil {
			b.Fatalf("refresh: %v %v", delta, err)
		}
	}
}
