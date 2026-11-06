package engine

import "testing"

func BenchmarkSubscriptionMetricUnchanged(b *testing.B) {
	db := setupPerfTable(b, 10000)
	b.Cleanup(func() { _ = db.Close() })
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT COUNT(*) AS n FROM t`).(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := state.refresh(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}
