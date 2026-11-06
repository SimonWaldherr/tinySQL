package engine

import (
	"fmt"
	"testing"
)

func BenchmarkSubscriptionAggregateAppend(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	for i := range table.Rows {
		table.Rows[i][0] = i
		table.Rows[i][3] = i % 100
	}
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp`).(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		table.Rows = append(table.Rows, []any{len(table.Rows), "group-1", "sub-1", 42, "note"})
		table.Version++
		if delta, err := state.refresh(b.Context()); err != nil || delta == nil {
			b.Fatalf("refresh: %v %v", delta, err)
		}
	}
}

func BenchmarkSubscriptionAggregateMutation(b *testing.B) {
	for _, mode := range []string{"update", "delete", "where_append"} {
		b.Run(mode, func(b *testing.B) {
			db := setupPerfTable(b, 20000)
			b.Cleanup(func() { _ = db.Close() })
			table, err := db.Get("default", "t")
			if err != nil {
				b.Fatal(err)
			}
			for i := range table.Rows {
				table.Rows[i][0] = i
				table.Rows[i][3] = i % 100
			}
			query := `SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp`
			if mode == "where_append" {
				query = `SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t WHERE val >= 40 GROUP BY grp`
			}
			state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
			if _, err := state.refresh(b.Context()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				switch mode {
				case "update":
					r := append([]any(nil), table.Rows[10000]...)
					r[3] = r[3].(int) + 1
					table.Rows[10000] = r
					table.MarkRowUpdated(10000)
				case "delete":
					table.Rows = append(table.Rows[:10000], table.Rows[10001:]...)
					table.MarkDirtyFrom(-1)
					table.Rows = append(table.Rows, []any{len(table.Rows), "group-1", "sub-1", 42, "note"})
				case "where_append":
					table.Rows = append(table.Rows, []any{len(table.Rows), "group-1", "sub-1", 42, "note"})
				}
				table.Version++
				b.StartTimer()
				if _, err := state.refresh(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSubscriptionAggregateHaving(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	table, _ := db.Get("default", "t")
	for i := range table.Rows {
		table.Rows[i][3] = i % 100
	}
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp HAVING SUM(val) > 100`).(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		table.Rows = append(table.Rows, []any{len(table.Rows), "group-1", "sub", 42, "note"})
		table.Version++
		if _, err := state.refresh(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubscriptionAggregateIndexedCount(b *testing.B) {
	db := filteredAggregateFixture(b, 20000, true)
	table, _ := db.Get("default", "facts")
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT category, COUNT(*) AS n FROM facts WHERE region='r1' AND time>=100 AND time<150 GROUP BY category`).(*Select)}
	if _, err := state.refresh(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		query := fmt.Sprintf(`INSERT INTO facts VALUES (%d,'r1','c1',120,1,48,11)`, len(table.Rows)+1)
		if _, err := Execute(b.Context(), db, "default", mustParse(query)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := state.refresh(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}
