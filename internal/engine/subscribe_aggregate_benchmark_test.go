package engine

import "testing"

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
