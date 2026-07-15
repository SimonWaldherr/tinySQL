package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAnalyzePersistsStatisticsAndChoosesMostSelectiveIndex(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execSQL(t, db, `CREATE TABLE events (id INT, category TEXT, user_id INT)`)

	values := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		category := "odd"
		if i%2 == 0 {
			category = "even"
		}
		values = append(values, fmt.Sprintf("(%d, '%s', %d)", i, category, i))
	}
	execSQL(t, db, `INSERT INTO events VALUES `+strings.Join(values, ", "))
	execSQL(t, db, `CREATE INDEX idx_events_category ON events(category)`)
	execSQL(t, db, `CREATE UNIQUE INDEX idx_events_user ON events(user_id)`)

	statsResult := execSQL(t, db, `ANALYZE events`)
	if len(statsResult.Rows) != 1 || expectAsInt(t, statsResult.Rows[0]["row_count"]) != 100 {
		t.Fatalf("ANALYZE result = %#v", statsResult.Rows)
	}

	table, err := db.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	stats := table.Statistics()
	if stats == nil || stats.Stale || stats.Columns["category"].DistinctCount != 2 || stats.Columns["user_id"].DistinctCount != 100 {
		t.Fatalf("unexpected table statistics: %#v", stats)
	}

	explain, err := Execute(ctx, db, "default", mustParse(`EXPLAIN SELECT id FROM events WHERE category = 'even' AND user_id = 42`))
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	found := false
	for _, row := range explain.Rows {
		if row["operation"] == "INDEX POINT SEEK" && strings.Contains(fmt.Sprint(row["detail"]), "index=idx_events_user") {
			found = true
		}
	}
	if !found {
		t.Fatalf("statistics did not select user index: %#v", explain.Rows)
	}

	sysStats := execSQL(t, db, `SELECT distinct_count, is_stale FROM sys.statistics WHERE table_name = 'events' AND column_name = 'user_id'`)
	if len(sysStats.Rows) != 1 || expectAsInt(t, sysStats.Rows[0]["distinct_count"]) != 100 || sysStats.Rows[0]["is_stale"] != false {
		t.Fatalf("sys.statistics = %#v", sysStats.Rows)
	}

	execSQL(t, db, `UPDATE events SET category = 'odd' WHERE user_id = 42`)
	if stats := table.Statistics(); stats == nil || !stats.Stale || stats.RowCount != 100 {
		t.Fatalf("statistics were not invalidated after DML: %#v", stats)
	}
}
