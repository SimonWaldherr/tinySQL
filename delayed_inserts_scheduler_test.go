package tinysql

import (
	"context"
	"testing"
	"time"
)

func TestDelayedInsertsScheduledDelivery(t *testing.T) {
	db := NewDB()
	run := func(sql string) *ResultSet {
		t.Helper()
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		return rs
	}
	run(`CREATE TABLE scheduled_docs (id INT)`)
	run(`CALL DELAY_INSERT('0s', 'INSERT INTO scheduled_docs VALUES (1)')`)
	run(`CREATE JOB delayed_ingest SCHEDULE INTERVAL 1000 AS CALL FLUSH_DELAYED_INSERTS(10)`)
	if err := StartJobScheduler(db, "default"); err != nil {
		t.Fatal(err)
	}
	defer StopJobScheduler(db)
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("scheduler did not deliver queued values")
		case <-ticker.C:
			if len(run(`SELECT * FROM scheduled_docs`).Rows) == 1 {
				return
			}
		}
	}
}
