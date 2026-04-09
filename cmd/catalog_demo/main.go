// catalog_demo – tinySQL Catalog & Scheduler demonstration.
//
// This tool shows how to use tinySQL's built-in catalog (table/view/function
// registry) and job scheduler to run periodic SQL maintenance tasks.
//
// The scheduler executor is backed by the real tinySQL engine, so every
// scheduled job actually runs its SQL statement and prints the result.
//
// Usage:
//
//	catalog_demo
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TinySQLExecutor implements storage.JobExecutor using the tinySQL engine.
// It parses and executes SQL statements, printing SELECT results to stdout.
type TinySQLExecutor struct {
	db     *tinysql.DB
	tenant string
}

func (e *TinySQLExecutor) ExecuteSQL(ctx context.Context, sql string) (interface{}, error) {
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	rs, err := tinysql.Execute(ctx, e.db, e.tenant, stmt)
	if err != nil {
		return nil, fmt.Errorf("execute: %w", err)
	}
	if rs != nil && len(rs.Rows) > 0 {
		log.Printf("job result (%d rows): %v", len(rs.Rows), formatFirstRow(rs))
	}
	return rs, nil
}

func formatFirstRow(rs *tinysql.ResultSet) string {
	if len(rs.Rows) == 0 {
		return "(empty)"
	}
	row := rs.Rows[0]
	parts := make([]string, 0, len(rs.Cols))
	for _, col := range rs.Cols {
		parts = append(parts, fmt.Sprintf("%s=%v", col, row[col]))
	}
	return strings.Join(parts, ", ")
}

func main() {
	fmt.Println("=== tinySQL Catalog & Scheduler Demo ===")
	fmt.Println()

	// ── Set up tinySQL database ────────────────────────────────────────────
	tdb := tinysql.NewDB()
	tenant := "default"
	ctx := context.Background()

	exec := func(sql string) {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			log.Fatalf("parse error: %v (sql: %s)", err, sql)
		}
		if _, err := tinysql.Execute(ctx, tdb, tenant, stmt); err != nil {
			log.Fatalf("execute error: %v (sql: %s)", err, sql)
		}
	}
	// Seed data for the scheduled jobs to work with.
	// Values are constructed from literal constants and loop counters only.
	exec(`CREATE TABLE events (id INT, kind TEXT, ts INT, payload TEXT)`)
	exec(`CREATE TABLE event_stats (kind TEXT, total INT, last_updated INT)`)

	baseTS := time.Now().Unix()
	kinds := [3]string{"click", "click", "view"} // index mod 3 selects the kind
	for i := 1; i <= 20; i++ {
		kind := kinds[i%3]
		id := i
		ts := baseTS - int64(i*60)
		payload := fmt.Sprintf("payload-%d", id)
		// Build INSERT from fully controlled integer/string values.
		exec(fmt.Sprintf(`INSERT INTO events VALUES (%d, '%s', %d, '%s')`,
			id, kind, ts, payload))
	}
	fmt.Println("✓ Seeded events table with 20 rows")

	// ── Register tables in catalog ─────────────────────────────────────────
	fmt.Println()
	fmt.Println("1. Registering tables in catalog...")
	catalog := tdb.Catalog()

	catalog.RegisterTable("main", "events", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "kind", Type: storage.StringType},
		{Name: "ts", Type: storage.IntType},
		{Name: "payload", Type: storage.StringType},
	})
	catalog.RegisterTable("main", "event_stats", []storage.Column{
		{Name: "kind", Type: storage.StringType},
		{Name: "total", Type: storage.IntType},
		{Name: "last_updated", Type: storage.IntType},
	})

	// ── Query catalog ──────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("2. Tables registered in catalog:")
	for _, t := range catalog.GetTables() {
		fmt.Printf("   - %s.%s (type: %s, created: %s)\n",
			t.Schema, t.Name, t.Type, t.CreatedAt.Format("15:04:05"))
	}

	fmt.Println()
	fmt.Println("3. Columns for 'events':")
	for _, c := range catalog.GetColumns("main", "events") {
		fmt.Printf("   - %-15s %s (position %d, nullable: %t)\n",
			c.Name, c.DataType, c.Position, c.IsNullable)
	}

	// ── Register a view ────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("4. Registering views...")
	catalog.RegisterView("main", "recent_events",
		"SELECT * FROM events ORDER BY ts DESC LIMIT 10")
	fmt.Println("   - registered view: recent_events")

	// ── Register functions ─────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("5. Registering functions...")
	catalog.RegisterFunction(&storage.CatalogFunction{
		Schema:          "main",
		Name:            "json_get",
		FunctionType:    "SCALAR",
		ArgTypes:        []string{"STRING", "STRING"},
		ReturnType:      "STRING",
		Language:        "BUILTIN",
		IsDeterministic: true,
		Description:     "Extract a field from a JSON string",
	})
	fmt.Println("   - registered function: json_get")

	// ── Create scheduled jobs ──────────────────────────────────────────────
	fmt.Println()
	fmt.Println("6. Creating scheduled jobs...")

	// INTERVAL job: refresh event stats every 2 seconds
	statsJob := &storage.CatalogJob{
		Name:         "refresh_event_stats",
		SQLText:      `SELECT kind, COUNT(*) AS total FROM events GROUP BY kind ORDER BY kind`,
		ScheduleType: "INTERVAL",
		IntervalMs:   2000,
		Enabled:      true,
		CatchUp:      false,
		MaxRuntimeMs: 5000,
	}
	catalog.RegisterJob(statsJob)
	fmt.Printf("   - INTERVAL job %q every 2s: %s\n", statsJob.Name, statsJob.SQLText)

	// ONCE job: run an integrity check 1 second from now
	runAt := time.Now().Add(1 * time.Second)
	integrityJob := &storage.CatalogJob{
		Name:         "integrity_check",
		SQLText:      `SELECT COUNT(*) AS total_events FROM events`,
		ScheduleType: "ONCE",
		RunAt:        &runAt,
		Enabled:      true,
		MaxRuntimeMs: 5000,
	}
	catalog.RegisterJob(integrityJob)
	fmt.Printf("   - ONCE job %q at %s: %s\n",
		integrityJob.Name, runAt.Format("15:04:05"), integrityJob.SQLText)

	// ── Start scheduler ────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("7. Starting scheduler (jobs will execute real SQL)...")
	executor := &TinySQLExecutor{db: tdb, tenant: tenant}
	scheduler := storage.NewScheduler(tdb, executor)

	if err := scheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// ── Monitor for a few seconds ──────────────────────────────────────────
	fmt.Println()
	fmt.Println("8. Monitoring jobs for 6 seconds (watch log output)...")
	time.Sleep(6 * time.Second)

	// ── Job status report ──────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("9. Job status:")
	for _, job := range catalog.ListJobs() {
		status := "disabled"
		if job.Enabled {
			status = "enabled"
		}
		lastRun := "never"
		if job.LastRunAt != nil {
			lastRun = job.LastRunAt.Format("15:04:05")
		}
		nextRun := "n/a"
		if job.NextRunAt != nil {
			nextRun = job.NextRunAt.Format("15:04:05")
		}
		fmt.Printf("   %-25s %s | last: %s | next: %s\n",
			job.Name, status, lastRun, nextRun)
	}

	// ── Cleanup ────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("10. Stopping scheduler...")
	scheduler.Stop()

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}
