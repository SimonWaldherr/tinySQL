package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// SimpleExecutor implements the JobExecutor interface for demo purposes
type SimpleExecutor struct {
	db *storage.DB
}

func (e *SimpleExecutor) ExecuteSQL(ctx context.Context, sql string) (interface{}, error) {
	log.Printf("Executing SQL: %s", sql)
	// In real implementation, this would parse and execute the SQL
	// For demo, just log it
	return nil, nil
}

func main() {
	fmt.Println("=== TinySQL Catalog & Scheduler Demo ===")
	fmt.Println()

	// Create database
	db := storage.NewDB()
	catalog := db.Catalog()

	// ==================== Register Tables ====================
	fmt.Println("1. Registering tables in catalog...")
	
	table1Cols := []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "name", Type: storage.StringType},
		{Name: "email", Type: storage.StringType},
		{Name: "created_at", Type: storage.TimeType},
	}
	catalog.RegisterTable("main", "users", table1Cols)

	table2Cols := []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "user_id", Type: storage.IntType},
		{Name: "amount", Type: storage.Float64Type},
		{Name: "status", Type: storage.StringType},
	}
	catalog.RegisterTable("main", "orders", table2Cols)

	// ==================== Query Catalog ====================
	fmt.Println("\n2. Querying catalog.tables:")
	tables := catalog.GetTables()
	for _, t := range tables {
		fmt.Printf("   - %s.%s (type: %s, created: %s)\n", 
			t.Schema, t.Name, t.Type, t.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	fmt.Println("\n3. Querying catalog.columns for 'users':")
	columns := catalog.GetColumns("main", "users")
	for _, c := range columns {
		fmt.Printf("   - %s.%s.%s: %s (position: %d, nullable: %t)\n",
			c.Schema, c.TableName, c.Name, c.DataType, c.Position, c.IsNullable)
	}

	// ==================== Register Views ====================
	fmt.Println("\n4. Registering views:")
	catalog.RegisterView("main", "active_users", "SELECT * FROM users WHERE status = 'active'")
	catalog.RegisterView("main", "order_summary", "SELECT user_id, COUNT(*) as order_count, SUM(amount) as total FROM orders GROUP BY user_id")

	// ==================== Register Functions ====================
	fmt.Println("\n5. Registering functions:")
	
	catalog.RegisterFunction(&storage.CatalogFunction{
		Schema:          "main",
		Name:            "file",
		FunctionType:    "SCALAR",
		ArgTypes:        []string{"STRING"},
		ReturnType:      "STRING",
		Language:        "BUILTIN",
		IsDeterministic: false,
		Description:     "Read file contents as string",
	})

	catalog.RegisterFunction(&storage.CatalogFunction{
		Schema:          "main",
		Name:            "table_from_json",
		FunctionType:    "TABLE",
		ArgTypes:        []string{"STRING"},
		ReturnType:      "TABLE",
		Language:        "BUILTIN",
		IsDeterministic: true,
		Description:     "Parse JSON array into table",
	})

	// ==================== Create Jobs ====================
	fmt.Println("\n6. Creating scheduled jobs:")

	// CRON job: Runs every minute
	cronJob := &storage.CatalogJob{
		Name:         "cleanup_old_logs",
		SQLText:      "DELETE FROM logs WHERE created_at < datetime('now', '-7 days')",
		ScheduleType: "CRON",
		CronExpr:     "0 * * * * *", // Every minute at :00 seconds
		Timezone:     "UTC",
		Enabled:      true,
		NoOverlap:    true,
		MaxRuntimeMs: 60000, // 1 minute max
	}
	catalog.RegisterJob(cronJob)
	fmt.Printf("   - Created CRON job: %s (%s)\n", cronJob.Name, cronJob.CronExpr)

	// INTERVAL job: Runs every 30 seconds
	intervalJob := &storage.CatalogJob{
		Name:         "refresh_stats",
		SQLText:      "INSERT INTO stats_cache SELECT * FROM compute_stats()",
		ScheduleType: "INTERVAL",
		IntervalMs:   30000, // 30 seconds
		Enabled:      true,
		CatchUp:      false,
		MaxRuntimeMs: 10000, // 10 seconds max
	}
	catalog.RegisterJob(intervalJob)
	fmt.Printf("   - Created INTERVAL job: %s (every 30s)\n", intervalJob.Name)

	// ONCE job: Runs at specific time
	runAt := time.Now().Add(5 * time.Second)
	onceJob := &storage.CatalogJob{
		Name:         "send_report",
		SQLText:      "SELECT generate_report('weekly')",
		ScheduleType: "ONCE",
		RunAt:        &runAt,
		Enabled:      true,
		MaxRuntimeMs: 30000, // 30 seconds max
	}
	catalog.RegisterJob(onceJob)
	fmt.Printf("   - Created ONCE job: %s (runs at %s)\n", onceJob.Name, runAt.Format("15:04:05"))

	// ==================== Start Scheduler ====================
	fmt.Println("\n7. Starting job scheduler...")
	executor := &SimpleExecutor{db: db}
	scheduler := storage.NewScheduler(db, executor)
	
	if err := scheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// ==================== Monitor Jobs ====================
	fmt.Println()
	fmt.Println("8. Monitoring jobs (15 seconds)...")
	fmt.Println("   (Watch the log output for job executions)")
	fmt.Println()

	time.Sleep(15 * time.Second)

	// ==================== Query Job Status ====================
	fmt.Println()
	fmt.Println("9. Job status after 15 seconds:")
	jobs := catalog.ListJobs()
	for _, job := range jobs {
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

		fmt.Printf("   - %s: %s | last: %s | next: %s\n", 
			job.Name, status, lastRun, nextRun)
	}

	// ==================== Cleanup ====================
	fmt.Println()
	fmt.Println("10. Stopping scheduler...")
	scheduler.Stop()

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}
