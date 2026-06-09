package engine

import (
	"context"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCreateJobUpdatesRunningScheduler(t *testing.T) {
	db := storage.NewDB()
	if err := db.StartJobScheduler(nil); err != nil {
		t.Fatalf("StartJobScheduler failed: %v", err)
	}
	t.Cleanup(db.StopJobScheduler)

	stmt := mustParse("CREATE JOB live_job SCHEDULE INTERVAL 1000 AS SELECT 1")
	if _, err := Execute(context.Background(), db, "main", stmt); err != nil {
		t.Fatalf("CREATE JOB failed: %v", err)
	}

	job, err := db.Catalog().GetJob("live_job")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if job.NextRunAt == nil {
		t.Fatal("expected running scheduler to calculate next_run_at")
	}
	if !job.Enabled {
		t.Fatal("expected job to be enabled")
	}
}

func TestCatalogJobHistoryVirtualTable(t *testing.T) {
	db := storage.NewDB()
	started := time.Now().Truncate(time.Millisecond)
	if err := db.Catalog().AddJobHistory(&storage.CatalogJobHistory{
		JobName:      "job1",
		StartedAt:    started,
		FinishedAt:   started.Add(10 * time.Millisecond),
		DurationMs:   10,
		Status:       "FAILED",
		ErrorMessage: "boom",
	}); err != nil {
		t.Fatalf("AddJobHistory failed: %v", err)
	}

	rs, err := Execute(context.Background(), db, "main", mustParse("SELECT job_name, status, error_message FROM catalog.job_history"))
	if err != nil {
		t.Fatalf("SELECT catalog.job_history failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if got := rs.Rows[0]["job_name"]; got != "job1" {
		t.Fatalf("job_name: got %v", got)
	}
	if got := rs.Rows[0]["status"]; got != "FAILED" {
		t.Fatalf("status: got %v", got)
	}
	if got := rs.Rows[0]["error_message"]; got != "boom" {
		t.Fatalf("error_message: got %v", got)
	}
}
