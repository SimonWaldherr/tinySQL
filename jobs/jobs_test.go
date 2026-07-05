package jobs

import (
	"strings"
	"testing"
	"time"
)

func TestBuildIntervalJob(t *testing.T) {
	enabled := false
	job, err := Build(Config{
		Name:         " refresh ",
		SQL:          " SELECT 1 ",
		ScheduleType: "interval",
		IntervalMs:   5000,
		Enabled:      &enabled,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if job.Name != "refresh" || job.SQLText != "SELECT 1" || job.ScheduleType != "INTERVAL" || job.Enabled {
		t.Fatalf("unexpected job: %#v", job)
	}
	if job.MaxRuntimeMs != defaultMaxRuntimeMs {
		t.Fatalf("expected default runtime, got %d", job.MaxRuntimeMs)
	}
}

func TestBuildRequiresScheduleFields(t *testing.T) {
	if _, err := Build(Config{Name: "bad", SQL: "SELECT 1", ScheduleType: "INTERVAL"}); err == nil || !strings.Contains(err.Error(), "interval_ms") {
		t.Fatalf("expected interval error, got %v", err)
	}
	if _, err := Build(Config{Name: "bad", SQL: "SELECT 1", ScheduleType: "CRON"}); err == nil || !strings.Contains(err.Error(), "cron_expr") {
		t.Fatalf("expected cron error, got %v", err)
	}
	if _, err := Build(Config{Name: "bad", SQL: "SELECT 1", ScheduleType: "ONCE"}); err == nil || !strings.Contains(err.Error(), "run_at") {
		t.Fatalf("expected once error, got %v", err)
	}
}

func TestConstructors(t *testing.T) {
	if _, err := NewInterval("i", "SELECT 1", time.Second, true); err != nil {
		t.Fatalf("NewInterval: %v", err)
	}
	if _, err := NewCron("c", "SELECT 1", "0 * * * * *", "UTC", true); err != nil {
		t.Fatalf("NewCron: %v", err)
	}
	if _, err := NewOnce("o", "SELECT 1", time.Now().Add(time.Minute), true); err != nil {
		t.Fatalf("NewOnce: %v", err)
	}
}
