package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type schedulerTestExecutor struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (e *schedulerTestExecutor) ExecuteSQL(_ context.Context, sql string) (interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, sql)
	if e.err != nil {
		return nil, e.err
	}
	return "ok", nil
}

func (e *schedulerTestExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func TestSchedulerScheduleJobVariants(t *testing.T) {
	db := NewDB()
	s := NewScheduler(db, nil)
	defer s.cron.Stop()

	interval := &CatalogJob{Name: "interval", ScheduleType: "INTERVAL", IntervalMs: int64(time.Minute / time.Millisecond), Enabled: true}
	if err := s.scheduleJob(interval); err != nil {
		t.Fatalf("schedule interval failed: %v", err)
	}
	if interval.NextRunAt == nil {
		t.Fatal("interval job did not receive NextRunAt")
	}

	runAt := time.Now().Add(time.Hour)
	once := &CatalogJob{Name: "once", ScheduleType: "ONCE", RunAt: &runAt, Enabled: true}
	if err := s.scheduleJob(once); err != nil {
		t.Fatalf("schedule once failed: %v", err)
	}
	if once.NextRunAt == nil || !once.NextRunAt.Equal(runAt) {
		t.Fatalf("once NextRunAt = %v, want %v", once.NextRunAt, runAt)
	}

	cronJob := &CatalogJob{Name: "cron", ScheduleType: "CRON", CronExpr: "*/5 * * * * *", Timezone: "bad/location", Enabled: true}
	if err := s.scheduleJob(cronJob); err != nil {
		t.Fatalf("schedule cron failed: %v", err)
	}
	if cronJob.NextRunAt == nil {
		t.Fatal("cron job did not receive NextRunAt")
	}
	if _, ok := s.cronEntries["cron"]; !ok {
		t.Fatal("cron job was not registered")
	}

	if err := s.scheduleJob(&CatalogJob{Name: "badcron", ScheduleType: "CRON"}); err == nil {
		t.Fatal("expected empty cron expression to fail")
	}
	if err := s.scheduleJob(&CatalogJob{Name: "unknown", ScheduleType: "NEVER"}); err == nil {
		t.Fatal("expected unknown schedule type to fail")
	}
}

func TestSchedulerExecuteJobSuccessFailureAndSkipped(t *testing.T) {
	db := NewDB()
	exec := &schedulerTestExecutor{}
	s := NewScheduler(db, exec)

	success := &CatalogJob{Name: "success", SQLText: "SELECT 1", ScheduleType: "INTERVAL", IntervalMs: 1000, Enabled: true}
	if err := db.Catalog().RegisterJob(success); err != nil {
		t.Fatalf("RegisterJob success failed: %v", err)
	}
	s.executeJob(success)
	s.wg.Wait()

	if exec.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.callCount())
	}
	registered, err := db.Catalog().GetJob("success")
	if err != nil {
		t.Fatalf("GetJob success failed: %v", err)
	}
	if registered.LastRunAt == nil || registered.NextRunAt == nil {
		t.Fatalf("runtime fields not updated: %#v", registered)
	}

	failingExec := &schedulerTestExecutor{err: errors.New("boom")}
	failingScheduler := NewScheduler(db, failingExec)
	failing := &CatalogJob{Name: "failing", SQLText: "SELECT bad", ScheduleType: "ONCE", Enabled: true}
	if err := db.Catalog().RegisterJob(failing); err != nil {
		t.Fatalf("RegisterJob failing failed: %v", err)
	}
	failingScheduler.executeJob(failing)
	failingScheduler.wg.Wait()

	noExecScheduler := NewScheduler(db, nil)
	noExec := &CatalogJob{Name: "noexec", SQLText: "SELECT 2", ScheduleType: "ONCE", Enabled: true}
	if err := db.Catalog().RegisterJob(noExec); err != nil {
		t.Fatalf("RegisterJob noexec failed: %v", err)
	}
	noExecScheduler.executeJob(noExec)
	noExecScheduler.wg.Wait()

	statuses := map[string]string{}
	for _, run := range db.Catalog().ListJobHistory() {
		statuses[run.JobName] = run.Status
	}
	if statuses["success"] != "SUCCEEDED" {
		t.Fatalf("success status = %q", statuses["success"])
	}
	if statuses["failing"] != "FAILED" {
		t.Fatalf("failing status = %q", statuses["failing"])
	}
	if statuses["noexec"] != "SKIPPED" {
		t.Fatalf("noexec status = %q", statuses["noexec"])
	}
}

func TestSchedulerNoOverlapAndLifecycle(t *testing.T) {
	db := NewDB()
	exec := &schedulerTestExecutor{}
	s := NewScheduler(db, exec)

	blocked := &CatalogJob{Name: "blocked", SQLText: "SELECT 1", NoOverlap: true, ScheduleType: "INTERVAL", IntervalMs: 1000, Enabled: true}
	if err := db.Catalog().RegisterJob(blocked); err != nil {
		t.Fatalf("RegisterJob blocked failed: %v", err)
	}

	s.mu.Lock()
	s.running["blocked"] = &jobExecution{startTime: time.Now(), cancelFn: func() {}}
	s.mu.Unlock()

	s.executeJob(blocked)
	for _, run := range db.Catalog().ListJobHistory() {
		if run.JobName == "blocked" && run.Status == "SKIPPED" {
			goto foundSkipped
		}
	}
	t.Fatal("expected skipped no-overlap history")

foundSkipped:
	if err := s.AddJob(&CatalogJob{Name: "enabled-cron", SQLText: "SELECT 1", ScheduleType: "CRON", CronExpr: "*/30 * * * * *", Enabled: true}); err != nil {
		t.Fatalf("AddJob enabled-cron failed: %v", err)
	}
	if _, ok := s.cronEntries["enabled-cron"]; !ok {
		t.Fatal("enabled cron job not scheduled")
	}
	if err := s.UpsertJob(&CatalogJob{Name: "disabled-cron", SQLText: "SELECT 1", ScheduleType: "CRON", CronExpr: "*/30 * * * * *", Enabled: false}); err != nil {
		t.Fatalf("UpsertJob disabled-cron failed: %v", err)
	}
	if _, ok := s.cronEntries["disabled-cron"]; ok {
		t.Fatal("disabled cron job should not be scheduled")
	}
	if err := s.RemoveJob("enabled-cron"); err != nil {
		t.Fatalf("RemoveJob failed: %v", err)
	}
	if _, ok := s.cronEntries["enabled-cron"]; ok {
		t.Fatal("removed cron job still scheduled")
	}

	if err := db.Catalog().RegisterJob(&CatalogJob{Name: "startup", SQLText: "SELECT 1", ScheduleType: "INTERVAL", IntervalMs: 3600000, Enabled: true}); err != nil {
		t.Fatalf("RegisterJob startup failed: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("second Start failed: %v", err)
	}
	s.Stop()
	s.Stop()
}

func TestSchedulerCheckIntervalJobs(t *testing.T) {
	db := NewDB()
	exec := &schedulerTestExecutor{}
	s := NewScheduler(db, exec)

	now := time.Now()
	due := now.Add(-time.Second)
	future := now.Add(time.Hour)
	jobs := []*CatalogJob{
		{Name: "due", SQLText: "SELECT due", ScheduleType: "INTERVAL", IntervalMs: 1000, NextRunAt: &due, Enabled: true},
		{Name: "future", SQLText: "SELECT future", ScheduleType: "INTERVAL", IntervalMs: 1000, NextRunAt: &future, Enabled: true},
		{Name: "once", SQLText: "SELECT once", ScheduleType: "ONCE", RunAt: &due, NextRunAt: &due, Enabled: true},
		{Name: "cron", SQLText: "SELECT cron", ScheduleType: "CRON", NextRunAt: &due, Enabled: true},
		{Name: "unset", SQLText: "SELECT unset", ScheduleType: "INTERVAL", IntervalMs: 1000, Enabled: true},
	}
	for _, job := range jobs {
		if err := db.Catalog().RegisterJob(job); err != nil {
			t.Fatalf("RegisterJob %s failed: %v", job.Name, err)
		}
	}

	s.checkIntervalJobs(now)
	s.wg.Wait()

	if exec.callCount() != 2 {
		t.Fatalf("executor calls = %d, want 2", exec.callCount())
	}
	once, err := db.Catalog().GetJob("once")
	if err != nil {
		t.Fatalf("GetJob once failed: %v", err)
	}
	if once.Enabled {
		t.Fatal("ONCE job should be disabled after execution")
	}
}

func TestSchedulerCalculateNextRunCatchUpAndInvalidInterval(t *testing.T) {
	s := NewScheduler(NewDB(), nil)

	lastRun := time.Now().Add(-5 * time.Minute)
	catchUp := &CatalogJob{Name: "catchup", ScheduleType: "INTERVAL", IntervalMs: int64(time.Minute / time.Millisecond), LastRunAt: &lastRun, CatchUp: true}
	s.calculateNextRun(catchUp)
	if catchUp.NextRunAt == nil || catchUp.NextRunAt.Before(time.Now()) {
		t.Fatalf("catch-up NextRunAt = %v", catchUp.NextRunAt)
	}

	noCatchUp := &CatalogJob{Name: "nocatchup", ScheduleType: "INTERVAL", IntervalMs: int64(time.Minute / time.Millisecond), LastRunAt: &lastRun}
	s.calculateNextRun(noCatchUp)
	if noCatchUp.NextRunAt == nil || noCatchUp.NextRunAt.Before(time.Now()) {
		t.Fatalf("non catch-up NextRunAt = %v", noCatchUp.NextRunAt)
	}

	invalid := &CatalogJob{Name: "invalid", ScheduleType: "INTERVAL", IntervalMs: 0}
	s.calculateNextRun(invalid)
	if invalid.NextRunAt != nil {
		t.Fatalf("invalid interval should not set NextRunAt: %v", invalid.NextRunAt)
	}

	cronJob := &CatalogJob{Name: "cron-next", ScheduleType: "CRON", CronExpr: "*/10 * * * * *", Timezone: "UTC"}
	s.calculateNextRun(cronJob)
	if cronJob.NextRunAt == nil {
		t.Fatal("cron calculateNextRun did not set NextRunAt")
	}
}
