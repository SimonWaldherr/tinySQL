package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type runCaptureExecutor struct {
	started chan JobRunInfo
	release chan struct{}
}

func (e *runCaptureExecutor) ExecuteSQL(ctx context.Context, _ string) (interface{}, error) {
	info, _ := JobRunFromContext(ctx)
	e.started <- info
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return nil, nil
	}
}
func TestSchedulerBoundedQueueAndOverlappingRunIDs(t *testing.T) {
	db := NewDB()
	defer db.Close()
	exec := &runCaptureExecutor{started: make(chan JobRunInfo, 4), release: make(chan struct{})}
	s := NewScheduler(db, exec)
	s.SetMaxConcurrentJobs(2)
	s.SetMaxQueuedJobs(1)
	defer s.Stop()
	job := &CatalogJob{Name: "same", SQLText: "SELECT 1", ScheduleType: "ONCE"}
	if err := db.Catalog().RegisterJob(job); err != nil {
		t.Fatal(err)
	}
	ids := make(map[int64]bool)
	for i := 0; i < 3; i++ {
		id, err := s.SubmitJob(job)
		if err != nil {
			t.Fatal(err)
		}
		if ids[id] {
			t.Fatal("duplicate run ID")
		}
		ids[id] = true
	}
	if _, err := s.SubmitJob(job); !errors.Is(err, ErrJobQueueFull) {
		t.Fatalf("overload: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case info := <-exec.started:
			if !ids[info.RunID] || info.IdempotencyKey == "" {
				t.Fatal(info)
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not start")
		}
	}
	stats := s.Stats()
	if stats.Running != 2 || stats.Queued != 1 || stats.Rejected != 1 {
		t.Fatal(stats)
	}
	if err := s.RemoveJob(job.Name); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("overlapping executions not all canceled")
	}
	for _, history := range db.Catalog().ListJobHistory() {
		if ids[history.RunID] && history.Status != "CANCELED" {
			t.Fatalf("run not canceled: %+v", history)
		}
	}
	if stats = s.Stats(); stats.Queued != 0 || stats.Running != 0 {
		t.Fatal(stats)
	}
}

type retryCaptureExecutor struct {
	mu    sync.Mutex
	infos []JobRunInfo
}

func (e *retryCaptureExecutor) ExecuteSQL(ctx context.Context, _ string) (interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	info, _ := JobRunFromContext(ctx)
	e.infos = append(e.infos, info)
	if len(e.infos) < 3 {
		return nil, errors.New("temporary failure")
	}
	return nil, nil
}
func TestSchedulerRetriesKeepExecutionIdentity(t *testing.T) {
	db := NewDB()
	defer db.Close()
	exec := &retryCaptureExecutor{}
	s := NewScheduler(db, exec)
	defer s.Stop()
	job := &CatalogJob{Name: "retry", SQLText: "SELECT 1", ScheduleType: "ONCE", MaxAttempts: 3, RetryDelayMs: 1}
	if err := db.Catalog().RegisterJob(job); err != nil {
		t.Fatal(err)
	}
	id, err := s.SubmitJob(job)
	if err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.infos) != 3 {
		t.Fatal(exec.infos)
	}
	for i, info := range exec.infos {
		if info.RunID != id || info.Attempt != i+1 || info.IdempotencyKey != exec.infos[0].IdempotencyKey {
			t.Fatal(info)
		}
	}
	history := db.Catalog().ListJobHistory()
	if len(history) != 1 || history[0].Attempts != 3 || history[0].Status != "SUCCEEDED" {
		t.Fatal(history)
	}
	if s.Stats().Retried != 2 {
		t.Fatal(s.Stats())
	}
}
