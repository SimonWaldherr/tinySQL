package storage

import (
	"context"
	"testing"
	"time"
)

type blockingSchedulerExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *blockingSchedulerExecutor) ExecuteSQL(context.Context, string) (interface{}, error) {
	close(e.started)
	<-e.release // deliberately ignore cancellation to exercise bounded shutdown
	return nil, nil
}

func TestSchedulerShutdownTimeoutAndRestart(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { db.Close() })
	executor := &blockingSchedulerExecutor{started: make(chan struct{}), release: make(chan struct{})}
	s := NewScheduler(db, executor)
	t.Cleanup(s.Stop)
	job := &CatalogJob{Name: "blocked", SQLText: "SELECT 1", ScheduleType: "ONCE"}
	if _, err := s.SubmitJob(job); err != nil {
		t.Fatal(err)
	}
	defer close(executor.release)
	<-executor.started
	s.mu.RLock()
	done := s.workDone
	s.mu.RUnlock()
	s.stopWithTimeout(time.Millisecond)
	select {
	case <-done:
		t.Fatal("timed-out shutdown reported unfinished work as complete")
	default:
	}
	if err := s.Start(); err == nil {
		t.Fatal("restart accepted an unfinished shutdown generation")
	}
	// Let the original worker exit before starting the next generation.
	executor.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("last worker did not signal completion after shutdown timed out")
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.pending != 0 || s.workers != 0 || len(s.active) != 0 {
		t.Fatalf("restart retained work: pending=%d, workers=%d, active=%d", s.pending, s.workers, len(s.active))
	}
}

func TestSchedulerCompletionGenerationIsRenewed(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { db.Close() })
	s := NewScheduler(db, nil)
	t.Cleanup(s.Stop)
	job := &CatalogJob{Name: "generation", SQLText: "SELECT 1", ScheduleType: "ONCE"}
	previous := s.workDone
	for i := 0; i < 3; i++ {
		if _, err := s.SubmitJob(job); err != nil {
			t.Fatal(err)
		}
		s.mu.RLock()
		done := s.workDone
		s.mu.RUnlock()
		if done == previous {
			t.Fatal("new work reused a completed generation")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("admitted job did not complete")
		}
		previous = done
	}
}
