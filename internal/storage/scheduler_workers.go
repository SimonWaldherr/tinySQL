package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"
)

var ErrJobQueueFull = errors.New("job queue is full")
var ErrSchedulerStopped = errors.New("scheduler is stopped")
var ErrJobAlreadyRunning = errors.New("job already running")

type JobRunInfo struct {
	RunID          int64
	IdempotencyKey string
	Attempt        int
}
type jobRunContextKey struct{}

// JobRunFromContext supplies a stable key across retries for external deduplication.
func JobRunFromContext(ctx context.Context) (JobRunInfo, bool) {
	info, ok := ctx.Value(jobRunContextKey{}).(JobRunInfo)
	return info, ok
}

type jobTask struct {
	job                 CatalogJob
	exec                *jobExecution
	ctx                 context.Context
	info                JobRunInfo
	queuedAt, startedAt time.Time
}

type SchedulerStats struct {
	TotalQueueWait, LastStartDelay, MaxStartDelay time.Duration
	Running, Queued, WorkerLimit, QueueLimit      int
	OldestQueuedAge                               time.Duration
	Rejected, Retried, Completed                  uint64
}

func (s *Scheduler) Stats() SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := SchedulerStats{TotalQueueWait: s.totalQueueWait, LastStartDelay: s.lastStartDelay, MaxStartDelay: s.maxStartDelay, Queued: len(s.queue), Running: len(s.active) - len(s.queue), WorkerLimit: s.maxWorkers, QueueLimit: s.maxQueued, Rejected: s.rejected, Retried: s.retried, Completed: s.completed}
	if len(s.queue) > 0 {
		out.OldestQueuedAge = time.Since(s.queue[0].queuedAt)
	}
	return out
}

// SubmitJob admits a copied job into bounded in-memory work storage. Successful
// admission is not a durability acknowledgement. Run history records completion.
func (s *Scheduler) SubmitJob(job *CatalogJob) (int64, error) {
	if job == nil || job.Name == "" {
		return 0, errors.New("job name is required")
	}
	// Own the definition so retries and catalog edits cannot mutate this run.
	copy := cloneScheduledJob(job)
	s.mu.Lock()
	rejection := error(nil)
	if s.stopped {
		rejection = ErrSchedulerStopped
	} else if copy.NoOverlap && s.running[copy.Name] != nil {
		rejection = ErrJobAlreadyRunning
	} else if s.workers >= s.maxWorkers && len(s.queue) >= s.maxQueued {
		rejection = ErrJobQueueFull
	}
	if rejection != nil {
		s.rejected++
		s.mu.Unlock()
		if errors.Is(rejection, ErrSchedulerStopped) {
			return 0, rejection
		}
		status := "REJECTED"
		if errors.Is(rejection, ErrJobAlreadyRunning) {
			status = "SKIPPED"
		}
		now := time.Now()
		_ = s.catalog.AddJobHistory(&CatalogJobHistory{JobName: copy.Name, StartedAt: now, FinishedAt: now, Status: status, ErrorMessage: rejection.Error()})
		s.db.notifyGlobalChanges()
		return 0, rejection
	}
	timeout := time.Duration(copy.MaxRuntimeMs) * time.Millisecond
	if copy.MaxRuntimeMs <= 0 || copy.MaxRuntimeMs > int64((1<<63-1)/time.Millisecond) {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	id := s.catalog.ReserveJobRunID()
	task := &jobTask{job: copy, ctx: ctx, exec: &jobExecution{startTime: time.Now(), cancelFn: cancel}, queuedAt: time.Now(), info: JobRunInfo{RunID: id, IdempotencyKey: fmt.Sprintf("%s:%d", s.instanceID, id)}}
	s.active[id] = task
	s.running[copy.Name] = task.exec
	s.wg.Add(1)
	if s.workers < s.maxWorkers {
		s.workers++
		go s.runWorker(task)
	} else {
		s.queue = append(s.queue, task)
	}
	s.mu.Unlock()
	return id, nil
}

func (s *Scheduler) launchQueuedLocked() {
	for len(s.queue) > 0 && s.workers < s.maxWorkers {
		task := s.popQueuedLocked()
		s.workers++
		go s.runWorker(task)
	}
}
func (s *Scheduler) popQueuedLocked() *jobTask {
	task := s.queue[0]
	s.queue[0] = nil
	s.queue = s.queue[1:]
	if len(s.queue) == 0 {
		s.queue = nil
	}
	return task
}
func (s *Scheduler) runWorker(task *jobTask) {
	for {
		s.runTask(task)
		s.mu.Lock()
		delete(s.active, task.info.RunID)
		if s.running[task.job.Name] == task.exec {
			delete(s.running, task.job.Name)
			for _, other := range s.active {
				if other.job.Name == task.job.Name {
					s.running[task.job.Name] = other.exec
					break
				}
			}
		}
		s.completed++
		s.wg.Done()
		if len(s.queue) == 0 || s.workers > s.maxWorkers {
			s.workers--
			s.mu.Unlock()
			return
		}
		task = s.popQueuedLocked()
		s.mu.Unlock()
	}
}

func (s *Scheduler) runTask(task *jobTask) {
	defer s.db.notifyGlobalChanges()
	defer task.exec.cancelFn()
	task.startedAt = time.Now()
	delay := time.Duration(0)
	if task.job.NextRunAt != nil && task.startedAt.After(*task.job.NextRunAt) {
		delay = task.startedAt.Sub(*task.job.NextRunAt)
	}
	s.mu.Lock()
	s.totalQueueWait += task.startedAt.Sub(task.queuedAt)
	s.lastStartDelay = delay
	if delay > s.maxStartDelay {
		s.maxStartDelay = delay
	}
	s.mu.Unlock()
	status := "SUCCEEDED"
	message := ""
	attempts := 0
	maxAttempts := task.job.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if maxAttempts > 100 {
		maxAttempts = 100
	}
	for attempts < maxAttempts {
		if err := task.ctx.Err(); err != nil {
			status = "CANCELED"
			message = err.Error()
			break
		}
		attempts++
		task.info.Attempt = attempts
		if s.executor == nil {
			status = "SKIPPED"
			message = "no executor configured"
			break
		}
		err := s.executeAttempt(task)
		if err == nil {
			break
		}
		status = "FAILED"
		message = err.Error()
		if task.ctx.Err() != nil {
			status = "CANCELED"
			break
		}
		if attempts == maxAttempts {
			break
		}
		s.mu.Lock()
		s.retried++
		s.mu.Unlock()
		delay := jobRetryDelay(task.job.RetryDelayMs, attempts)
		timer := time.NewTimer(delay)
		select {
		case <-task.ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		status = "SUCCEEDED"
		message = ""
	}
	if err := task.ctx.Err(); err != nil {
		status = "CANCELED"
		message = err.Error()
	}
	job := task.job
	job.LastRunAt = &task.startedAt
	s.calculateNextRun(&job)
	if job.ScheduleType == "ONCE" {
		job.NextRunAt = nil
	}
	s.catalog.completeScheduledJob(&task.job, task.startedAt, job.NextRunAt)
	finished := time.Now()
	_ = s.catalog.AddJobHistory(&CatalogJobHistory{RunID: task.info.RunID, JobName: job.Name, IdempotencyKey: task.info.IdempotencyKey, Attempts: attempts, QueuedAt: task.queuedAt, QueueWaitMs: task.startedAt.Sub(task.queuedAt).Milliseconds(), StartDelayMs: delay.Milliseconds(), StartedAt: task.startedAt, FinishedAt: finished, DurationMs: finished.Sub(task.startedAt).Milliseconds(), Status: status, ErrorMessage: message})
}
func (s *Scheduler) executeAttempt(task *jobTask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic executing job: %v", r)
		}
	}()
	_, err = s.executor.ExecuteSQL(context.WithValue(task.ctx, jobRunContextKey{}, task.info), task.job.SQLText)
	return err
}
func jobRetryDelay(ms int64, attempt int) time.Duration {
	if ms <= 0 {
		ms = 100
	}
	if ms > 30000 {
		ms = 30000
	}
	delay := time.Duration(ms) * time.Millisecond
	for i := 1; i < attempt && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
}

// RetryJob manually starts a new run of a configured job. Its new idempotency
// key distinguishes it from automatic retries of a prior execution.
func (s *Scheduler) RetryJob(name string) (int64, error) {
	job, err := s.catalog.GetJob(name)
	if err != nil {
		return 0, err
	}
	return s.SubmitJob(job)
}

func (c *CatalogManager) setJobNextRun(name string, next *time.Time) {
	c.lockWrite()
	defer c.unlockWrite()
	if job := c.jobs[name]; job != nil {
		if next == nil {
			job.NextRunAt = nil
		} else {
			v := *next
			job.NextRunAt = &v
		}
	}
}
func cloneScheduledJob(job *CatalogJob) CatalogJob {
	out := *job
	if job.LastRunAt != nil {
		v := *job.LastRunAt
		out.LastRunAt = &v
	}
	if job.NextRunAt != nil {
		v := *job.NextRunAt
		out.NextRunAt = &v
	}
	if job.RunAt != nil {
		v := *job.RunAt
		out.RunAt = &v
	}
	return out
}

func (c *CatalogManager) completeScheduledJob(original *CatalogJob, last time.Time, next *time.Time) {
	c.lockWrite()
	defer c.unlockWrite()
	job := c.jobs[original.Name]
	if job == nil {
		return
	}
	if job.LastRunAt != nil && job.LastRunAt.After(last) {
		return
	}
	a, b := *original, *job
	a.LastRunAt = nil
	a.NextRunAt = nil
	a.CreatedAt = time.Time{}
	a.UpdatedAt = time.Time{}
	a.Enabled = false
	b.LastRunAt = nil
	b.NextRunAt = nil
	b.CreatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	b.Enabled = false
	job.LastRunAt = &last
	job.UpdatedAt = time.Now()
	// An old execution must not overwrite a newly edited schedule.
	if reflect.DeepEqual(a, b) {
		if next == nil {
			job.NextRunAt = nil
		} else {
			v := *next
			job.NextRunAt = &v
		}
	}
}
