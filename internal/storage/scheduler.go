package storage

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// ==================== Job Scheduler ====================
// Executes scheduled jobs based on CRON expressions or intervals

// Scheduler manages scheduled job execution
type Scheduler struct {
	db          *DB
	catalog     *CatalogManager
	cron        *cron.Cron
	mu          sync.RWMutex
	running     map[string]*jobExecution // Track currently running jobs
	cronEntries map[string]cron.EntryID
	stopCh      chan struct{}
	started     bool
	wg          sync.WaitGroup
	executor    JobExecutor // Interface for executing SQL
}

// JobExecutor interface allows the scheduler to execute SQL without circular dependencies
type JobExecutor interface {
	ExecuteSQL(ctx context.Context, sql string) (interface{}, error)
}

// jobExecution tracks a running job instance
type jobExecution struct {
	startTime time.Time
	cancelFn  context.CancelFunc
}

// NewScheduler creates a new job scheduler
func NewScheduler(db *DB, executor JobExecutor) *Scheduler {
	loc, _ := time.LoadLocation("UTC")
	return &Scheduler{
		db:          db,
		catalog:     db.Catalog(),
		cron:        cron.New(cron.WithLocation(loc), cron.WithSeconds()),
		running:     make(map[string]*jobExecution),
		cronEntries: make(map[string]cron.EntryID),
		stopCh:      make(chan struct{}),
		executor:    executor,
	}
}

// Start begins the scheduler loop
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}

	// Register all enabled jobs
	jobs := s.catalog.ListEnabledJobs()
	for _, job := range jobs {
		if err := s.scheduleJob(job); err != nil {
			log.Printf("Failed to schedule job %q: %v", job.Name, err)
		}
	}

	// Start cron scheduler
	s.cron.Start()

	// Capture the stop channel under the lock so the goroutine never reads
	// s.stopCh directly; Stop() may write s.stopCh = nil concurrently.
	stopCh := s.stopCh
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runIntervalScheduler(stopCh)
	}()
	s.started = true

	log.Printf("Job scheduler started with %d jobs", len(jobs))
	return nil
}

// Stop halts the scheduler and cancels all running jobs
func (s *Scheduler) Stop() {
	wasStarted, stopCh := func() (bool, chan struct{}) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.started {
			return false, nil
		}
		s.started = false
		ch := s.stopCh
		s.stopCh = nil
		return true, ch
	}()
	if !wasStarted {
		return
	}
	if stopCh != nil {
		close(stopCh)
	}

	// Stop cron
	ctx := s.cron.Stop()
	<-ctx.Done()

	s.mu.RLock()
	running := make(map[string]*jobExecution, len(s.running))
	for name, exec := range s.running {
		running[name] = exec
	}
	s.mu.RUnlock()

	// Cancel all running jobs
	for name, exec := range running {
		log.Printf("Canceling running job %q", name)
		exec.cancelFn()
	}

	// s.wg.Wait() has no timeout of its own: a job that ignores context
	// cancellation (e.g. blocked on I/O the job's SQL doesn't check ctx
	// inside) would hang here forever, and since Stop is called from
	// DB.Close, that means a stuck job could hang the whole shutdown path
	// indefinitely. Bound the wait so shutdown always completes.
	waitDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(schedulerShutdownTimeout):
		log.Printf("job scheduler: %d job(s) did not finish within %s of Stop being called; continuing shutdown anyway", len(running), schedulerShutdownTimeout)
	}

	log.Println("Job scheduler stopped")
}

// schedulerShutdownTimeout bounds how long Stop waits for already-running
// jobs to exit after their context is canceled, so a misbehaving job (one
// that doesn't check ctx.Done()) can't hang the whole DB.Close() path.
const schedulerShutdownTimeout = 30 * time.Second

// scheduleJob registers a job with the appropriate scheduler
func (s *Scheduler) scheduleJob(job *CatalogJob) error {
	s.unscheduleJobLocked(job.Name)
	switch job.ScheduleType {
	case "CRON":
		return s.scheduleCronJob(job)
	case "INTERVAL":
		// Handled by interval scheduler
		s.calculateNextRun(job)
		return nil
	case "ONCE":
		// Handled by interval scheduler
		if job.RunAt != nil {
			job.NextRunAt = job.RunAt
		}
		return nil
	default:
		return fmt.Errorf("unknown schedule type: %s", job.ScheduleType)
	}
}

// scheduleCronJob registers a CRON-based job
func (s *Scheduler) scheduleCronJob(job *CatalogJob) error {
	if job.CronExpr == "" {
		return fmt.Errorf("CRON expression empty for job %q", job.Name)
	}

	// Parse timezone
	loc := time.UTC
	if job.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(job.Timezone)
		if err != nil {
			log.Printf("Invalid timezone %q for job %q, using UTC", job.Timezone, job.Name)
			loc = time.UTC
		}
	}

	// Create a wrapped scheduler with location
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(job.CronExpr)
	if err != nil {
		return fmt.Errorf("invalid CRON expression %q: %w", job.CronExpr, err)
	}

	// Calculate next run
	nextRun := schedule.Next(time.Now().In(loc))
	job.NextRunAt = &nextRun

	// Register with cron
	id, err := s.cron.AddFunc(job.CronExpr, func() {
		s.executeJob(job)
	})
	if err == nil {
		s.cronEntries[job.Name] = id
	}

	return err
}

func (s *Scheduler) unscheduleJobLocked(name string) {
	if id, ok := s.cronEntries[name]; ok {
		s.cron.Remove(id)
		delete(s.cronEntries, name)
	}
}

// runIntervalScheduler handles INTERVAL and ONCE jobs.
// stopCh is passed by value (captured under the scheduler lock in Start) so
// this goroutine never reads the s.stopCh field, avoiding a data race with
// Stop() which writes s.stopCh = nil while holding s.mu.
func (s *Scheduler) runIntervalScheduler(stopCh <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case now := <-ticker.C:
			s.checkIntervalJobs(now)
		}
	}
}

// checkIntervalJobs checks if any INTERVAL or ONCE jobs need to run
func (s *Scheduler) checkIntervalJobs(now time.Time) {
	jobs := s.catalog.ListEnabledJobs()
	for _, job := range jobs {
		if job.ScheduleType != "INTERVAL" && job.ScheduleType != "ONCE" {
			continue
		}

		if job.NextRunAt == nil {
			continue
		}

		if now.After(*job.NextRunAt) || now.Equal(*job.NextRunAt) {
			s.executeJob(job)

			// For ONCE jobs, disable after execution
			if job.ScheduleType == "ONCE" {
				job.Enabled = false
				if err := s.catalog.RegisterJob(job); err != nil {
					log.Printf("Failed to disable ONCE job %q: %v", job.Name, err)
				}
			}
		}
	}
}

// executeJob runs a job's SQL with proper concurrency control
func (s *Scheduler) executeJob(job *CatalogJob) {
	// Create execution context with timeout
	timeout := time.Duration(job.MaxRuntimeMs) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Minute // Default 5 minutes
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	exec := &jobExecution{
		startTime: time.Now(),
		cancelFn:  cancel,
	}

	// Check no_overlap flag and register as running atomically under one lock.
	skip := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if job.NoOverlap {
			if _, isRunning := s.running[job.Name]; isRunning {
				return true
			}
		}
		s.running[job.Name] = exec
		s.wg.Add(1)
		return false
	}()

	if skip {
		cancel()
		log.Printf("Job %q already running, skipping (no_overlap=true)", job.Name)
		now := time.Now()
		_ = s.catalog.AddJobHistory(&CatalogJobHistory{
			JobName:    job.Name,
			StartedAt:  now,
			FinishedAt: now,
			Status:     "SKIPPED",
		})
		return
	}

	// Execute job in goroutine
	go func() {
		defer s.wg.Done()
		status := "SUCCEEDED"
		errMsg := ""
		defer func() {
			ctxErr := ctx.Err()
			cancel()
			s.mu.Lock()
			delete(s.running, job.Name)
			s.mu.Unlock()

			// Update last_run_at and calculate next_run_at
			lastRun := exec.startTime
			s.calculateNextRun(job)
			if err := s.catalog.UpdateJobRuntimePtr(job.Name, lastRun, job.NextRunAt); err != nil {
				log.Printf("Failed to update job runtime for %q: %v", job.Name, err)
			}
			finishedAt := time.Now()
			if ctxErr != nil && status == "SUCCEEDED" {
				status = "CANCELED"
				errMsg = ctxErr.Error()
			}
			if err := s.catalog.AddJobHistory(&CatalogJobHistory{
				JobName:      job.Name,
				StartedAt:    exec.startTime,
				FinishedAt:   finishedAt,
				DurationMs:   finishedAt.Sub(exec.startTime).Milliseconds(),
				Status:       status,
				ErrorMessage: errMsg,
			}); err != nil {
				log.Printf("Failed to add job history for %q: %v", job.Name, err)
			}
		}()

		// Recover from a panic inside job execution (e.g. a parser bug
		// triggered by the job's own stored SQL text) so one bad job can't
		// take down the whole process; mirrors executeStatement's
		// panic-to-error conversion in internal/engine/exec_statement.go,
		// but records the panic as a FAILED job run instead of returning it
		// as an error to a caller.
		defer func() {
			if r := recover(); r != nil {
				status = "FAILED"
				errMsg = fmt.Sprintf("panic executing job: %v", r)
				log.Printf("Job %q panicked: %v", job.Name, r)
			}
		}()

		log.Printf("Executing job %q", job.Name)

		// Execute SQL through executor interface
		if s.executor != nil {
			if _, err := s.executor.ExecuteSQL(ctx, job.SQLText); err != nil {
				status = "FAILED"
				errMsg = err.Error()
				log.Printf("Job %q failed: %v", job.Name, err)
			} else {
				log.Printf("Job %q completed successfully", job.Name)
			}
		} else {
			status = "SKIPPED"
			errMsg = "no executor configured"
			log.Printf("Job %q skipped (no executor configured)", job.Name)
		}
	}()
}

// calculateNextRun computes the next execution time based on schedule type
func (s *Scheduler) calculateNextRun(job *CatalogJob) {
	now := time.Now()

	switch job.ScheduleType {
	case "INTERVAL":
		if job.IntervalMs <= 0 {
			log.Printf("Invalid interval for job %q", job.Name)
			return
		}

		interval := time.Duration(job.IntervalMs) * time.Millisecond

		if job.LastRunAt == nil {
			// First run
			nextRun := now.Add(interval)
			job.NextRunAt = &nextRun
		} else if job.CatchUp {
			// Catch up missed runs
			nextRun := job.LastRunAt.Add(interval)
			for nextRun.Before(now) {
				nextRun = nextRun.Add(interval)
			}
			job.NextRunAt = &nextRun
		} else {
			// Schedule from now
			nextRun := now.Add(interval)
			job.NextRunAt = &nextRun
		}

	case "CRON":
		// CRON scheduling handled by cron library
		// This is called after execution to log next run
		if job.CronExpr != "" {
			parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
			if schedule, err := parser.Parse(job.CronExpr); err == nil {
				loc := time.UTC
				if job.Timezone != "" {
					if l, err := time.LoadLocation(job.Timezone); err == nil {
						loc = l
					}
				}
				nextRun := schedule.Next(now.In(loc))
				job.NextRunAt = &nextRun
			}
		}

	case "ONCE":
		// Already set during registration
	}
}

// AddJob registers a new job and schedules it immediately if enabled
func (s *Scheduler) AddJob(job *CatalogJob) error {
	return s.UpsertJob(job)
}

// UpsertJob registers or updates a job and refreshes its live schedule.
func (s *Scheduler) UpsertJob(job *CatalogJob) error {
	if err := s.catalog.RegisterJob(job); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.unscheduleJobLocked(job.Name)
	if !job.Enabled {
		return nil
	}
	return s.scheduleJob(job)
}

// RemoveJob unregisters a job and stops its execution
func (s *Scheduler) RemoveJob(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unscheduleJobLocked(name)

	// Cancel if running
	if exec, ok := s.running[name]; ok {
		exec.cancelFn()
		delete(s.running, name)
	}

	return s.catalog.DeleteJob(name)
}
