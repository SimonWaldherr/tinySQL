package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// ==================== Job Scheduler ====================
// Executes scheduled jobs based on CRON expressions or intervals

// Scheduler manages scheduled job execution
type Scheduler struct {
	totalQueueWait, lastStartDelay, maxStartDelay time.Duration
	db                                            *DB
	catalog                                       *CatalogManager
	cron                                          *cron.Cron
	mu                                            sync.RWMutex
	running                                       map[string]*jobExecution // Track currently running jobs
	cronEntries                                   map[string]cron.EntryID
	stopCh                                        chan struct{}
	started                                       bool
	wg                                            sync.WaitGroup
	executor                                      JobExecutor // Interface for executing SQL

	lifeMu                         sync.Mutex
	active                         map[int64]*jobTask
	queue                          []*jobTask
	workers, maxWorkers, maxQueued int
	stopped                        bool
	instanceID                     string
	rejected, retried, completed   uint64
}

const defaultMaxConcurrentJobs = 8

// SetMaxConcurrentJobs adjusts the worker limit. Existing executions finish
// normally when the limit is lowered; newly queued work obeys the new limit.
func (s *Scheduler) SetMaxConcurrentJobs(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxWorkers = n
	s.launchQueuedLocked()
}

// SetMaxQueuedJobs bounds waiting jobs. Existing queued work is retained when
// lowering the limit; new work is rejected until there is room. Zero disables waiting.
func (s *Scheduler) SetMaxQueuedJobs(n int) {
	if n < 0 {
		return
	}
	s.mu.Lock()
	s.maxQueued = n
	s.mu.Unlock()
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
		active:      make(map[int64]*jobTask),
		maxWorkers:  defaultMaxConcurrentJobs, maxQueued: 256, instanceID: uuid.NewString(),
	}
}

// Start begins the scheduler loop
func (s *Scheduler) Start() error {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}

	if s.stopped && s.workers > 0 {
		return errors.New("scheduler is still stopping")
	}
	s.stopped = false

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
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.started = false
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
	for _, task := range s.active {
		task.exec.cancelFn()
	}
	// Retain queued jobs for workers to record cancellation without executing SQL.
	s.mu.Unlock()
	<-s.cron.Stop().Done()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	timer := time.NewTimer(schedulerShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.Print("job scheduler shutdown timed out")
	}
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
		s.calculateNextRun(job)
		s.catalog.setJobNextRun(job.Name, job.NextRunAt)
		return nil
	case "ONCE":
		// Handled by interval scheduler
		if job.RunAt != nil {
			job.NextRunAt = job.RunAt
		}
		s.catalog.setJobNextRun(job.Name, job.NextRunAt)
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

	schedule, err := parseJobCronSchedule(job)
	if err != nil {
		return err
	}
	nextRun := schedule.Next(time.Now())
	job.NextRunAt = &nextRun
	s.catalog.setJobNextRun(job.Name, job.NextRunAt)
	// Register the already parsed schedule, including its timezone. AddFunc
	// would parse again using the scheduler's default location (UTC).
	s.cronEntries[job.Name] = s.cron.Schedule(schedule, cron.FuncJob(func() {
		s.executeJob(job)
	}))
	return nil
}

func parseJobCronSchedule(job *CatalogJob) (cron.Schedule, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	expr := strings.TrimSpace(job.CronExpr)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid CRON expression %q: %w", job.CronExpr, err)
	}
	if spec, ok := schedule.(*cron.SpecSchedule); ok && !strings.HasPrefix(expr, "TZ=") && !strings.HasPrefix(expr, "CRON_TZ=") {
		loc := time.UTC
		if job.Timezone != "" {
			loc, err = time.LoadLocation(job.Timezone)
			if err != nil {
				log.Printf("Invalid timezone %q for job %q, using UTC", job.Timezone, job.Name)
				loc = time.UTC
			}
		}
		spec.Location = loc
	}
	return schedule, nil
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
			if _, err := s.SubmitJob(job); err != nil {
				continue
			}

			if job.ScheduleType == "INTERVAL" {
				s.calculateNextRun(job)
				s.catalog.setJobNextRun(job.Name, job.NextRunAt)
			}

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

// executeJob is the fire-and-forget scheduler entry point. Overload is visible
// in Stats and history; SubmitJob exposes the rejection directly to callers.
func (s *Scheduler) executeJob(job *CatalogJob) {
	_, _ = s.SubmitJob(job)
}

// calculateNextRun computes the next execution time based on schedule type
func (s *Scheduler) calculateNextRun(job *CatalogJob) {
	now := time.Now()

	switch job.ScheduleType {
	case "INTERVAL":
		if job.IntervalMs <= 0 || job.IntervalMs > int64((1<<63-1)/time.Millisecond) {
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
			nextRun := nextIntervalRun(*job.LastRunAt, now, interval)
			job.NextRunAt = &nextRun
		} else {
			// Schedule from now
			nextRun := now.Add(interval)
			job.NextRunAt = &nextRun
		}

	case "CRON":
		if schedule, err := parseJobCronSchedule(job); err == nil {
			nextRun := schedule.Next(now)
			job.NextRunAt = &nextRun
		}

	case "ONCE":
		// Already set during registration
	}
}

// nextIntervalRun skips missed ticks arithmetically while preserving the
// original cadence and inclusive deadline. This takes at most two steps for
// spans within time.Duration's range, with additional steps for longer spans.
func nextIntervalRun(last, now time.Time, interval time.Duration) time.Time {
	next := last.Add(interval)
	for next.Before(now) {
		steps := now.Sub(next) / interval
		if steps == 0 {
			steps = 1
		}
		next = next.Add(steps * interval)
	}
	return next
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
	copy := cloneScheduledJob(job)
	return s.scheduleJob(&copy)
}

// RemoveJob unregisters a job and stops its execution
func (s *Scheduler) RemoveJob(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unscheduleJobLocked(name)

	for _, task := range s.active {
		if task.job.Name == name {
			task.exec.cancelFn()
		}
	}
	if exec, ok := s.running[name]; ok {
		exec.cancelFn()
	}

	return s.catalog.DeleteJob(name)
}
