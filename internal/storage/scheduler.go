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
	db         *DB
	catalog    *CatalogManager
	cron       *cron.Cron
	mu         sync.RWMutex
	running    map[string]*jobExecution // Track currently running jobs
	stopCh     chan struct{}
	executor   JobExecutor // Interface for executing SQL
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
		db:       db,
		catalog:  db.Catalog(),
		cron:     cron.New(cron.WithLocation(loc), cron.WithSeconds()),
		running:  make(map[string]*jobExecution),
		stopCh:   make(chan struct{}),
		executor: executor,
	}
}

// Start begins the scheduler loop
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Register all enabled jobs
	jobs := s.catalog.ListEnabledJobs()
	for _, job := range jobs {
		if err := s.scheduleJob(job); err != nil {
			log.Printf("Failed to schedule job %q: %v", job.Name, err)
		}
	}

	// Start cron scheduler
	s.cron.Start()

	// Start interval/once scheduler in goroutine
	go s.runIntervalScheduler()

	log.Printf("Job scheduler started with %d jobs", len(jobs))
	return nil
}

// Stop halts the scheduler and cancels all running jobs
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop cron
	ctx := s.cron.Stop()
	<-ctx.Done()

	// Signal interval scheduler to stop
	close(s.stopCh)

	// Cancel all running jobs
	for name, exec := range s.running {
		log.Printf("Canceling running job %q", name)
		exec.cancelFn()
	}

	log.Println("Job scheduler stopped")
}

// scheduleJob registers a job with the appropriate scheduler
func (s *Scheduler) scheduleJob(job *CatalogJob) error {
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
	_, err = s.cron.AddFunc(job.CronExpr, func() {
		s.executeJob(job)
	})

	return err
}

// runIntervalScheduler handles INTERVAL and ONCE jobs
func (s *Scheduler) runIntervalScheduler() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
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
	s.mu.Lock()

	// Check no_overlap flag
	if job.NoOverlap {
		if _, isRunning := s.running[job.Name]; isRunning {
			s.mu.Unlock()
			log.Printf("Job %q already running, skipping (no_overlap=true)", job.Name)
			return
		}
	}

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
	s.running[job.Name] = exec
	s.mu.Unlock()

	// Execute job in goroutine
	go func() {
		defer func() {
			cancel()
			s.mu.Lock()
			delete(s.running, job.Name)
			s.mu.Unlock()

			// Update last_run_at and calculate next_run_at
			lastRun := exec.startTime
			s.calculateNextRun(job)
			if err := s.catalog.UpdateJobRuntime(job.Name, lastRun, *job.NextRunAt); err != nil {
				log.Printf("Failed to update job runtime for %q: %v", job.Name, err)
			}
		}()

		log.Printf("Executing job %q", job.Name)

		// Execute SQL through executor interface
		if s.executor != nil {
			if _, err := s.executor.ExecuteSQL(ctx, job.SQLText); err != nil {
				log.Printf("Job %q failed: %v", job.Name, err)
			} else {
				log.Printf("Job %q completed successfully", job.Name)
			}
		} else {
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
	if err := s.catalog.RegisterJob(job); err != nil {
		return err
	}

	if job.Enabled {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.scheduleJob(job)
	}

	return nil
}

// RemoveJob unregisters a job and stops its execution
func (s *Scheduler) RemoveJob(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel if running
	if exec, ok := s.running[name]; ok {
		exec.cancelFn()
		delete(s.running, name)
	}

	return s.catalog.DeleteJob(name)
}
