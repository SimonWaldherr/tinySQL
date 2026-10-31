package tinysql

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// JobExecutor executes SQL text for scheduled jobs.
type JobExecutor = storage.JobExecutor

// Scheduler manages scheduled job execution.
type Scheduler = storage.Scheduler

// CatalogJob describes a scheduled job.
type CatalogJob = storage.CatalogJob

// CatalogJobHistory records a scheduled job run.
type CatalogJobHistory = storage.CatalogJobHistory

// SQLJobExecutor executes scheduled job SQL against a tinySQL database tenant.
type SQLJobExecutor struct {
	DB     *DB
	Tenant string
}

// NewSQLJobExecutor creates a scheduler executor backed by tinySQL itself.
func NewSQLJobExecutor(db *DB, tenant string) *SQLJobExecutor {
	if tenant == "" {
		tenant = "default"
	}
	return &SQLJobExecutor{DB: db, Tenant: tenant}
}

// ExecuteSQL parses and executes one SQL statement for the scheduler.
func (e *SQLJobExecutor) ExecuteSQL(ctx context.Context, sql string) (interface{}, error) {
	if e == nil || e.DB == nil {
		return nil, fmt.Errorf("nil SQL job executor")
	}
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return Execute(ctx, e.DB, e.Tenant, stmt)
}

// StartJobScheduler starts the database job scheduler for a tenant.
func StartJobScheduler(db *DB, tenant string) error {
	if db == nil {
		return fmt.Errorf("nil DB")
	}
	return db.StartJobScheduler(NewSQLJobExecutor(db, tenant))
}

// RestartJobScheduler restarts the database job scheduler for a tenant.
func RestartJobScheduler(db *DB, tenant string) error {
	if db == nil {
		return fmt.Errorf("nil DB")
	}
	return db.RestartJobScheduler(NewSQLJobExecutor(db, tenant))
}

// StopJobScheduler stops the database job scheduler.
func StopJobScheduler(db *DB) {
	if db == nil {
		return
	}
	db.StopJobScheduler()
}

type SchedulerStats = storage.SchedulerStats

type JobRunInfo = storage.JobRunInfo

var ErrJobQueueFull = storage.ErrJobQueueFull

var ErrSchedulerStopped = storage.ErrSchedulerStopped

var ErrJobAlreadyRunning = storage.ErrJobAlreadyRunning

func JobRunFromContext(ctx context.Context) (JobRunInfo, bool) { return storage.JobRunFromContext(ctx) }
