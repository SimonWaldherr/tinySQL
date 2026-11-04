package tinysql

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Permission enumerates RBAC operations that can be granted to roles.
type Permission = storage.Permission

// Grant authorizes a Permission on a schema/table pair, with "*" as wildcard.
type Grant = storage.Grant

// CatalogRole is a named RBAC role and its grants.
type CatalogRole = storage.CatalogRole

// CatalogUser is an RBAC user principal. PasswordHash is opaque.
type CatalogUser = storage.CatalogUser

// CatalogManager stores schema, RBAC, function, trigger, view, and job metadata.
type CatalogManager = storage.CatalogManager

// CatalogTable describes a table registered in the catalog.
type CatalogTable = storage.CatalogTable

// CatalogColumn describes a table column registered in the catalog.
type CatalogColumn = storage.CatalogColumn

// CatalogView describes a logical SQL view.
type CatalogView = storage.CatalogView

// CatalogMaterializedView describes a cached materialized view.
type CatalogMaterializedView = storage.CatalogMaterializedView

// CatalogDependency records a dependency between catalog objects.
type CatalogDependency = storage.CatalogDependency

// CatalogFunction describes a scalar, aggregate, or table function.
type CatalogFunction = storage.CatalogFunction

// CatalogTrigger describes a trigger registered in the catalog.
type CatalogTrigger = storage.CatalogTrigger

const (
	PermSelect Permission = storage.PermSelect
	PermInsert Permission = storage.PermInsert
	PermUpdate Permission = storage.PermUpdate
	PermDelete Permission = storage.PermDelete
	PermDDL    Permission = storage.PermDDL
	PermAll    Permission = storage.PermAll
)

// WithUser returns a context carrying the acting username for RBAC
// permission checks (see CREATE USER/CREATE ROLE/GRANT below). Pass the
// result to Execute/ExecuteCompiled in place of a plain context.
//
// RBAC is opt-in: until the first CREATE USER statement runs against a
// database, every Execute call is permitted regardless of context — so
// existing code that never touches users/roles sees no behavior change.
// Once a user exists, every subsequent Execute call requires one via
// WithUser, or it's rejected with an "access denied" error.
//
// Example:
//
//	tinysql.Execute(ctx, db, "default", mustParse(`CREATE ROLE admin_role`))
//	tinysql.Execute(ctx, db, "default", mustParse(`GRANT ALL ON * TO ROLE admin_role`))
//	tinysql.Execute(ctx, db, "default", mustParse(`CREATE USER admin WITH PASSWORD 'x' ROLE admin_role`))
//	// RBAC is now active; every further call needs an authenticated context:
//	adminCtx := tinysql.WithUser(ctx, "admin")
//	tinysql.Execute(adminCtx, db, "default", stmt)
func WithUser(ctx context.Context, username string) context.Context {
	return engine.WithUser(ctx, username)
}

// UserFromContext returns the username set by WithUser, if any.
func UserFromContext(ctx context.Context) (string, bool) {
	return engine.UserFromContext(ctx)
}

// ParsePermission validates and normalizes an RBAC permission keyword.
func ParsePermission(s string) (Permission, error) {
	return storage.ParsePermission(s)
}

// --- Job scheduler ---

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

// --- Query subscriptions ---

type TableRef = storage.TableRef

type ChangeWatch = storage.ChangeWatch

type ChangeWatchStats = storage.ChangeWatchStats

type QueryChange = engine.QueryChange

type QuerySubscription = engine.QuerySubscription

type SubscriptionStats = engine.SubscriptionStats

type SubscriptionOptions = engine.SubscriptionOptions

// SubscribeSQL delivers an initial result and coalesced result deltas for a
// SELECT, including views, joins and GROUP BY. Complex queries are re-executed
// on database changes; result deltas are unordered. Close when done.
func SubscribeSQL(ctx context.Context, db *DB, tenant, query string) (*QuerySubscription, error) {
	return engine.SubscribeSQL(ctx, db, tenant, query)
}

// SubscribeSQLWithOptions applies optional result size limits.
func SubscribeSQLWithOptions(ctx context.Context, db *DB, tenant, query string, opts SubscriptionOptions) (*QuerySubscription, error) {
	return engine.SubscribeSQLWithOptions(ctx, db, tenant, query, opts)
}
