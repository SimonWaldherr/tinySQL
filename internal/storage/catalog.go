// Package storage provides persistence primitives and metadata structures
// used by tinySQL. This file implements a lightweight in-memory system
// catalog used for introspection (tables, columns, views, functions)
// and simple job scheduling metadata.
package storage

import (
	"fmt"
	"sync"
	"time"
)

// ==================== System Catalog ====================
// Provides metadata tables for introspection and job scheduling

// CatalogManager manages system catalog tables (`catalog.tables`,
// `catalog.columns`, etc.) and provides thread-safe registration and
// lookup helpers used by the rest of the system for introspection and
// scheduling. CatalogManager is safe for concurrent use.
type CatalogManager struct {
	mu      sync.RWMutex
	tables  map[string]*CatalogTable
	columns map[string][]CatalogColumn
	views   map[string]*CatalogView
	funcs   map[string]*CatalogFunction
	jobs    map[string]*CatalogJob
}

// NewCatalogManager allocates and returns an initialized CatalogManager.
func NewCatalogManager() *CatalogManager {
	return &CatalogManager{
		tables:  make(map[string]*CatalogTable),
		columns: make(map[string][]CatalogColumn),
		views:   make(map[string]*CatalogView),
		funcs:   make(map[string]*CatalogFunction),
		jobs:    make(map[string]*CatalogJob),
	}
}

// CatalogTable represents metadata for a single table
// CatalogTable holds basic metadata for a registered table or view.
type CatalogTable struct {
	Schema    string
	Name      string
	Type      string // 'TABLE', 'VIEW', 'SYSTEM'
	RowCount  int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CatalogColumn represents metadata for a table column
// CatalogColumn represents a column in a catalog table including its
// position and declared data type. `DefaultValue` may be nil if none
// is defined.
type CatalogColumn struct {
	Schema       string
	TableName    string
	Name         string
	Position     int
	DataType     string
	IsNullable   bool
	DefaultValue *string
}

// CatalogView represents a saved view definition
// CatalogView stores the definition of a saved view.
type CatalogView struct {
	Schema    string
	Name      string
	SQLText   string
	CreatedAt time.Time
}

// CatalogFunction represents metadata for scalar and table-valued functions
// CatalogFunction describes registered functions (builtin or user
// defined). `FunctionType` categorizes the function, and `Language`
// indicates the implementation origin.
type CatalogFunction struct {
	Schema          string
	Name            string
	FunctionType    string // 'SCALAR', 'TABLE', 'AGGREGATE', 'WINDOW'
	ArgTypes        []string
	ReturnType      string
	Language        string // 'BUILTIN', 'SQL', 'GO'
	IsDeterministic bool
	Description     string
}

// CatalogJob represents a scheduled job
// CatalogJob describes a scheduled job. The fields provide flexible
// scheduling options (CRON, interval, or single-run) and execution
// metadata for bookkeeping and scheduling decisions.
type CatalogJob struct {
	Name         string
	SQLText      string
	ScheduleType string     // 'CRON', 'INTERVAL', 'ONCE'
	CronExpr     string     // For CRON schedule
	IntervalMs   int64      // For INTERVAL schedule (milliseconds)
	RunAt        *time.Time // For ONCE schedule
	Timezone     string
	Enabled      bool
	CatchUp      bool // Run missed executions
	NoOverlap    bool // Prevent concurrent runs
	MaxRuntimeMs int64
	LastRunAt    *time.Time
	NextRunAt    *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ==================== Catalog Operations ====================

// RegisterTable registers a table and its columns in the system catalog.
// The provided `cols` slice is converted to `CatalogColumn` entries and
// stored under the key `schema.name`.
func (c *CatalogManager) RegisterTable(schema, name string, cols []Column) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := schema + "." + name
	c.tables[key] = &CatalogTable{
		Schema:    schema,
		Name:      name,
		Type:      "TABLE",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Register columns
	catalogCols := make([]CatalogColumn, len(cols))
	for i, col := range cols {
		catalogCols[i] = CatalogColumn{
			Schema:     schema,
			TableName:  name,
			Name:       col.Name,
			Position:   i + 1,
			DataType:   col.Type.String(),
			IsNullable: true, // tinySQL doesn't enforce NOT NULL yet
		}
	}
	c.columns[key] = catalogCols

	return nil
}

// RegisterView registers a view definition under `schema.name`.
func (c *CatalogManager) RegisterView(schema, name, sqlText string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := schema + "." + name
	c.views[key] = &CatalogView{
		Schema:    schema,
		Name:      name,
		SQLText:   sqlText,
		CreatedAt: time.Now(),
	}

	return nil
}

// RegisterFunction registers or updates a function's metadata.
func (c *CatalogManager) RegisterFunction(fn *CatalogFunction) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fn.Schema + "." + fn.Name
	c.funcs[key] = fn
	return nil
}

// RegisterJob adds a new scheduled job or updates an existing entry.
// Job names must be non-empty.
func (c *CatalogManager) RegisterJob(job *CatalogJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if job.Name == "" {
		return fmt.Errorf("job name cannot be empty")
	}

	job.UpdatedAt = time.Now()
	if c.jobs[job.Name] == nil {
		job.CreatedAt = time.Now()
	}

	c.jobs[job.Name] = job
	return nil
}

// GetJob retrieves a job by name, returning an error if not found.
func (c *CatalogManager) GetJob(name string) (*CatalogJob, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	job, ok := c.jobs[name]
	if !ok {
		return nil, fmt.Errorf("job %q not found", name)
	}
	return job, nil
}

// ListJobs returns a slice containing all registered jobs.
func (c *CatalogManager) ListJobs() []*CatalogJob {
	c.mu.RLock()
	defer c.mu.RUnlock()

	jobs := make([]*CatalogJob, 0, len(c.jobs))
	for _, job := range c.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// ListEnabledJobs returns only jobs whose `Enabled` flag is true.
func (c *CatalogManager) ListEnabledJobs() []*CatalogJob {
	c.mu.RLock()
	defer c.mu.RUnlock()

	jobs := make([]*CatalogJob, 0)
	for _, job := range c.jobs {
		if job.Enabled {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// UpdateJobRuntime updates runtime bookkeeping fields for a named job.
// It sets `LastRunAt`, `NextRunAt` and marks the job as recently updated.
func (c *CatalogManager) UpdateJobRuntime(name string, lastRun, nextRun time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[name]
	if !ok {
		return fmt.Errorf("job %q not found", name)
	}

	job.LastRunAt = &lastRun
	job.NextRunAt = &nextRun
	job.UpdatedAt = time.Now()
	return nil
}

// DeleteJob removes a job from the catalog, returning an error when the
// job does not exist.
func (c *CatalogManager) DeleteJob(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.jobs[name]; !ok {
		return fmt.Errorf("job %q not found", name)
	}

	delete(c.jobs, name)
	return nil
}

// GetTables returns a slice with metadata for all registered tables and
// views.
func (c *CatalogManager) GetTables() []*CatalogTable {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]*CatalogTable, 0, len(c.tables))
	for _, t := range c.tables {
		tables = append(tables, t)
	}
	return tables
}

// GetFunctions returns metadata for all registered functions.
func (c *CatalogManager) GetFunctions() []*CatalogFunction {
	c.mu.RLock()
	defer c.mu.RUnlock()

	funcs := make([]*CatalogFunction, 0, len(c.funcs))
	for _, f := range c.funcs {
		funcs = append(funcs, f)
	}
	return funcs
}

// GetViews returns metadata for all registered views.
func (c *CatalogManager) GetViews() []*CatalogView {
	c.mu.RLock()
	defer c.mu.RUnlock()

	views := make([]*CatalogView, 0, len(c.views))
	for _, v := range c.views {
		views = append(views, v)
	}
	return views
}

// GetAllColumns aggregates and returns columns registered for every
// table in the catalog. The returned slice is a concatenation of the
// internal per-table column lists.
func (c *CatalogManager) GetAllColumns() []CatalogColumn {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cols := make([]CatalogColumn, 0)
	for _, list := range c.columns {
		cols = append(cols, list...)
	}
	return cols
}

// GetColumns returns the column metadata for `schema.tableName`. If the
// table is unknown an empty slice is returned.
func (c *CatalogManager) GetColumns(schema, tableName string) []CatalogColumn {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := schema + "." + tableName
	return c.columns[key]
}

// Catalog returns the CatalogManager attached to the DB, creating one
// lazily if necessary.
func (db *DB) Catalog() *CatalogManager {
	if db.catalog == nil {
		db.catalog = NewCatalogManager()
	}
	return db.catalog
}
