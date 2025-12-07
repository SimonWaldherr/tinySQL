package storage

import (
	"fmt"
	"sync"
	"time"
)

// ==================== System Catalog ====================
// Provides metadata tables for introspection and job scheduling

// CatalogManager manages system catalog tables (catalog.tables, catalog.columns, etc.)
type CatalogManager struct {
	mu      sync.RWMutex
	tables  map[string]*CatalogTable
	columns map[string][]CatalogColumn
	views   map[string]*CatalogView
	funcs   map[string]*CatalogFunction
	jobs    map[string]*CatalogJob
}

// NewCatalogManager creates a new catalog manager
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
type CatalogTable struct {
	Schema    string
	Name      string
	Type      string // 'TABLE', 'VIEW', 'SYSTEM'
	RowCount  int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CatalogColumn represents metadata for a table column
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
type CatalogView struct {
	Schema    string
	Name      string
	SQLText   string
	CreatedAt time.Time
}

// CatalogFunction represents metadata for scalar and table-valued functions
type CatalogFunction struct {
	Schema       string
	Name         string
	FunctionType string // 'SCALAR', 'TABLE', 'AGGREGATE', 'WINDOW'
	ArgTypes     []string
	ReturnType   string
	Language     string // 'BUILTIN', 'SQL', 'GO'
	IsDeterministic bool
	Description  string
}

// CatalogJob represents a scheduled job
type CatalogJob struct {
	Name          string
	SQLText       string
	ScheduleType  string // 'CRON', 'INTERVAL', 'ONCE'
	CronExpr      string // For CRON schedule
	IntervalMs    int64  // For INTERVAL schedule (milliseconds)
	RunAt         *time.Time // For ONCE schedule
	Timezone      string
	Enabled       bool
	CatchUp       bool // Run missed executions
	NoOverlap     bool // Prevent concurrent runs
	MaxRuntimeMs  int64
	LastRunAt     *time.Time
	NextRunAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ==================== Catalog Operations ====================

// RegisterTable adds a table to the catalog
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

// RegisterView adds a view to the catalog
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

// RegisterFunction adds a function to the catalog
func (c *CatalogManager) RegisterFunction(fn *CatalogFunction) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fn.Schema + "." + fn.Name
	c.funcs[key] = fn
	return nil
}

// RegisterJob adds or updates a job in the catalog
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

// GetJob retrieves a job by name
func (c *CatalogManager) GetJob(name string) (*CatalogJob, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	job, ok := c.jobs[name]
	if !ok {
		return nil, fmt.Errorf("job %q not found", name)
	}
	return job, nil
}

// ListJobs returns all registered jobs
func (c *CatalogManager) ListJobs() []*CatalogJob {
	c.mu.RLock()
	defer c.mu.RUnlock()

	jobs := make([]*CatalogJob, 0, len(c.jobs))
	for _, job := range c.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// ListEnabledJobs returns all enabled jobs
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

// UpdateJobRuntime updates last_run_at and next_run_at for a job
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

// DeleteJob removes a job from the catalog
func (c *CatalogManager) DeleteJob(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.jobs[name]; !ok {
		return fmt.Errorf("job %q not found", name)
	}

	delete(c.jobs, name)
	return nil
}

// GetTables returns all tables in the catalog
func (c *CatalogManager) GetTables() []*CatalogTable {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]*CatalogTable, 0, len(c.tables))
	for _, t := range c.tables {
		tables = append(tables, t)
	}
	return tables
}

// GetColumns returns all columns for a table
func (c *CatalogManager) GetColumns(schema, tableName string) []CatalogColumn {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := schema + "." + tableName
	return c.columns[key]
}

// Attach catalog manager to DB
func (db *DB) Catalog() *CatalogManager {
	if db.catalog == nil {
		db.catalog = NewCatalogManager()
	}
	return db.catalog
}
