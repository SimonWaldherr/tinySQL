// Package storage provides persistence primitives and metadata structures
// used by tinySQL. This file implements a lightweight in-memory system
// catalog used for introspection (tables, columns, views, functions)
// and simple job scheduling metadata.
package storage

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ==================== System Catalog ====================
// Provides metadata tables for introspection and job scheduling

// TriggerTiming indicates when a trigger fires.
type TriggerTiming string

const (
	// TriggerBefore fires before the DML operation.
	TriggerBefore TriggerTiming = "BEFORE"
	// TriggerAfter fires after the DML operation.
	TriggerAfter TriggerTiming = "AFTER"
	// TriggerInsteadOf replaces the DML operation (views).
	TriggerInsteadOf TriggerTiming = "INSTEAD OF"
)

// TriggerEvent indicates what event fires the trigger.
type TriggerEvent string

const (
	// TriggerInsert fires on INSERT.
	TriggerInsert TriggerEvent = "INSERT"
	// TriggerUpdate fires on UPDATE.
	TriggerUpdate TriggerEvent = "UPDATE"
	// TriggerDelete fires on DELETE.
	TriggerDelete TriggerEvent = "DELETE"
)

// CatalogTrigger holds a stored trigger definition.
type CatalogTrigger struct {
	Name       string
	Table      string
	Timing     TriggerTiming
	Event      TriggerEvent
	ForEachRow bool
	WhenExpr   string // optional WHEN clause SQL text
	Body       string // SQL text of trigger body (semicolon-separated stmts)
	CreatedAt  time.Time
}

// CatalogManager manages system catalog tables (`catalog.tables`,
// `catalog.columns`, etc.) and provides thread-safe registration and
// lookup helpers used by the rest of the system for introspection and
// scheduling. CatalogManager is safe for concurrent use.
type CatalogManager struct {
	mu           sync.RWMutex
	tables       map[string]*CatalogTable
	columns      map[string][]CatalogColumn
	views        map[string]*CatalogView
	mviews       map[string]*CatalogMaterializedView
	dependencies map[string][]CatalogDependency
	indexes      map[string]*CatalogIndex
	funcs        map[string]*CatalogFunction
	jobs         map[string]*CatalogJob
	jobRuns      []*CatalogJobHistory
	nextRun      int64
	triggers     map[string]*CatalogTrigger // keyed by trigger name
	rbac         *rbacState                 // users/roles/grants; see rbac.go
}

// NewCatalogManager allocates and returns an initialized CatalogManager.
func NewCatalogManager() *CatalogManager {
	return &CatalogManager{
		tables:       make(map[string]*CatalogTable),
		columns:      make(map[string][]CatalogColumn),
		views:        make(map[string]*CatalogView),
		mviews:       make(map[string]*CatalogMaterializedView),
		dependencies: make(map[string][]CatalogDependency),
		indexes:      make(map[string]*CatalogIndex),
		funcs:        make(map[string]*CatalogFunction),
		jobs:         make(map[string]*CatalogJob),
		jobRuns:      make([]*CatalogJobHistory, 0),
		nextRun:      1,
		triggers:     make(map[string]*CatalogTrigger),
		rbac:         newRBACState(),
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

// CatalogMaterializedView stores a saved query with physical refresh metadata.
type CatalogMaterializedView struct {
	Schema             string
	Name               string
	SQLText            string
	CacheTableName     string
	StaleAfterMs       int64
	RefreshEveryMs     int64
	DailyAt            string
	Timezone           string
	WithData           bool
	InvalidateOnChange bool
	IsStale            bool
	LastRefreshAt      *time.Time
	LastDurationMs     int64
	LastError          string
	IsRefreshing       bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CatalogDependency records that one catalog object depends on another object.
type CatalogDependency struct {
	Schema          string
	ObjectName      string
	ObjectType      string
	DependsOnSchema string
	DependsOnName   string
	DependsOnType   string
	DependencyType  string
	CreatedAt       time.Time
}

// CatalogIndex stores metadata for a CREATE INDEX statement. tinySQL records
// index definitions for introspection, but the query planner does not consume
// them yet.
type CatalogIndex struct {
	// Tenant scopes the index definition to one logical database tenant.
	// Unlike views and functions, materialized secondary indexes belong to a
	// physical tenant table, so catalog metadata must never be shared across
	// tenants with the same schema/name.
	Tenant    string
	Schema    string
	Name      string
	Table     string
	Columns   []string
	Unique    bool
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

// CatalogJobHistory records one completed, failed, skipped, or canceled job run.
type CatalogJobHistory struct {
	RunID        int64
	JobName      string
	StartedAt    time.Time
	FinishedAt   time.Time
	DurationMs   int64
	Status       string // 'SUCCEEDED', 'FAILED', 'SKIPPED', 'CANCELED'
	ErrorMessage string
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
		var defaultValue *string
		if col.HasDefault {
			v := catalogDefaultValue(col.DefaultValue)
			defaultValue = &v
		}
		catalogCols[i] = CatalogColumn{
			Schema:       schema,
			TableName:    name,
			Name:         col.Name,
			Position:     i + 1,
			DataType:     columnDeclaredType(col),
			IsNullable:   !col.NotNull && col.Constraint != PrimaryKey,
			DefaultValue: defaultValue,
		}
	}
	c.columns[key] = catalogCols

	return nil
}

func columnDeclaredType(col Column) string {
	if col.DeclaredType != "" {
		return col.DeclaredType
	}
	return col.Type.String()
}

func catalogDefaultValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "X'" + fmt.Sprintf("%X", x) + "'"
	default:
		return fmt.Sprint(x)
	}
}

// RegisterView registers a view definition under `schema.name`.
func (c *CatalogManager) RegisterView(schema, name, sqlText string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := schema + "." + name
	now := time.Now()
	c.views[key] = &CatalogView{
		Schema:    schema,
		Name:      name,
		SQLText:   sqlText,
		CreatedAt: now,
	}
	c.tables[key] = &CatalogTable{
		Schema:    schema,
		Name:      name,
		Type:      "VIEW",
		CreatedAt: now,
		UpdatedAt: now,
	}

	return nil
}

// DeleteView removes a view definition and its catalog table entry.
func (c *CatalogManager) DeleteView(schema, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := schema + "." + name
	if _, ok := c.views[key]; !ok {
		return fmt.Errorf("view %q not found", name)
	}
	delete(c.views, key)
	delete(c.tables, key)
	delete(c.columns, key)
	delete(c.dependencies, key)
	return nil
}

// GetView retrieves a view definition by schema and name.
func (c *CatalogManager) GetView(schema, name string) (*CatalogView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.views[schema+"."+name]
	if !ok {
		return nil, false
	}
	cp := *v
	return &cp, true
}

// RegisterMaterializedView registers or updates a materialized view definition.
func (c *CatalogManager) RegisterMaterializedView(mv *CatalogMaterializedView) error {
	if mv == nil || mv.Name == "" {
		return fmt.Errorf("materialized view name cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if mv.Schema == "" {
		mv.Schema = "main"
	}
	key := mv.Schema + "." + mv.Name
	now := time.Now()
	if existing := c.mviews[key]; existing != nil && !existing.CreatedAt.IsZero() && mv.CreatedAt.IsZero() {
		mv.CreatedAt = existing.CreatedAt
	}
	if mv.CreatedAt.IsZero() {
		mv.CreatedAt = now
	}
	mv.UpdatedAt = now
	if mv.CacheTableName == "" {
		mv.CacheTableName = "__mv_" + strings.ToLower(mv.Name)
	}
	cp := *mv
	c.mviews[key] = &cp
	c.tables[key] = &CatalogTable{
		Schema:    mv.Schema,
		Name:      mv.Name,
		Type:      "MATERIALIZED VIEW",
		CreatedAt: mv.CreatedAt,
		UpdatedAt: now,
	}
	return nil
}

// DeleteMaterializedView removes a materialized view definition.
func (c *CatalogManager) DeleteMaterializedView(schema, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := schema + "." + name
	if _, ok := c.mviews[key]; !ok {
		return fmt.Errorf("materialized view %q not found", name)
	}
	delete(c.mviews, key)
	delete(c.tables, key)
	delete(c.columns, key)
	delete(c.dependencies, key)
	return nil
}

// GetMaterializedView retrieves a materialized view definition by schema/name.
func (c *CatalogManager) GetMaterializedView(schema, name string) (*CatalogMaterializedView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mv, ok := c.mviews[schema+"."+name]
	if !ok {
		return nil, false
	}
	cp := *mv
	return &cp, true
}

// GetMaterializedViews returns all materialized view definitions.
func (c *CatalogManager) GetMaterializedViews() []*CatalogMaterializedView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*CatalogMaterializedView, 0, len(c.mviews))
	for _, mv := range c.mviews {
		cp := *mv
		out = append(out, &cp)
	}
	return out
}

// TryBeginMaterializedViewRefresh marks a materialized view as refreshing.
func (c *CatalogManager) TryBeginMaterializedViewRefresh(schema, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	mv := c.mviews[schema+"."+name]
	if mv == nil || mv.IsRefreshing {
		return false
	}
	mv.IsRefreshing = true
	mv.UpdatedAt = time.Now()
	return true
}

// FinishMaterializedViewRefresh updates refresh bookkeeping.
func (c *CatalogManager) FinishMaterializedViewRefresh(schema, name string, refreshedAt time.Time, durationMs int64, rowCount int64, errMsg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	mv := c.mviews[schema+"."+name]
	if mv == nil {
		return fmt.Errorf("materialized view %q not found", name)
	}
	mv.IsRefreshing = false
	mv.LastDurationMs = durationMs
	mv.LastError = errMsg
	if errMsg == "" {
		mv.LastRefreshAt = &refreshedAt
		mv.IsStale = false
	}
	mv.UpdatedAt = time.Now()
	if t := c.tables[schema+"."+name]; t != nil {
		t.RowCount = rowCount
		t.UpdatedAt = mv.UpdatedAt
	}
	return nil
}

// SetDependencies replaces the dependency list for a catalog object.
func (c *CatalogManager) SetDependencies(schema, objectName, objectType string, deps []CatalogDependency) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := schema + "." + objectName
	if len(deps) == 0 {
		delete(c.dependencies, key)
		return
	}
	now := time.Now()
	out := make([]CatalogDependency, 0, len(deps))
	for _, dep := range deps {
		if dep.Schema == "" {
			dep.Schema = schema
		}
		if dep.ObjectName == "" {
			dep.ObjectName = objectName
		}
		if dep.ObjectType == "" {
			dep.ObjectType = objectType
		}
		if dep.DependsOnSchema == "" {
			dep.DependsOnSchema = "main"
		}
		if dep.DependsOnType == "" {
			dep.DependsOnType = "UNKNOWN"
		}
		if dep.DependencyType == "" {
			dep.DependencyType = "NORMAL"
		}
		if dep.CreatedAt.IsZero() {
			dep.CreatedAt = now
		}
		out = append(out, dep)
	}
	c.dependencies[key] = out
}

// GetDependencies returns all recorded catalog dependencies.
func (c *CatalogManager) GetDependencies() []CatalogDependency {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]CatalogDependency, 0)
	for _, deps := range c.dependencies {
		out = append(out, deps...)
	}
	return out
}

// RegisterIndex stores index metadata in the default tenant. New engine code
// should use RegisterIndexForTenant so tenant ownership is explicit.
func (c *CatalogManager) RegisterIndex(idx *CatalogIndex) error {
	tenant := "default"
	if idx != nil && idx.Tenant != "" {
		tenant = idx.Tenant
	}
	return c.RegisterIndexForTenant(tenant, idx)
}

// RegisterIndexForTenant stores index metadata for one tenant. If an index
// with the same tenant/schema/name exists, it is replaced.
func (c *CatalogManager) RegisterIndexForTenant(tenant string, idx *CatalogIndex) error {
	if idx == nil || idx.Name == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if idx.Table == "" {
		return fmt.Errorf("index table cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx.Schema == "" {
		idx.Schema = "main"
	}
	if idx.CreatedAt.IsZero() {
		idx.CreatedAt = time.Now()
	}
	cp := *idx
	cp.Tenant = normalizeCatalogTenant(tenant)
	cp.Columns = append([]string(nil), idx.Columns...)
	c.indexes[catalogIndexKey(cp.Tenant, cp.Schema, cp.Name)] = &cp
	return nil
}

// DeleteIndex removes an index definition from the default tenant. New engine
// code should use DeleteIndexForTenant so tenant ownership is explicit.
func (c *CatalogManager) DeleteIndex(schema, name string) error {
	return c.DeleteIndexForTenant("default", schema, name)
}

// DeleteIndexForTenant removes a stored index definition from one tenant.
func (c *CatalogManager) DeleteIndexForTenant(tenant, schema, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := catalogIndexKey(tenant, schema, name)
	if _, ok := c.indexes[key]; !ok {
		return fmt.Errorf("index %q not found", name)
	}
	delete(c.indexes, key)
	return nil
}

// DeleteIndexesForTable removes default-tenant indexes registered for a
// table. New engine code should use DeleteIndexesForTenantTable.
func (c *CatalogManager) DeleteIndexesForTable(table string) {
	c.DeleteIndexesForTenantTable("default", table)
}

// DeleteIndexesForTenantTable removes all indexes registered for a table in
// one tenant only.
func (c *CatalogManager) DeleteIndexesForTenantTable(tenant, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, idx := range c.indexes {
		if idx.Tenant != "" && normalizeCatalogTenant(idx.Tenant) == normalizeCatalogTenant(tenant) && strings.EqualFold(idx.Table, table) {
			delete(c.indexes, key)
		}
	}
}

// GetIndex retrieves a default-tenant index definition by schema and name.
// New engine code should use GetIndexForTenant.
func (c *CatalogManager) GetIndex(schema, name string) (*CatalogIndex, bool) {
	return c.GetIndexForTenant("default", schema, name)
}

// GetIndexForTenant retrieves one tenant's index definition by schema and
// name. Legacy unscoped catalog records intentionally do not match: guessing
// their owner would let one tenant read or delete another tenant's metadata.
func (c *CatalogManager) GetIndexForTenant(tenant, schema, name string) (*CatalogIndex, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx, ok := c.indexes[catalogIndexKey(tenant, schema, name)]
	if !ok {
		return nil, false
	}
	cp := *idx
	cp.Columns = append([]string(nil), idx.Columns...)
	return &cp, true
}

// GetIndexes returns all registered index definitions, including legacy
// unscoped records. It exists for administrative callers; query execution
// and virtual tables must use GetIndexesForTenant.
func (c *CatalogManager) GetIndexes() []*CatalogIndex {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*CatalogIndex, 0, len(c.indexes))
	for _, idx := range c.indexes {
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		out = append(out, &cp)
	}
	return out
}

// GetIndexesForTenant returns only index definitions owned by tenant. Legacy
// records with an empty tenant are deliberately excluded rather than being
// guessed as a particular tenant.
func (c *CatalogManager) GetIndexesForTenant(tenant string) []*CatalogIndex {
	c.mu.RLock()
	defer c.mu.RUnlock()
	normalizedTenant := normalizeCatalogTenant(tenant)
	out := make([]*CatalogIndex, 0)
	for _, idx := range c.indexes {
		if idx.Tenant == "" || normalizeCatalogTenant(idx.Tenant) != normalizedTenant {
			continue
		}
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		out = append(out, &cp)
	}
	return out
}

func normalizeCatalogTenant(tenant string) string {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return "default"
	}
	return strings.ToLower(tenant)
}

func catalogIndexKey(tenant, schema, name string) string {
	if schema == "" {
		schema = "main"
	}
	return normalizeCatalogTenant(tenant) + "\x00" + strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

func legacyCatalogIndexKey(schema, name string) string {
	if schema == "" {
		schema = "main"
	}
	return "\x00legacy\x00" + strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

// MarkMaterializedViewsStaleByDependency marks opt-in materialized views stale
// when they depend on the changed object. It returns the affected view names.
func (c *CatalogManager) MarkMaterializedViewsStaleByDependency(schema, changedName string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	changedKey := strings.ToLower(schema + "." + changedName)
	affected := make([]string, 0)
	for key, deps := range c.dependencies {
		mv := c.mviews[key]
		if mv == nil || !mv.InvalidateOnChange {
			continue
		}
		for _, dep := range deps {
			depKey := strings.ToLower(dep.DependsOnSchema + "." + dep.DependsOnName)
			if depKey != changedKey {
				continue
			}
			mv.IsStale = true
			mv.UpdatedAt = time.Now()
			affected = append(affected, mv.Name)
			break
		}
	}
	return affected
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
	return c.UpdateJobRuntimePtr(name, lastRun, &nextRun)
}

// UpdateJobRuntimePtr updates runtime bookkeeping fields for a named job.
// Passing nil for nextRun clears NextRunAt, which is expected for completed
// one-shot jobs.
func (c *CatalogManager) UpdateJobRuntimePtr(name string, lastRun time.Time, nextRun *time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, ok := c.jobs[name]
	if !ok {
		return fmt.Errorf("job %q not found", name)
	}

	job.LastRunAt = &lastRun
	job.NextRunAt = nextRun
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

// AddJobHistory appends a job execution history row and assigns a run id.
func (c *CatalogManager) AddJobHistory(run *CatalogJobHistory) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if run == nil {
		return fmt.Errorf("job history cannot be nil")
	}
	if run.JobName == "" {
		return fmt.Errorf("job history job name cannot be empty")
	}
	cp := *run
	if cp.RunID == 0 {
		cp.RunID = c.nextRun
		c.nextRun++
	} else if cp.RunID >= c.nextRun {
		c.nextRun = cp.RunID + 1
	}
	c.jobRuns = append(c.jobRuns, &cp)
	return nil
}

// ListJobHistory returns all job execution history rows.
func (c *CatalogManager) ListJobHistory() []*CatalogJobHistory {
	c.mu.RLock()
	defer c.mu.RUnlock()

	runs := make([]*CatalogJobHistory, 0, len(c.jobRuns))
	for _, run := range c.jobRuns {
		cp := *run
		runs = append(runs, &cp)
	}
	return runs
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

// RegisterTrigger stores a trigger definition in the catalog. If a trigger with
// the same name already exists it is replaced.
func (c *CatalogManager) RegisterTrigger(t *CatalogTrigger) error {
	if t == nil || t.Name == "" {
		return fmt.Errorf("trigger name cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	c.triggers[t.Name] = t
	return nil
}

// DropTrigger removes a named trigger from the catalog. It returns an error
// when the trigger does not exist.
func (c *CatalogManager) DropTrigger(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.triggers[name]; !ok {
		return fmt.Errorf("trigger %q not found", name)
	}
	delete(c.triggers, name)
	return nil
}

// GetTriggers returns all triggers for the given table, timing, and event.
func (c *CatalogManager) GetTriggers(table string, timing TriggerTiming, event TriggerEvent) []*CatalogTrigger {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*CatalogTrigger
	for _, t := range c.triggers {
		if strings.EqualFold(t.Table, table) &&
			t.Timing == timing &&
			t.Event == event {
			out = append(out, t)
		}
	}
	return out
}

// GetTriggersForEvent returns the BEFORE and AFTER row-trigger lists for one
// table/event pair while holding the catalog lock only once. DML executors
// resolve both timings together before entering their row loop, avoiding two
// full catalog scans per statement and any lookup work per affected row.
func (c *CatalogManager) GetTriggersForEvent(table string, event TriggerEvent) (before, after []*CatalogTrigger) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.triggers {
		if !strings.EqualFold(t.Table, table) || t.Event != event {
			continue
		}
		switch t.Timing {
		case TriggerBefore:
			before = append(before, t)
		case TriggerAfter:
			after = append(after, t)
		}
	}
	return before, after
}

// ListTriggers returns all registered trigger definitions.
func (c *CatalogManager) ListTriggers() []*CatalogTrigger {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*CatalogTrigger, 0, len(c.triggers))
	for _, t := range c.triggers {
		out = append(out, t)
	}
	return out
}

// Catalog returns the CatalogManager attached to the DB, creating one
// lazily if necessary. The catalog is reached by every SQL statement for
// RBAC checks, so its one-time construction must be synchronized when a
// database/sql connector opens several physical reader connections at once.
func (db *DB) Catalog() *CatalogManager {
	db.catalogMu.RLock()
	catalog := db.catalog
	db.catalogMu.RUnlock()
	if catalog != nil {
		return catalog
	}
	db.catalogMu.Lock()
	defer db.catalogMu.Unlock()
	if db.catalog == nil {
		db.catalog = NewCatalogManager()
	}
	return db.catalog
}

// setCatalog installs a fully decoded catalog while loading a database. It is
// deliberately private: callers that need the active catalog use Catalog(),
// which gives them a stable, concurrency-safe instance.
func (db *DB) setCatalog(catalog *CatalogManager) {
	db.catalogMu.Lock()
	db.catalog = catalog
	db.catalogMu.Unlock()
}

// StartJobScheduler starts the database job scheduler with the given executor.
func (db *DB) StartJobScheduler(executor JobExecutor) error {
	db.mu.Lock()
	if db.scheduler == nil {
		db.scheduler = NewScheduler(db, executor)
	} else {
		db.scheduler.executor = executor
	}
	scheduler := db.scheduler
	db.mu.Unlock()
	return scheduler.Start()
}

// StopJobScheduler stops the database job scheduler if it is running.
func (db *DB) StopJobScheduler() {
	db.mu.Lock()
	scheduler := db.scheduler
	db.scheduler = nil
	db.mu.Unlock()
	if scheduler != nil {
		scheduler.Stop()
	}
}

// RestartJobScheduler restarts the database job scheduler with the executor.
func (db *DB) RestartJobScheduler(executor JobExecutor) error {
	db.StopJobScheduler()
	return db.StartJobScheduler(executor)
}

// JobScheduler returns the active database job scheduler, if any.
func (db *DB) JobScheduler() *Scheduler {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.scheduler
}

// RegisterJob registers a job and refreshes the live scheduler when present.
func (db *DB) RegisterJob(job *CatalogJob) error {
	db.mu.RLock()
	scheduler := db.scheduler
	db.mu.RUnlock()
	if scheduler != nil {
		if err := scheduler.UpsertJob(job); err != nil {
			return err
		}
		return db.saveBackendCatalog()
	}
	if err := db.Catalog().RegisterJob(job); err != nil {
		return err
	}
	return db.saveBackendCatalog()
}

// DeleteJob deletes a job and unschedules/cancels it when present.
func (db *DB) DeleteJob(name string) error {
	db.mu.RLock()
	scheduler := db.scheduler
	db.mu.RUnlock()
	if scheduler != nil {
		if err := scheduler.RemoveJob(name); err != nil {
			return err
		}
		return db.saveBackendCatalog()
	}
	if err := db.Catalog().DeleteJob(name); err != nil {
		return err
	}
	return db.saveBackendCatalog()
}
