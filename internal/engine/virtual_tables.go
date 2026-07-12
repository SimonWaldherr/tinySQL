package engine

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Virtual System Tables  (SELECT * FROM sys.<name>)
//
// These tables are computed on-the-fly from runtime state and do not exist in
// any physical storage.  They allow introspection of:
//
//   sys.tables       – real tables across all tenants (auto-populated)
//   sys.columns      – real columns from actual table schemas
//   sys.constraints  – PK / FK / UNIQUE constraints
//   sys.indexes      – CREATE INDEX metadata for materialized planner-backed indexes
//   sys.views        – same as catalog.views but auto-populated
//   sys.functions    – all registered functions (builtin + extended + vector)
//   sys.variables    – server variables (version, pid, mode, …)
//   sys.status       – runtime status (goroutines, uptime, Go version, …)
//   sys.memory       – Go runtime memory statistics
//   sys.storage      – storage backend statistics
//   sys.config       – database configuration
//   sys.connections  – active tenant / connection info
//   sys.procedures   – process-local in-memory stored procedures
//   sys.objects      – unified status for tables, views, jobs, triggers, …
//   sys.dependencies – dependency graph for views and materialized views
// ============================================================================

// startTime records when the process started so sys.status can report uptime.
var startTime = time.Now()

// resolveSysTable returns the rows for a given sys.<name> virtual table.
// It returns nil, nil when the name is not recognized so the caller can fall
// back to the default "unknown catalog/sys table" error.
func resolveSysTable(env ExecEnv, name string) ([]Row, error) {
	switch name {
	case "objects":
		return allObjectStatusRows(env), nil
	case "dependencies":
		return dependencyRows(env), nil
	case "tables":
		return sysTablesRows(env), nil
	case "columns":
		return sysColumnsRows(env), nil
	case "constraints":
		return sysConstraintsRows(env), nil
	case "indexes":
		return sysIndexesRows(env), nil
	case "views":
		return sysViewsRows(env), nil
	case "materialized_views":
		return sysMaterializedViewsRows(env), nil
	case "functions":
		return sysFunctionsRows(), nil
	case "procedures":
		return sysProceduresRows(), nil
	case "variables":
		return sysVariablesRows(env), nil
	case "status":
		return sysStatusRows(), nil
	case "memory":
		return sysMemoryRows(), nil
	case "storage":
		return sysStorageRows(env), nil
	case "config":
		return sysConfigRows(env), nil
	case "connections":
		return sysConnectionsRows(env), nil
	case "triggers":
		return sysTriggersRows(env), nil
	default:
		return nil, fmt.Errorf("unknown sys table: %s", name)
	}
}

// ─────────────────────────── sys.dependencies / catalog.dependencies ─────

func dependencyRows(env ExecEnv) []Row {
	deps := env.db.Catalog().GetDependencies()
	rows := make([]Row, len(deps))
	for i, dep := range deps {
		r := make(Row)
		putVal(r, "schema", dep.Schema)
		putVal(r, "object_name", dep.ObjectName)
		putVal(r, "object_type", dep.ObjectType)
		putVal(r, "depends_on_schema", dep.DependsOnSchema)
		putVal(r, "depends_on_name", dep.DependsOnName)
		putVal(r, "depends_on_type", dep.DependsOnType)
		putVal(r, "dependency_type", dep.DependencyType)
		putVal(r, "created_at", dep.CreatedAt)
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.tables ──────────────────────────────────

func sysTablesRows(env ExecEnv) []Row {
	var rows []Row
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			schema, name := splitObjectName(t.Name)
			r := make(Row)
			putVal(r, "tenant", tn)
			putVal(r, "schema", schema)
			putVal(r, "name", name)
			putVal(r, "full_name", catalogDisplayName(schema, name))
			putVal(r, "columns", len(t.Cols))
			putVal(r, "rows", len(t.Rows))
			putVal(r, "is_temp", t.IsTemp)
			putVal(r, "version", t.Version)
			rows = append(rows, r)
		}
	}
	return rows
}

// ─────────────────────────── sys.objects / catalog.objects ───────────────

func allObjectStatusRows(env ExecEnv) []Row {
	rows := make([]Row, 0)
	rows = append(rows, tableStatusRows(env)...)
	rows = append(rows, viewStatusRows(env)...)
	rows = append(rows, materializedViewStatusRows(env)...)
	rows = append(rows, jobStatusRows(env)...)
	rows = append(rows, triggerStatusRows(env)...)
	rows = append(rows, functionStatusRows(env)...)
	rows = append(rows, procedureStatusRows(env)...)
	return rows
}

func tableStatusRows(env ExecEnv) []Row {
	rows := make([]Row, 0)
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			if strings.HasPrefix(strings.ToLower(t.Name), "__mv_") {
				continue
			}
			schema, name := splitObjectName(t.Name)
			r := make(Row)
			putVal(r, "schema", schema)
			putVal(r, "tenant", tn)
			putVal(r, "name", name)
			putVal(r, "full_name", catalogDisplayName(schema, name))
			putVal(r, "object_type", "TABLE")
			putVal(r, "status", "ONLINE")
			putVal(r, "rows", len(t.Rows))
			putVal(r, "columns", len(t.Cols))
			putVal(r, "version", t.Version)
			putVal(r, "is_stale", nil)
			putVal(r, "last_refresh_at", nil)
			putVal(r, "next_run_at", nil)
			putVal(r, "last_error", nil)
			rows = append(rows, r)
		}
	}
	return rows
}

func viewStatusRows(env ExecEnv) []Row {
	views := env.db.Catalog().GetViews()
	rows := make([]Row, 0, len(views))
	for _, v := range views {
		r := make(Row)
		putVal(r, "schema", v.Schema)
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", v.Name)
		putVal(r, "object_type", "VIEW")
		putVal(r, "status", "QUERYABLE")
		putVal(r, "rows", nil)
		putVal(r, "columns", nil)
		putVal(r, "version", nil)
		putVal(r, "is_stale", nil)
		putVal(r, "last_refresh_at", nil)
		putVal(r, "next_run_at", nil)
		putVal(r, "last_error", nil)
		putVal(r, "sql_text", v.SQLText)
		putVal(r, "created_at", v.CreatedAt)
		rows = append(rows, r)
	}
	return rows
}

func materializedViewStatusRows(env ExecEnv) []Row {
	views := env.db.Catalog().GetMaterializedViews()
	rows := make([]Row, 0, len(views))
	for _, v := range views {
		rowCount := any(nil)
		columnCount := any(nil)
		cacheExists := false
		if cache, err := env.db.Get(env.tenant, v.CacheTableName); err == nil {
			cacheExists = true
			rowCount = len(cache.Rows)
			columnCount = len(cache.Cols)
		}
		stale := materializedViewIsStale(v, cacheExists)
		status := "FRESH"
		switch {
		case v.IsRefreshing:
			status = "REFRESHING"
		case v.LastError != "":
			status = "ERROR"
		case !cacheExists:
			status = "UNMATERIALIZED"
		case stale:
			status = "STALE"
		}
		r := make(Row)
		putVal(r, "schema", v.Schema)
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", v.Name)
		putVal(r, "object_type", "MATERIALIZED_VIEW")
		putVal(r, "status", status)
		putVal(r, "rows", rowCount)
		putVal(r, "columns", columnCount)
		putVal(r, "version", nil)
		putVal(r, "is_stale", stale)
		putVal(r, "last_refresh_at", v.LastRefreshAt)
		putVal(r, "next_run_at", nil)
		putVal(r, "last_error", v.LastError)
		putVal(r, "cache_table_name", v.CacheTableName)
		putVal(r, "stale_after_ms", v.StaleAfterMs)
		putVal(r, "refresh_every_ms", v.RefreshEveryMs)
		putVal(r, "daily_at", v.DailyAt)
		putVal(r, "invalidate_on_change", v.InvalidateOnChange)
		putVal(r, "sql_text", v.SQLText)
		putVal(r, "created_at", v.CreatedAt)
		putVal(r, "updated_at", v.UpdatedAt)
		rows = append(rows, r)
	}
	return rows
}

func materializedViewIsStale(v *storage.CatalogMaterializedView, cacheExists bool) bool {
	if !cacheExists {
		return true
	}
	if v.IsStale {
		return true
	}
	if v.StaleAfterMs <= 0 {
		return false
	}
	if v.LastRefreshAt == nil {
		return true
	}
	return time.Since(*v.LastRefreshAt) >= time.Duration(v.StaleAfterMs)*time.Millisecond
}

func jobStatusRows(env ExecEnv) []Row {
	jobs := env.db.Catalog().ListJobs()
	rows := make([]Row, 0, len(jobs))
	for _, j := range jobs {
		status := "DISABLED"
		if j.Enabled {
			status = "ENABLED"
		}
		r := make(Row)
		putVal(r, "schema", "main")
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", j.Name)
		putVal(r, "object_type", "JOB")
		putVal(r, "status", status)
		putVal(r, "rows", nil)
		putVal(r, "columns", nil)
		putVal(r, "version", nil)
		putVal(r, "is_stale", nil)
		putVal(r, "last_refresh_at", nil)
		putVal(r, "next_run_at", j.NextRunAt)
		putVal(r, "last_error", nil)
		putVal(r, "schedule_type", j.ScheduleType)
		putVal(r, "last_run_at", j.LastRunAt)
		putVal(r, "sql_text", j.SQLText)
		putVal(r, "created_at", j.CreatedAt)
		putVal(r, "updated_at", j.UpdatedAt)
		rows = append(rows, r)
	}
	return rows
}

func triggerStatusRows(env ExecEnv) []Row {
	triggers := env.db.Catalog().ListTriggers()
	rows := make([]Row, 0, len(triggers))
	for _, tr := range triggers {
		r := make(Row)
		putVal(r, "schema", "main")
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", tr.Name)
		putVal(r, "object_type", "TRIGGER")
		putVal(r, "status", "ENABLED")
		putVal(r, "table_name", tr.Table)
		putVal(r, "event", tr.Event)
		putVal(r, "timing", tr.Timing)
		putVal(r, "rows", nil)
		putVal(r, "columns", nil)
		putVal(r, "version", nil)
		putVal(r, "is_stale", nil)
		putVal(r, "last_refresh_at", nil)
		putVal(r, "next_run_at", nil)
		putVal(r, "last_error", nil)
		putVal(r, "created_at", tr.CreatedAt)
		rows = append(rows, r)
	}
	return rows
}

func functionStatusRows(env ExecEnv) []Row {
	funcs := env.db.Catalog().GetFunctions()
	rows := make([]Row, 0, len(funcs))
	for _, fn := range funcs {
		r := make(Row)
		putVal(r, "schema", fn.Schema)
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", fn.Name)
		putVal(r, "object_type", "FUNCTION")
		putVal(r, "status", "AVAILABLE")
		putVal(r, "function_type", fn.FunctionType)
		putVal(r, "rows", nil)
		putVal(r, "columns", nil)
		putVal(r, "version", nil)
		putVal(r, "is_stale", nil)
		putVal(r, "last_refresh_at", nil)
		putVal(r, "next_run_at", nil)
		putVal(r, "last_error", nil)
		rows = append(rows, r)
	}
	return rows
}

func procedureStatusRows(env ExecEnv) []Row {
	procs := ListStoredProcedures()
	rows := make([]Row, 0, len(procs))
	for _, proc := range procs {
		r := make(Row)
		putVal(r, "schema", "sys")
		putVal(r, "tenant", env.tenant)
		putVal(r, "name", proc.Name)
		putVal(r, "object_type", "PROCEDURE")
		putVal(r, "status", "AVAILABLE")
		putVal(r, "rows", nil)
		putVal(r, "columns", nil)
		putVal(r, "version", nil)
		putVal(r, "is_stale", nil)
		putVal(r, "last_refresh_at", nil)
		putVal(r, "next_run_at", nil)
		putVal(r, "last_error", nil)
		putVal(r, "registered_at", proc.RegisteredAt)
		rows = append(rows, r)
	}
	return rows
}

// ─────────────────────────── sys.columns ─────────────────────────────────

func sysColumnsRows(env ExecEnv) []Row {
	var rows []Row
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			schema, tableName := splitObjectName(t.Name)
			for i, c := range t.Cols {
				r := make(Row)
				putVal(r, "tenant", tn)
				putVal(r, "schema", schema)
				putVal(r, "table_name", tableName)
				putVal(r, "full_table_name", catalogDisplayName(schema, tableName))
				putVal(r, "name", c.Name)
				putVal(r, "position", i+1)
				putVal(r, "data_type", c.Type.String())
				putVal(r, "constraint", constraintStr(c.Constraint))
				putVal(r, "is_nullable", c.Constraint != storage.PrimaryKey)
				if c.ForeignKey != nil {
					putVal(r, "fk_table", c.ForeignKey.Table)
					putVal(r, "fk_column", c.ForeignKey.Column)
				} else {
					putVal(r, "fk_table", nil)
					putVal(r, "fk_column", nil)
				}
				rows = append(rows, r)
			}
		}
	}
	return rows
}

// ─────────────────────────── sys.constraints ─────────────────────────────

func sysConstraintsRows(env ExecEnv) []Row {
	var rows []Row
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			schema, tableName := splitObjectName(t.Name)
			for _, c := range t.Cols {
				if c.Constraint == storage.NoConstraint {
					continue
				}
				r := make(Row)
				putVal(r, "tenant", tn)
				putVal(r, "schema", schema)
				putVal(r, "table_name", tableName)
				putVal(r, "full_table_name", catalogDisplayName(schema, tableName))
				putVal(r, "column_name", c.Name)
				putVal(r, "constraint_type", constraintStr(c.Constraint))
				if c.ForeignKey != nil {
					putVal(r, "fk_table", c.ForeignKey.Table)
					putVal(r, "fk_column", c.ForeignKey.Column)
				} else {
					putVal(r, "fk_table", nil)
					putVal(r, "fk_column", nil)
				}
				rows = append(rows, r)
			}
		}
	}
	return rows
}

// ─────────────────────────── sys.indexes ─────────────────────────────────

func sysIndexesRows(env ExecEnv) []Row {
	indexes := env.db.Catalog().GetIndexes()
	rows := make([]Row, len(indexes))
	for i, idx := range indexes {
		r := make(Row)
		putVal(r, "schema", idx.Schema)
		putVal(r, "name", idx.Name)
		putVal(r, "table_name", idx.Table)
		putVal(r, "columns", strings.Join(idx.Columns, ","))
		putVal(r, "is_unique", idx.Unique)
		putVal(r, "created_at", idx.CreatedAt)
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.views ───────────────────────────────────

func sysViewsRows(env ExecEnv) []Row {
	views := env.db.Catalog().GetViews()
	rows := make([]Row, len(views))
	for i, v := range views {
		r := make(Row)
		putVal(r, "schema", v.Schema)
		putVal(r, "name", v.Name)
		putVal(r, "sql_text", v.SQLText)
		putVal(r, "created_at", v.CreatedAt)
		rows[i] = r
	}
	return rows
}

func sysMaterializedViewsRows(env ExecEnv) []Row {
	views := env.db.Catalog().GetMaterializedViews()
	rows := make([]Row, len(views))
	for i, v := range views {
		r := make(Row)
		putVal(r, "schema", v.Schema)
		putVal(r, "name", v.Name)
		putVal(r, "sql_text", v.SQLText)
		putVal(r, "cache_table_name", v.CacheTableName)
		putVal(r, "stale_after_ms", v.StaleAfterMs)
		putVal(r, "refresh_every_ms", v.RefreshEveryMs)
		putVal(r, "daily_at", v.DailyAt)
		putVal(r, "timezone", v.Timezone)
		putVal(r, "last_refresh_at", v.LastRefreshAt)
		putVal(r, "last_error", v.LastError)
		putVal(r, "is_stale", v.IsStale)
		putVal(r, "invalidate_on_change", v.InvalidateOnChange)
		putVal(r, "is_refreshing", v.IsRefreshing)
		putVal(r, "created_at", v.CreatedAt)
		putVal(r, "updated_at", v.UpdatedAt)
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.functions ───────────────────────────────

func sysFunctionsRows() []Row {
	fns := getAllFunctions()
	names := make([]string, 0, len(fns))
	for k := range fns {
		names = append(names, k)
	}
	sort.Strings(names)

	// Also gather table-valued function names.
	tvfNames := make(map[string]bool)
	for _, name := range listTableFuncNames() {
		tvfNames[strings.ToUpper(name)] = true
	}

	rows := make([]Row, 0, len(names))
	for _, name := range names {
		r := make(Row)
		putVal(r, "name", name)
		fnType := "SCALAR"
		upper := strings.ToUpper(name)
		switch {
		case isAggregateName(upper):
			fnType = "AGGREGATE"
		case isWindowFuncName(upper):
			fnType = "WINDOW"
		case tvfNames[upper]:
			fnType = "TABLE"
		}
		putVal(r, "function_type", fnType)
		putVal(r, "language", "BUILTIN")
		rows = append(rows, r)
	}

	// Add table-valued functions that are not already in the scalar registry.
	for _, tn := range listTableFuncNames() {
		upper := strings.ToUpper(tn)
		if _, exists := fns[upper]; !exists {
			r := make(Row)
			putVal(r, "name", upper)
			putVal(r, "function_type", "TABLE")
			putVal(r, "language", "BUILTIN")
			rows = append(rows, r)
		}
	}

	return rows
}

// ─────────────────────────── sys.procedures ──────────────────────────────

func sysProceduresRows() []Row {
	procs := ListStoredProcedures()
	rows := make([]Row, 0, len(procs))
	for _, proc := range procs {
		r := make(Row)
		putVal(r, "name", proc.Name)
		putVal(r, "language", "GO")
		putVal(r, "storage", "MEMORY")
		putVal(r, "registered_at", proc.RegisteredAt)
		rows = append(rows, r)
	}
	return rows
}

// isAggregateName returns true for known aggregate function names.
func isAggregateName(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX",
		"GROUP_CONCAT", "STRING_AGG",
		"FIRST", "LAST",
		"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX":
		return true
	}
	return false
}

// isWindowFuncName returns true for known window function names.
func isWindowFuncName(name string) bool {
	switch name {
	case "ROW_NUMBER", "RANK", "DENSE_RANK",
		"LAG", "LEAD",
		"FIRST_VALUE", "LAST_VALUE",
		"MOVING_SUM", "MOVING_AVG",
		"NTILE", "PERCENT_RANK", "CUME_DIST":
		return true
	}
	return false
}

// listTableFuncNames returns the names of all registered table-valued functions.
func listTableFuncNames() []string {
	// tableFuncRegistry is in table_functions.go.
	names := make([]string, 0, len(tableFuncRegistry))
	for k := range tableFuncRegistry {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ─────────────────────────── sys.variables ───────────────────────────────

func sysVariablesRows(env ExecEnv) []Row {
	vars := [][2]string{
		{"version", Version()},
		{"go_version", runtime.Version()},
		{"os", runtime.GOOS},
		{"arch", runtime.GOARCH},
		{"storage_mode", env.db.StorageMode().String()},
		{"pid", fmt.Sprintf("%d", os.Getpid())},
		{"max_procs", fmt.Sprintf("%d", runtime.GOMAXPROCS(0))},
		{"num_cpu", fmt.Sprintf("%d", runtime.NumCPU())},
	}
	if cfg := env.db.Config(); cfg != nil {
		vars = append(vars, [2]string{"db_path", cfg.Path})
		vars = append(vars, [2]string{"sync_on_mutate", fmt.Sprintf("%t", cfg.SyncOnMutate)})
		vars = append(vars, [2]string{"compress_files", fmt.Sprintf("%t", cfg.CompressFiles)})
	}
	rows := make([]Row, len(vars))
	for i, kv := range vars {
		r := make(Row)
		putVal(r, "name", kv[0])
		putVal(r, "value", kv[1])
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.status ──────────────────────────────────

func sysStatusRows() []Row {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	uptime := time.Since(startTime).Truncate(time.Second)

	kv := [][2]string{
		{"uptime", uptime.String()},
		{"uptime_seconds", fmt.Sprintf("%d", int64(uptime.Seconds()))},
		{"goroutines", fmt.Sprintf("%d", runtime.NumGoroutine())},
		{"go_version", runtime.Version()},
		{"os", runtime.GOOS},
		{"arch", runtime.GOARCH},
		{"num_cpu", fmt.Sprintf("%d", runtime.NumCPU())},
		{"max_procs", fmt.Sprintf("%d", runtime.GOMAXPROCS(0))},
		{"pid", fmt.Sprintf("%d", os.Getpid())},
		{"heap_alloc_mb", fmt.Sprintf("%.2f", float64(mem.HeapAlloc)/1024/1024)},
		{"sys_mb", fmt.Sprintf("%.2f", float64(mem.Sys)/1024/1024)},
	}
	kv = append(kv, runtimeStatusMemoryRows(mem)...)

	rows := make([]Row, len(kv))
	for i, pair := range kv {
		r := make(Row)
		putVal(r, "key", pair[0])
		putVal(r, "value", pair[1])
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.memory ──────────────────────────────────

func sysMemoryRows() []Row {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	kv := [][2]string{
		{"alloc_bytes", fmt.Sprintf("%d", mem.Alloc)},
		{"alloc_mb", fmt.Sprintf("%.2f", float64(mem.Alloc)/1024/1024)},
		{"total_alloc_bytes", fmt.Sprintf("%d", mem.TotalAlloc)},
		{"total_alloc_mb", fmt.Sprintf("%.2f", float64(mem.TotalAlloc)/1024/1024)},
		{"sys_bytes", fmt.Sprintf("%d", mem.Sys)},
		{"sys_mb", fmt.Sprintf("%.2f", float64(mem.Sys)/1024/1024)},
		{"heap_alloc_bytes", fmt.Sprintf("%d", mem.HeapAlloc)},
		{"heap_alloc_mb", fmt.Sprintf("%.2f", float64(mem.HeapAlloc)/1024/1024)},
		{"heap_sys_bytes", fmt.Sprintf("%d", mem.HeapSys)},
		{"heap_idle_bytes", fmt.Sprintf("%d", mem.HeapIdle)},
		{"heap_inuse_bytes", fmt.Sprintf("%d", mem.HeapInuse)},
		{"heap_released_bytes", fmt.Sprintf("%d", mem.HeapReleased)},
		{"heap_objects", fmt.Sprintf("%d", mem.HeapObjects)},
		{"num_goroutine", fmt.Sprintf("%d", runtime.NumGoroutine())},
	}
	kv = append(kv, runtimeDetailedMemoryRows(mem)...)

	rows := make([]Row, len(kv))
	for i, pair := range kv {
		r := make(Row)
		putVal(r, "key", pair[0])
		putVal(r, "value", pair[1])
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.storage ─────────────────────────────────

func sysStorageRows(env ExecEnv) []Row {
	stats := env.db.BackendStats()

	kv := [][2]string{
		{"mode", stats.Mode.String()},
		{"tables_in_memory", fmt.Sprintf("%d", stats.TablesInMemory)},
		{"tables_on_disk", fmt.Sprintf("%d", stats.TablesOnDisk)},
		{"memory_used_bytes", fmt.Sprintf("%d", stats.MemoryUsedBytes)},
		{"memory_used_mb", fmt.Sprintf("%.2f", float64(stats.MemoryUsedBytes)/1024/1024)},
		{"memory_limit_bytes", fmt.Sprintf("%d", stats.MemoryLimitBytes)},
		{"memory_limit_mb", fmt.Sprintf("%.2f", float64(stats.MemoryLimitBytes)/1024/1024)},
		{"disk_used_bytes", fmt.Sprintf("%d", stats.DiskUsedBytes)},
		{"disk_used_mb", fmt.Sprintf("%.2f", float64(stats.DiskUsedBytes)/1024/1024)},
		{"cache_hit_rate", fmt.Sprintf("%.4f", stats.CacheHitRate)},
		{"sync_count", fmt.Sprintf("%d", stats.SyncCount)},
		{"load_count", fmt.Sprintf("%d", stats.LoadCount)},
		{"eviction_count", fmt.Sprintf("%d", stats.EvictionCount)},
	}

	rows := make([]Row, len(kv))
	for i, pair := range kv {
		r := make(Row)
		putVal(r, "key", pair[0])
		putVal(r, "value", pair[1])
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.config ──────────────────────────────────

func sysConfigRows(env ExecEnv) []Row {
	kv := [][2]string{
		{"storage_mode", env.db.StorageMode().String()},
	}

	if cfg := env.db.Config(); cfg != nil {
		kv = append(kv, [][2]string{
			{"path", cfg.Path},
			{"max_memory_bytes", fmt.Sprintf("%d", cfg.MaxMemoryBytes)},
			{"sync_on_mutate", fmt.Sprintf("%t", cfg.SyncOnMutate)},
			{"compress_files", fmt.Sprintf("%t", cfg.CompressFiles)},
			{"checkpoint_every", fmt.Sprintf("%d", cfg.CheckpointEvery)},
			{"checkpoint_interval", cfg.CheckpointInterval.String()},
		}...)
	}

	rows := make([]Row, len(kv))
	for i, pair := range kv {
		r := make(Row)
		putVal(r, "key", pair[0])
		putVal(r, "value", pair[1])
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── sys.connections ─────────────────────────────

func sysConnectionsRows(env ExecEnv) []Row {
	tenants := env.db.ListTenants()
	rows := make([]Row, len(tenants))
	for i, tn := range tenants {
		tables := env.db.ListTables(tn)
		totalRows := 0
		for _, t := range tables {
			totalRows += len(t.Rows)
		}
		r := make(Row)
		putVal(r, "tenant", tn)
		putVal(r, "table_count", len(tables))
		putVal(r, "total_rows", totalRows)
		rows[i] = r
	}
	return rows
}

// ─────────────────────────── helpers ─────────────────────────────────────

func constraintStr(c storage.ConstraintType) string {
	switch c {
	case storage.PrimaryKey:
		return "PRIMARY KEY"
	case storage.ForeignKey:
		return "FOREIGN KEY"
	case storage.Unique:
		return "UNIQUE"
	default:
		return "NONE"
	}
}

// Version returns the tinySQL version string. It attempts to read the
// module version from debug.ReadBuildInfo and falls back to "dev".
func Version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

// ─────────────────────────── sys.triggers ────────────────────────────────────

func sysTriggersRows(env ExecEnv) []Row {
	triggers := env.db.Catalog().ListTriggers()
	rows := make([]Row, 0, len(triggers))
	for _, t := range triggers {
		r := make(Row)
		putVal(r, "name", t.Name)
		putVal(r, "table", t.Table)
		putVal(r, "timing", string(t.Timing))
		putVal(r, "event", string(t.Event))
		putVal(r, "for_each_row", t.ForEachRow)
		putVal(r, "when_expr", t.WhenExpr)
		putVal(r, "body", t.Body)
		putVal(r, "created_at", t.CreatedAt.Format(time.RFC3339))
		rows = append(rows, r)
	}
	return rows
}
