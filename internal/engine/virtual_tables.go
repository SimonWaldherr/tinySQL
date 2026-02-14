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
//   sys.indexes      – index metadata (stub — CREATE INDEX is a no-op)
//   sys.views        – same as catalog.views but auto-populated
//   sys.functions    – all registered functions (builtin + extended + vector)
//   sys.variables    – server variables (version, pid, mode, …)
//   sys.status       – runtime status (goroutines, uptime, Go version, …)
//   sys.memory       – Go runtime memory statistics
//   sys.storage      – storage backend statistics
//   sys.config       – database configuration
//   sys.connections  – active tenant / connection info
// ============================================================================

// startTime records when the process started so sys.status can report uptime.
var startTime = time.Now()

// resolveSysTable returns the rows for a given sys.<name> virtual table.
// It returns nil, nil when the name is not recognised so the caller can fall
// back to the default "unknown catalog/sys table" error.
func resolveSysTable(env ExecEnv, name string) ([]Row, error) {
	switch name {
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
	case "functions":
		return sysFunctionsRows(), nil
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
	default:
		return nil, fmt.Errorf("unknown sys table: %s", name)
	}
}

// ─────────────────────────── sys.tables ──────────────────────────────────

func sysTablesRows(env ExecEnv) []Row {
	var rows []Row
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			r := make(Row)
			putVal(r, "tenant", tn)
			putVal(r, "name", t.Name)
			putVal(r, "columns", len(t.Cols))
			putVal(r, "rows", len(t.Rows))
			putVal(r, "is_temp", t.IsTemp)
			putVal(r, "version", t.Version)
			rows = append(rows, r)
		}
	}
	return rows
}

// ─────────────────────────── sys.columns ─────────────────────────────────

func sysColumnsRows(env ExecEnv) []Row {
	var rows []Row
	for _, tn := range env.db.ListTenants() {
		for _, t := range env.db.ListTables(tn) {
			for i, c := range t.Cols {
				r := make(Row)
				putVal(r, "tenant", tn)
				putVal(r, "table_name", t.Name)
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
			for _, c := range t.Cols {
				if c.Constraint == storage.NoConstraint {
					continue
				}
				r := make(Row)
				putVal(r, "tenant", tn)
				putVal(r, "table_name", t.Name)
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

func sysIndexesRows(_ ExecEnv) []Row {
	// CREATE INDEX is currently a no-op in tinySQL – return empty.
	return []Row{}
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
		{"gc_runs", fmt.Sprintf("%d", mem.NumGC)},
		{"heap_alloc_mb", fmt.Sprintf("%.2f", float64(mem.HeapAlloc)/1024/1024)},
		{"sys_mb", fmt.Sprintf("%.2f", float64(mem.Sys)/1024/1024)},
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
		{"stack_inuse_bytes", fmt.Sprintf("%d", mem.StackInuse)},
		{"stack_sys_bytes", fmt.Sprintf("%d", mem.StackSys)},
		{"gc_runs", fmt.Sprintf("%d", mem.NumGC)},
		{"gc_pause_total_ns", fmt.Sprintf("%d", mem.PauseTotalNs)},
		{"gc_pause_total_ms", fmt.Sprintf("%.2f", float64(mem.PauseTotalNs)/1e6)},
		{"gc_cpu_fraction", fmt.Sprintf("%.6f", mem.GCCPUFraction)},
		{"num_goroutine", fmt.Sprintf("%d", runtime.NumGoroutine())},
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
