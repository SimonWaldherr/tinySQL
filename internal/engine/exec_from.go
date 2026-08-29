// Resolving a FROM clause to rows: physical tables, subqueries, CTE results,
// table-valued functions, views, materialized views and the sys.* catalog
// views.
package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// resolveFromClause resolves the FROM clause of a SELECT statement and returns the initial rows.
// It handles: no FROM (dummy row), subqueries, table functions, CTEs, catalog tables, sys tables, and regular tables.
func resolveFromClause(env ExecEnv, cteEnv ExecEnv, s *Select) ([]Row, error) {
	// Check if FROM clause exists
	if s.From.Table == "" && s.From.Subquery == nil && s.From.TableFunc == nil {
		// No FROM clause - create a single dummy row for expression evaluation
		return []Row{make(Row)}, nil
	}

	if s.From.Subquery != nil {
		return resolveSubquery(cteEnv, s)
	}

	if s.From.TableFunc != nil {
		return resolveTableFunc(cteEnv, s)
	}

	// No subquery and no table function: this can be a CTE name, a virtual
	// catalog table (catalog.*), a sys table (sys.*), or a regular table.
	return resolveTableSource(cteEnv, env, s)
}

// resolveSubquery handles FROM (SELECT ...) AS alias
func resolveSubquery(env ExecEnv, s *Select) ([]Row, error) {
	subResult, err := executeSelect(env, s.From.Subquery)
	if err != nil {
		return nil, err
	}
	return aliasRows(subResult.Rows, s.From.Alias), nil
}

// derivedKeys are the output map keys a source key contributes: the key itself
// lower-cased, and the same prefixed with the FROM alias.
type derivedKeys struct {
	plain     string
	qualified string
}

// aliasRows re-keys rows for the outer select: every key lower-cased, and
// additionally alias-qualified when the FROM item has an alias.
//
// Both derived keys depend only on the source key and the alias, so they are
// computed once per distinct key rather than once per cell. That matters most
// for table functions -- the parser defaults a table function's alias to the
// function name, so the qualified branch always runs, and building
// alias+"."+key per cell dominated the allocation profile of every VEC_SEARCH,
// RAG_SEARCH, HYBRID_SEARCH and FTS_SEARCH query.
func aliasRows(rows []Row, alias string) []Row {
	out := make([]Row, len(rows))
	qualify := alias != ""
	width := 1
	if qualify {
		width = 2
	}
	// strings.ToLower maps rune-wise, so lowering the alias and the key
	// separately gives the same string as lowering the concatenation.
	lowerAlias := strings.ToLower(alias)
	keys := make(map[string]derivedKeys)
	for i, row := range rows {
		dst := make(Row, len(row)*width)
		for k, v := range row {
			dk, ok := keys[k]
			if !ok {
				dk.plain = strings.ToLower(k)
				if qualify {
					dk.qualified = lowerAlias + "." + dk.plain
				}
				keys[k] = dk
			}
			dst[dk.plain] = v
			if qualify {
				dst[dk.qualified] = v
			}
		}
		out[i] = dst
	}
	return out
}

// resolveTableFunc handles FROM table-valued function
func resolveTableFunc(env ExecEnv, s *Select) ([]Row, error) {
	fnName := s.From.TableFunc.Name
	tf, ok := GetTableFunc(fnName)
	if !ok {
		return nil, fmt.Errorf("unknown table function: %s", fnName)
	}
	// Validate args optionally
	if err := tf.ValidateArgs(s.From.TableFunc.Args); err != nil {
		return nil, fmt.Errorf("%s: %v", fnName, err)
	}
	// Execute table function (no correlated row for top-level FROM)
	rs, err := tf.Execute(env.ctx, s.From.TableFunc.Args, env, nil)
	if err != nil {
		return nil, err
	}
	return aliasRows(rs.Rows, s.From.Alias), nil
}

// resolveTableSource handles CTE, catalog, sys, or regular table resolution
func resolveTableSource(cteEnv ExecEnv, env ExecEnv, s *Select) ([]Row, error) {
	// Prefer CTE binding first
	if cteEnv.ctes != nil {
		cteName := strings.ToLower(s.From.Table)
		if cteResult, exists := cteEnv.ctes[cteName]; exists {
			return rowsFromCTEResult(cteEnv, cteResult, s.From), nil
		}
	}

	// Handle virtual catalog.* tables
	if strings.HasPrefix(strings.ToLower(s.From.Table), "catalog.") {
		return resolveCatalogTable(env, s)
	}

	// Handle virtual sys.* tables
	if strings.HasPrefix(strings.ToLower(s.From.Table), "sys.") {
		return resolveSysVirtualTable(env, s)
	}

	if isSQLiteSchemaTable(s.From.Table) {
		return resolveSQLiteSchemaTable(env, s), nil
	}

	// Treat as a regular table
	return resolveRegularTable(cteEnv, env, s)
}

// rowsFromCTEResult materializes a CTE ResultSet as a FROM/JOIN source. It
// retains unqualified names plus both the CTE name and alias qualifiers, so a
// CTE behaves identically whether it appears on the left of FROM or right of
// a JOIN. Non-recursive CTEs memoize this conversion per source qualifier:
// converting their result repeatedly is otherwise pure allocation and map
// copy work when earlier sibling CTEs or nested queries reuse them.
func rowsFromCTEResult(env ExecEnv, cteResult *ResultSet, source FromItem) []Row {
	cteName := strings.ToLower(source.Table)
	qualifier := cteName
	if source.Alias != "" {
		qualifier = strings.ToLower(source.Alias)
	}
	keysPerColumn := 2
	if qualifier != cteName {
		keysPerColumn++
	}
	var cacheKey cteRowCacheKey
	if cteResult.cteCacheable && env.cteRowCache != nil {
		cacheKey = cteRowCacheKey{result: cteResult, cteName: cteName, qualifier: qualifier}
		if rows, ok := env.cteRowCache.load(cacheKey); ok {
			return rows
		}
	}
	columnKeys := make([]string, len(cteResult.Cols))
	cteKeys := make([]string, len(cteResult.Cols))
	qualifierKeys := make([]string, len(cteResult.Cols))
	for i, col := range cteResult.Cols {
		columnKeys[i] = strings.ToLower(col)
		cteKeys[i] = cteName + "." + columnKeys[i]
		if qualifier != cteName {
			qualifierKeys[i] = qualifier + "." + columnKeys[i]
		}
	}
	rows := make([]Row, len(cteResult.Rows))
	for i, row := range cteResult.Rows {
		// The row contains the unqualified value, the CTE-qualified value,
		// and (when different) the source-alias-qualified value.
		out := make(Row, len(cteResult.Cols)*keysPerColumn)
		for colIdx, key := range columnKeys {
			v, ok := getValLower(row, key)
			if !ok {
				continue
			}
			out[key] = v
			out[cteKeys[colIdx]] = v
			if qualifier != cteName {
				out[qualifierKeys[colIdx]] = v
			}
		}
		rows[i] = out
	}
	if cteResult.cteCacheable && env.cteRowCache != nil {
		return env.cteRowCache.store(cacheKey, rows)
	}
	return rows
}

func resultSetTable(name string, cols []string) *storage.Table {
	tableCols := make([]storage.Column, 0, len(cols))
	for _, col := range cols {
		tableCols = append(tableCols, storage.Column{Name: col})
	}
	return &storage.Table{Name: name, Cols: tableCols}
}

// resolveCatalogTable handles catalog.* virtual tables
func resolveCatalogTable(env ExecEnv, s *Select) ([]Row, error) {
	parts := strings.SplitN(s.From.Table, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid catalog reference: %s", s.From.Table)
	}
	name := strings.ToLower(parts[1])

	switch name {
	case "objects":
		return allObjectStatusRows(env), nil
	case "dependencies":
		return dependencyRows(env), nil
	case "tables":
		return resolveCatalogTables(env, s)
	case "columns":
		return resolveCatalogColumns(env, s)
	case "functions":
		return resolveCatalogFunctions(env, s)
	case "jobs":
		return resolveCatalogJobs(env, s)
	case "job_history":
		return resolveCatalogJobHistory(env, s)
	case "views":
		return resolveCatalogViews(env, s)
	case "materialized_views":
		return resolveCatalogMaterializedViews(env, s)
	default:
		return nil, fmt.Errorf("unknown catalog table: %s", name)
	}
}

// resolveCatalogTables handles catalog.tables
func resolveCatalogTables(env ExecEnv, s *Select) ([]Row, error) {
	// Auto-populate from real tables, then merge catalog-only entries.
	leftRows := sysTablesRows(env)
	catTabs := env.db.Catalog().GetTables()
	catMap := make(map[string]*storage.CatalogTable, len(catTabs))
	for _, ct := range catTabs {
		catMap[strings.ToLower(ct.Schema+"."+ct.Name)] = ct
	}
	// Track which real tables we've seen.
	seen := make(map[string]bool, len(leftRows))
	for _, r := range leftRows {
		tName, _ := r["name"].(string)
		tSchema, _ := r["schema"].(string)
		if tSchema == "" {
			tSchema = "main"
		}
		key := strings.ToLower(tSchema + "." + tName)
		seen[key] = true
		if ct, ok := catMap[key]; ok {
			putVal(r, "schema", ct.Schema)
			putVal(r, "type", ct.Type)
			putVal(r, "row_count", ct.RowCount)
			putVal(r, "rows", ct.RowCount)
			putVal(r, "created_at", ct.CreatedAt)
			putVal(r, "updated_at", ct.UpdatedAt)
		} else {
			putVal(r, "schema", tSchema)
			putVal(r, "type", "TABLE")
		}
	}
	// Add catalog-only entries that aren't real tables yet.
	for _, ct := range catTabs {
		if seen[strings.ToLower(ct.Schema+"."+ct.Name)] {
			continue
		}
		r := make(Row)
		putVal(r, "schema", ct.Schema)
		putVal(r, "name", ct.Name)
		putVal(r, "full_name", catalogDisplayName(ct.Schema, ct.Name))
		putVal(r, "type", ct.Type)
		putVal(r, "row_count", ct.RowCount)
		putVal(r, "rows", ct.RowCount)
		putVal(r, "created_at", ct.CreatedAt)
		putVal(r, "updated_at", ct.UpdatedAt)
		if s.From.Alias != "" {
			putVal(r, s.From.Alias+".schema", ct.Schema)
			putVal(r, s.From.Alias+".name", ct.Name)
		}
		leftRows = append(leftRows, r)
	}
	return leftRows, nil
}

// resolveCatalogColumns handles catalog.columns
func resolveCatalogColumns(env ExecEnv, s *Select) ([]Row, error) {
	cols := env.db.Catalog().GetAllColumns()
	leftRows := make([]Row, len(cols))
	for i, c := range cols {
		leftRows[i] = make(Row)
		putVal(leftRows[i], "schema", c.Schema)
		putVal(leftRows[i], "table_name", c.TableName)
		putVal(leftRows[i], "name", c.Name)
		putVal(leftRows[i], "position", c.Position)
		putVal(leftRows[i], "data_type", c.DataType)
		putVal(leftRows[i], "is_nullable", c.IsNullable)
		if c.DefaultValue != nil {
			putVal(leftRows[i], "default_value", *c.DefaultValue)
		} else {
			putVal(leftRows[i], "default_value", nil)
		}
		if s.From.Alias != "" {
			putVal(leftRows[i], s.From.Alias+".table_name", c.TableName)
		}
	}
	return leftRows, nil
}

// resolveCatalogFunctions handles catalog.functions
func resolveCatalogFunctions(env ExecEnv, s *Select) ([]Row, error) {
	// Auto-populate from real function registry, then overlay catalog entries.
	leftRows := sysFunctionsRows()
	catFns := env.db.Catalog().GetFunctions()
	catMap := make(map[string]*storage.CatalogFunction, len(catFns))
	for _, cf := range catFns {
		catMap[strings.ToUpper(cf.Name)] = cf
	}
	// Track seen function names.
	seen := make(map[string]bool, len(leftRows))
	for _, r := range leftRows {
		name, _ := r["name"].(string)
		seen[strings.ToUpper(name)] = true
		if cf, ok := catMap[strings.ToUpper(name)]; ok {
			putVal(r, "schema", cf.Schema)
			if cf.Description != "" {
				putVal(r, "description", cf.Description)
			}
			if cf.ReturnType != "" {
				putVal(r, "return_type", cf.ReturnType)
			}
			if cf.IsDeterministic {
				putVal(r, "is_deterministic", cf.IsDeterministic)
			}
		}
	}
	// Add catalog-only functions not in the builtin registry.
	for _, cf := range catFns {
		if seen[strings.ToUpper(cf.Name)] {
			continue
		}
		r := make(Row)
		putVal(r, "schema", cf.Schema)
		putVal(r, "name", cf.Name)
		putVal(r, "function_type", cf.FunctionType)
		putVal(r, "return_type", cf.ReturnType)
		putVal(r, "language", cf.Language)
		putVal(r, "is_deterministic", cf.IsDeterministic)
		putVal(r, "description", cf.Description)
		leftRows = append(leftRows, r)
	}
	return leftRows, nil
}

// resolveCatalogJobs handles catalog.jobs
func resolveCatalogJobs(env ExecEnv, s *Select) ([]Row, error) {
	jobs := env.db.Catalog().ListJobs()
	leftRows := make([]Row, len(jobs))
	for i, j := range jobs {
		leftRows[i] = make(Row)
		putVal(leftRows[i], "name", j.Name)
		putVal(leftRows[i], "sql_text", j.SQLText)
		putVal(leftRows[i], "schedule_type", j.ScheduleType)
		putVal(leftRows[i], "cron_expr", j.CronExpr)
		putVal(leftRows[i], "interval_ms", j.IntervalMs)
		putVal(leftRows[i], "run_at", j.RunAt)
		putVal(leftRows[i], "timezone", j.Timezone)
		putVal(leftRows[i], "enabled", j.Enabled)
		putVal(leftRows[i], "catch_up", j.CatchUp)
		putVal(leftRows[i], "no_overlap", j.NoOverlap)
		putVal(leftRows[i], "max_runtime_ms", j.MaxRuntimeMs)
		putVal(leftRows[i], "last_run_at", j.LastRunAt)
		putVal(leftRows[i], "next_run_at", j.NextRunAt)
		putVal(leftRows[i], "created_at", j.CreatedAt)
		putVal(leftRows[i], "updated_at", j.UpdatedAt)
	}
	return leftRows, nil
}

// resolveCatalogJobHistory handles catalog.job_history
func resolveCatalogJobHistory(env ExecEnv, s *Select) ([]Row, error) {
	runs := env.db.Catalog().ListJobHistory()
	leftRows := make([]Row, len(runs))
	for i, run := range runs {
		leftRows[i] = make(Row)
		putVal(leftRows[i], "run_id", run.RunID)
		putVal(leftRows[i], "job_name", run.JobName)
		putVal(leftRows[i], "started_at", run.StartedAt)
		putVal(leftRows[i], "finished_at", run.FinishedAt)
		putVal(leftRows[i], "duration_ms", run.DurationMs)
		putVal(leftRows[i], "status", run.Status)
		putVal(leftRows[i], "error_message", run.ErrorMessage)
	}
	return leftRows, nil
}

// resolveCatalogViews handles catalog.views
func resolveCatalogViews(env ExecEnv, s *Select) ([]Row, error) {
	views := env.db.Catalog().GetViews()
	leftRows := make([]Row, len(views))
	for i, v := range views {
		leftRows[i] = make(Row)
		putVal(leftRows[i], "schema", v.Schema)
		putVal(leftRows[i], "name", v.Name)
		putVal(leftRows[i], "sql_text", v.SQLText)
		putVal(leftRows[i], "created_at", v.CreatedAt)
	}
	return leftRows, nil
}

// resolveCatalogMaterializedViews handles catalog.materialized_views
func resolveCatalogMaterializedViews(env ExecEnv, s *Select) ([]Row, error) {
	views := env.db.Catalog().GetMaterializedViews()
	leftRows := make([]Row, len(views))
	for i, v := range views {
		leftRows[i] = make(Row)
		putVal(leftRows[i], "schema", v.Schema)
		putVal(leftRows[i], "name", v.Name)
		putVal(leftRows[i], "sql_text", v.SQLText)
		putVal(leftRows[i], "cache_table_name", v.CacheTableName)
		putVal(leftRows[i], "stale_after_ms", v.StaleAfterMs)
		putVal(leftRows[i], "refresh_every_ms", v.RefreshEveryMs)
		putVal(leftRows[i], "daily_at", v.DailyAt)
		putVal(leftRows[i], "timezone", v.Timezone)
		putVal(leftRows[i], "with_data", v.WithData)
		putVal(leftRows[i], "last_refresh_at", v.LastRefreshAt)
		putVal(leftRows[i], "last_duration_ms", v.LastDurationMs)
		putVal(leftRows[i], "last_error", v.LastError)
		putVal(leftRows[i], "is_stale", v.IsStale)
		putVal(leftRows[i], "invalidate_on_change", v.InvalidateOnChange)
		putVal(leftRows[i], "is_refreshing", v.IsRefreshing)
		putVal(leftRows[i], "created_at", v.CreatedAt)
		putVal(leftRows[i], "updated_at", v.UpdatedAt)
	}
	return leftRows, nil
}

// resolveSysVirtualTable handles sys.* virtual tables
func resolveSysVirtualTable(env ExecEnv, s *Select) ([]Row, error) {
	parts := strings.SplitN(s.From.Table, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid sys reference: %s", s.From.Table)
	}
	name := strings.ToLower(parts[1])
	sysRows, err := resolveSysTable(env, name)
	if err != nil {
		return nil, err
	}
	leftRows := sysRows
	// Apply alias if present.
	if s.From.Alias != "" {
		for _, r := range leftRows {
			for k, v := range r {
				if !strings.Contains(k, ".") {
					r[s.From.Alias+"."+k] = v
				}
			}
		}
	}
	return leftRows, nil
}

// resolveRegularTable handles regular table lookup
func resolveRegularTable(cteEnv ExecEnv, env ExecEnv, s *Select) ([]Row, error) {
	if isSQLiteSchemaTable(s.From.Table) {
		return resolveSQLiteSchemaTable(env, s), nil
	}

	var leftT *storage.Table
	var err error
	if cteEnv.ctes != nil {
		leftT, err = cteEnv.db.Get(cteEnv.tenant, s.From.Table)
	} else {
		leftT, err = env.db.Get(env.tenant, s.From.Table)
	}
	if err == nil {
		leftRows, _ := rowsFromTable(leftT, aliasOr(s.From))
		return leftRows, nil
	}

	if rows, found, viewErr := resolveMaterializedViewSource(env, s); found || viewErr != nil {
		return rows, viewErr
	}
	if rows, found, viewErr := resolveViewSource(env, s); found || viewErr != nil {
		return rows, viewErr
	}
	return nil, err
}

func resolveMaterializedViewSource(env ExecEnv, s *Select) ([]Row, bool, error) {
	schema, name := splitObjectName(s.From.Table)
	mv, ok := env.db.Catalog().GetMaterializedView(schema, name)
	if !ok {
		return nil, false, nil
	}
	cache, err := ensureMaterializedViewCache(env, s.From.Table, mv)
	if err != nil {
		return nil, true, err
	}
	leftRows, _ := rowsFromTable(cache, aliasOr(s.From))
	return leftRows, true, nil
}

func ensureMaterializedViewCache(env ExecEnv, sourceName string, mv *storage.CatalogMaterializedView) (*storage.Table, error) {
	cache, cacheErr := env.db.Get(env.tenant, mv.CacheTableName)
	cacheExists := cacheErr == nil

	needsRefresh := false
	if !cacheExists {
		needsRefresh = mv.WithData || mv.StaleAfterMs > 0
		if !needsRefresh {
			return nil, fmt.Errorf("materialized view %q has no data", sourceName)
		}
	} else if mv.IsStale {
		needsRefresh = true
	} else if mv.StaleAfterMs > 0 {
		if mv.LastRefreshAt == nil {
			needsRefresh = true
		} else {
			needsRefresh = time.Since(*mv.LastRefreshAt) >= time.Duration(mv.StaleAfterMs)*time.Millisecond
		}
	}

	if needsRefresh {
		if err := refreshMaterializedView(env, sourceName); err != nil && !cacheExists {
			return nil, err
		}
		refreshed, err := env.db.Get(env.tenant, mv.CacheTableName)
		if err == nil {
			cache = refreshed
			cacheExists = true
		}
	}
	if !cacheExists {
		return nil, cacheErr
	}
	return cache, nil
}

func resolveViewSource(env ExecEnv, s *Select) ([]Row, bool, error) {
	schema, name := splitObjectName(s.From.Table)
	view, ok := env.db.Catalog().GetView(schema, name)
	if !ok {
		return nil, false, nil
	}
	if env.viewDepth >= 16 {
		return nil, true, fmt.Errorf("view expansion exceeded depth limit")
	}
	stmt, err := NewParser(view.SQLText).ParseStatement()
	if err != nil {
		return nil, true, fmt.Errorf("view %q parse failed: %w", view.Name, err)
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return nil, true, fmt.Errorf("view %q query is not a SELECT", view.Name)
	}
	viewEnv := env
	viewEnv.ctes = nil
	viewEnv.viewDepth++
	rs, err := executeSelect(viewEnv, sel)
	if err != nil {
		return nil, true, err
	}
	return rowsFromResultSet(rs, aliasOr(s.From)), true, nil
}

func rowsFromResultSet(rs *ResultSet, alias string) []Row {
	if rs == nil {
		return nil
	}
	rows := make([]Row, len(rs.Rows))
	for i, r := range rs.Rows {
		out := make(Row, len(rs.Cols)*2)
		for _, c := range rs.Cols {
			key := strings.ToLower(c)
			val, ok := getValLower(r, key)
			if !ok {
				continue
			}
			putVal(out, c, val)
			if alias != "" {
				putVal(out, alias+"."+c, val)
			}
		}
		rows[i] = out
	}
	return rows
}

// applyDistinctOn applies DISTINCT ON semantics: keep first row per distinct-on key.
// The ORDER BY clause controls which row is considered "first"; so it applies ORDER BY first if present.
func applyDistinctOn(env ExecEnv, s *Select, outRows []Row) ([]Row, error) {
	// If ORDER BY present, sort now so the first row per key is the right one
	if len(s.OrderBy) > 0 {
		outRows = applySortOrder(s.OrderBy, outRows)
	}
	seen := make(map[string]bool)
	var res []Row
	// keyBuf is reused across rows via buf[:0] and encoded with the same
	// self-delimiting writeFmtKeyPart scheme GROUP BY/PIVOT/DISTINCT/set-op
	// dedup already use (row_helpers.go) instead of fmt.Sprintf+strings.Join:
	// no per-value reflection walk, no per-row []string/Join allocation, and
	// (as a side effect of self-delimiting parts) no risk of two different
	// value tuples colliding because a string value happened to contain the
	// old "|" separator literally.
	keyBuf := make([]byte, 0, 64)
	for _, r := range outRows {
		keyBuf = keyBuf[:0]
		for _, e := range s.DistinctOn {
			v, err := evalExpr(env, e, r)
			if err != nil {
				return nil, err
			}
			keyBuf = writeFmtKeyPart(keyBuf, v)
		}
		if seen[string(keyBuf)] {
			continue
		}
		seen[string(keyBuf)] = true
		res = append(res, r)
	}
	return res, nil
}
