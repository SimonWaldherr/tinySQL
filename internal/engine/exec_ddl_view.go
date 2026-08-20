// Views, materialized views and the jobs that refresh them: CREATE, DROP and
// ALTER for each, the cache table a materialized view is backed by, and the
// scheduled-refresh registration that keeps it fresh.
package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeCreateView(env ExecEnv, s *CreateView) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	if _, exists := env.db.Catalog().GetView(schema, name); exists {
		if s.IfNotExists && !s.OrReplace {
			return nil, nil
		}
		if !s.OrReplace {
			return nil, fmt.Errorf("view %q already exists", s.Name)
		}
	}
	sqlText := s.SQLText
	if strings.TrimSpace(sqlText) == "" {
		return nil, fmt.Errorf("CREATE VIEW %s missing stored SQL text", s.Name)
	}
	if err := env.db.Catalog().RegisterView(schema, name, sqlText); err != nil {
		return nil, err
	}
	env.db.Catalog().SetDependencies(schema, name, "VIEW", selectDependencies(env.db.Catalog(), schema, name, "VIEW", s.Select))
	return nil, nil
}

func executeCreateMaterializedView(env ExecEnv, s *CreateMaterializedView) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	if _, exists := env.db.Catalog().GetMaterializedView(schema, name); exists {
		if s.IfNotExists && !s.OrReplace {
			return nil, nil
		}
		if !s.OrReplace {
			return nil, fmt.Errorf("materialized view %q already exists", s.Name)
		}
	}
	if strings.TrimSpace(s.SQLText) == "" {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW %s missing stored SQL text", s.Name)
	}
	mv := &storage.CatalogMaterializedView{
		Schema:             schema,
		Name:               name,
		SQLText:            s.SQLText,
		CacheTableName:     materializedViewCacheTableNameFor(schema, name),
		StaleAfterMs:       s.StaleAfterMs,
		RefreshEveryMs:     s.RefreshEveryMs,
		DailyAt:            s.DailyAt,
		Timezone:           s.Timezone,
		WithData:           s.WithData,
		InvalidateOnChange: s.InvalidateOnChange,
	}
	if err := env.db.Catalog().RegisterMaterializedView(mv); err != nil {
		return nil, err
	}
	env.db.Catalog().SetDependencies(schema, name, "MATERIALIZED_VIEW", selectDependencies(env.db.Catalog(), schema, name, "MATERIALIZED_VIEW", s.Select))
	if err := registerMaterializedViewRefreshJobs(env, mv); err != nil {
		return nil, err
	}
	if s.WithData {
		if err := refreshMaterializedView(env, s.Name); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func executeCreateJob(env ExecEnv, s *CreateJob) (*ResultSet, error) {
	job := &storage.CatalogJob{
		Name:         s.Name,
		SQLText:      s.SQLText,
		ScheduleType: s.ScheduleType,
		CronExpr:     s.CronExpr,
		IntervalMs:   s.IntervalMs,
		Timezone:     s.Timezone,
		Enabled:      s.Enabled,
		NoOverlap:    s.NoOverlap,
		MaxRuntimeMs: s.MaxRuntimeMs,
		CatchUp:      s.CatchUp,
	}
	if s.RunAt != nil {
		job.RunAt = s.RunAt
	}
	if err := env.db.RegisterJob(job); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeAlterJob(env ExecEnv, s *AlterJob) (*ResultSet, error) {
	job, err := env.db.Catalog().GetJob(s.Name)
	if err != nil {
		return nil, err
	}
	if s.Enable != nil {
		job.Enabled = *s.Enable
	}
	if err := env.db.RegisterJob(job); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropJob(env ExecEnv, s *DropJob) (*ResultSet, error) {
	if err := env.db.DeleteJob(s.Name); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropView(env ExecEnv, s *DropView) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	if s.IfExists {
		if _, ok := env.db.Catalog().GetView(schema, name); !ok {
			return nil, nil
		}
	}
	return nil, env.db.Catalog().DeleteView(schema, name)
}

func executeDropMaterializedView(env ExecEnv, s *DropMaterializedView) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	displayName := catalogDisplayName(schema, name)
	mv, ok := env.db.Catalog().GetMaterializedView(schema, name)
	if !ok {
		if s.IfExists {
			return nil, nil
		}
		return nil, fmt.Errorf("materialized view %q not found", s.Name)
	}
	_ = env.db.DeleteJob(materializedViewIntervalJobName(displayName))
	_ = env.db.DeleteJob(materializedViewDailyJobName(displayName))
	if _, err := env.db.Get(env.tenant, mv.CacheTableName); err == nil {
		_ = env.db.Drop(env.tenant, mv.CacheTableName)
	}
	return nil, env.db.Catalog().DeleteMaterializedView(schema, name)
}

func executeRefreshMaterializedView(env ExecEnv, s *RefreshMaterializedView) (*ResultSet, error) {
	_ = s.Concurrently
	if err := refreshMaterializedView(env, s.Name); err != nil {
		return nil, err
	}
	return &ResultSet{Cols: []string{"refreshed"}, Rows: []Row{{"refreshed": s.Name}}}, nil
}

func executeAlterViewMaterialize(env ExecEnv, s *AlterViewMaterialize) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	view, ok := env.db.Catalog().GetView(schema, name)
	if !ok {
		return nil, fmt.Errorf("view %q not found", s.Name)
	}
	if _, exists := env.db.Catalog().GetMaterializedView(schema, name); exists {
		return nil, fmt.Errorf("materialized view %q already exists", s.Name)
	}
	mv := &storage.CatalogMaterializedView{
		Schema:             schema,
		Name:               name,
		SQLText:            view.SQLText,
		CacheTableName:     materializedViewCacheTableNameFor(schema, name),
		StaleAfterMs:       s.StaleAfterMs,
		RefreshEveryMs:     s.RefreshEveryMs,
		DailyAt:            s.DailyAt,
		Timezone:           s.Timezone,
		WithData:           s.WithData,
		InvalidateOnChange: s.InvalidateOnChange,
		CreatedAt:          view.CreatedAt,
	}
	if err := env.db.Catalog().DeleteView(schema, name); err != nil {
		return nil, err
	}
	if err := env.db.Catalog().RegisterMaterializedView(mv); err != nil {
		_ = env.db.Catalog().RegisterView(schema, name, view.SQLText)
		return nil, err
	}
	stmt, parseErr := NewParser(view.SQLText).ParseStatement()
	if parseErr == nil {
		if sel, ok := stmt.(*Select); ok {
			env.db.Catalog().SetDependencies(schema, name, "MATERIALIZED_VIEW", selectDependencies(env.db.Catalog(), schema, name, "MATERIALIZED_VIEW", sel))
		}
	}
	if err := registerMaterializedViewRefreshJobs(env, mv); err != nil {
		return nil, err
	}
	if s.WithData {
		if err := refreshMaterializedView(env, s.Name); err != nil {
			return nil, err
		}
	}
	return &ResultSet{Cols: []string{"materialized"}, Rows: []Row{{"materialized": s.Name}}}, nil
}

func executeAlterMaterializedViewToView(env ExecEnv, s *AlterMaterializedViewToView) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	displayName := catalogDisplayName(schema, name)
	mv, ok := env.db.Catalog().GetMaterializedView(schema, name)
	if !ok {
		return nil, fmt.Errorf("materialized view %q not found", s.Name)
	}
	if _, exists := env.db.Catalog().GetView(schema, name); exists {
		return nil, fmt.Errorf("view %q already exists", s.Name)
	}
	_ = env.db.DeleteJob(materializedViewIntervalJobName(displayName))
	_ = env.db.DeleteJob(materializedViewDailyJobName(displayName))
	if _, err := env.db.Get(env.tenant, mv.CacheTableName); err == nil {
		_ = env.db.Drop(env.tenant, mv.CacheTableName)
	}
	if err := env.db.Catalog().DeleteMaterializedView(schema, name); err != nil {
		return nil, err
	}
	if err := env.db.Catalog().RegisterView(schema, name, mv.SQLText); err != nil {
		return nil, err
	}
	stmt, parseErr := NewParser(mv.SQLText).ParseStatement()
	if parseErr == nil {
		if sel, ok := stmt.(*Select); ok {
			env.db.Catalog().SetDependencies(schema, name, "VIEW", selectDependencies(env.db.Catalog(), schema, name, "VIEW", sel))
		}
	}
	return &ResultSet{Cols: []string{"view"}, Rows: []Row{{"view": s.Name}}}, nil
}

func materializedViewCacheTableName(name string) string {
	return "__mv_" + strings.ToLower(name)
}

func materializedViewCacheTableNameFor(schema, name string) string {
	if schema == "" || schema == "main" {
		return materializedViewCacheTableName(name)
	}
	return "__mv_" + sanitizeObjectID(schema+"_"+name)
}

func materializedViewIntervalJobName(name string) string {
	return "__mv_refresh_" + sanitizeObjectID(name) + "_interval"
}

func materializedViewDailyJobName(name string) string {
	return "__mv_refresh_" + sanitizeObjectID(name) + "_daily"
}

func catalogDisplayName(schema, name string) string {
	if schema == "" || schema == "main" {
		return name
	}
	return schema + "." + name
}

func sanitizeObjectID(name string) string {
	replacer := strings.NewReplacer(".", "_", " ", "_")
	return strings.ToLower(replacer.Replace(name))
}

func registerMaterializedViewRefreshJobs(env ExecEnv, mv *storage.CatalogMaterializedView) error {
	viewName := catalogDisplayName(mv.Schema, mv.Name)
	if mv.RefreshEveryMs > 0 {
		if err := env.db.RegisterJob(&storage.CatalogJob{
			Name:         materializedViewIntervalJobName(viewName),
			SQLText:      "REFRESH MATERIALIZED VIEW " + viewName,
			ScheduleType: "INTERVAL",
			IntervalMs:   mv.RefreshEveryMs,
			Enabled:      true,
			NoOverlap:    true,
		}); err != nil {
			return err
		}
	}
	if mv.DailyAt != "" {
		cronExpr, err := dailyAtToCron(mv.DailyAt)
		if err != nil {
			return err
		}
		if err := env.db.RegisterJob(&storage.CatalogJob{
			Name:         materializedViewDailyJobName(viewName),
			SQLText:      "REFRESH MATERIALIZED VIEW " + viewName,
			ScheduleType: "CRON",
			CronExpr:     cronExpr,
			Timezone:     mv.Timezone,
			Enabled:      true,
			NoOverlap:    true,
		}); err != nil {
			return err
		}
	}
	return nil
}

func dailyAtToCron(dailyAt string) (string, error) {
	parts := strings.Split(dailyAt, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("daily refresh time must be HH:MM")
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return "", fmt.Errorf("daily refresh hour must be 0..23")
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return "", fmt.Errorf("daily refresh minute must be 0..59")
	}
	return fmt.Sprintf("0 %d %d * * *", minute, hour), nil
}

func refreshMaterializedView(env ExecEnv, name string) (err error) {
	schema, objectName := splitObjectName(name)
	mv, ok := env.db.Catalog().GetMaterializedView(schema, objectName)
	if !ok {
		return fmt.Errorf("materialized view %q not found", name)
	}
	if !env.db.Catalog().TryBeginMaterializedViewRefresh(schema, objectName) {
		return fmt.Errorf("materialized view %q is already refreshing", name)
	}

	start := time.Now()
	rowCount := int64(0)
	defer func() {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		_ = env.db.Catalog().FinishMaterializedViewRefresh(schema, objectName, time.Now(), time.Since(start).Milliseconds(), rowCount, errMsg)
	}()

	stmt, parseErr := NewParser(mv.SQLText).ParseStatement()
	if parseErr != nil {
		return parseErr
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return fmt.Errorf("materialized view %q query is not a SELECT", name)
	}
	// Recurses via execStmt, not Execute: this runs inside materialized-view
	// refresh, already inside Execute's write lock on the same goroutine.
	rs, execErr := execStmt(env, sel)
	if execErr != nil {
		return execErr
	}
	if rs == nil {
		return fmt.Errorf("materialized view %q query produced no result set", name)
	}

	cols := make([]storage.Column, len(rs.Cols))
	for i, c := range rs.Cols {
		colType := storage.TextType
		if len(rs.Rows) > 0 {
			colType = inferType(rs.Rows[0][strings.ToLower(c)])
		}
		cols[i] = storage.Column{Name: c, Type: colType}
	}
	cache := storage.NewTable(mv.CacheTableName, cols, false)
	for _, r := range rs.Rows {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = r[strings.ToLower(c.Name)]
		}
		cache.Rows = append(cache.Rows, row)
	}
	rowCount = int64(len(cache.Rows))
	// Replace, not Drop-then-Put: those are two separate lock acquisitions,
	// so a concurrent SELECT calling ensureMaterializedViewCache's own
	// env.db.Get could land in the gap between them and see the cache table
	// as briefly not existing, which drops it out of the "cache already
	// exists" fast path that would otherwise swallow a same-instant
	// "already refreshing" error from a second concurrent refresher --
	// surfacing that error to the SELECT instead of silently serving the
	// (still valid) existing cache. Replace closes that window.
	return env.db.Replace(env.tenant, cache)
}
