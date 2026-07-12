// Package engine implements the tinySQL execution engine.
//
// What: This module evaluates parsed SQL statements (AST) against the storage
// layer and produces ResultSets. It covers DDL/DML/SELECT, joins, grouping,
// aggregates, expression evaluation, simple functions (JSON_*, DATEDIFF, etc.),
// and a minimal tri-state boolean logic (true/false/unknown).
//
// How: The executor converts tables to row maps with both qualified and
// unqualified column keys, applies WHERE/GROUP/HAVING/ORDER/LIMIT/OFFSET, and
// optionally combines results with UNION/EXCEPT/INTERSECT. Expression
// evaluation is recursive over a small algebra of literals, variables, unary/
// binary ops, IS NULL, and function calls. Aggregate evaluation runs per-
// group with reusable helpers shared with the scalar evaluator.
//
// Why: Keeping execution self-contained and data-structure driven (Row maps
// and simple slices) makes the engine easy to reason about and to extend with
// new functions, operators, and clauses without introducing heavy planners.
package engine

import (
	"container/heap"
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// allFunctions is a lazily-initialized, read-only function registry that
// merges builtin, extended, and vector functions once and reuses the result
// for all subsequent evalFuncCall invocations. This avoids allocating three
// maps and merging them on every function evaluation.
var (
	allFunctions     map[string]funcHandler
	allFunctionsOnce sync.Once
)

func getAllFunctions() map[string]funcHandler {
	allFunctionsOnce.Do(func() {
		m := getBuiltinFunctions()
		for k, v := range getExtendedFunctions() {
			m[k] = v
		}
		for k, v := range getVectorFunctions() {
			m[k] = v
		}
		for k, v := range getExtraTypeFunctions() {
			m[k] = v
		}
		for k, v := range getFTSFunctions() {
			m[k] = v
		}
		for k, v := range getTextFunctions() {
			m[k] = v
		}
		for k, v := range getGeoFunctions() {
			m[k] = v
		}
		allFunctions = m
	})
	return allFunctions
}

// Row represents a single result row mapped by lower-cased column name.
// Keys include both qualified (table.column) and unqualified (column) names
// to simplify expression evaluation and projection.
type Row map[string]any

// ResultSet holds the column order and the returned rows from a query.
// Cols preserve the display order; Rows store values in a case-insensitive map.
type ResultSet struct {
	Cols []string
	Rows []Row
}

type ExecEnv struct {
	ctx         context.Context
	tenant      string
	db          *storage.DB
	ctes        map[string]*ResultSet // For CTE support
	windowRows  []Row                 // All rows for window function context
	windowIndex int                   // Current row index in window context
	viewDepth   int
	// triggerRow carries new.<col>/old.<col> pseudo-columns while executing a
	// trigger body statement (see executeTrigger in triggers.go), so
	// NEW.col/OLD.col resolve even though the body statement's own row
	// context (e.g. an INSERT's VALUES row) has no such columns.
	triggerRow Row
}

// Execute runs a parsed SQL statement against the given storage DB and tenant.
// It dispatches to handlers per statement kind and returns a ResultSet for
// SELECT (nil for DDL/DML). The context is checked at safe points to support
// cancellation.
//
// Concurrency: storage.DB's own mutex only protects the tenant->table map
// structure, not the contents of a *storage.Table (Rows/Cols/Version) once a
// caller holds one — INSERT/UPDATE/DELETE mutate Table.Rows with no lock of
// their own, so two goroutines calling Execute concurrently on the same DB
// would otherwise race on that slice (a real data race, not just stale
// reads: concurrent unsynchronized slice append + range is undefined
// behavior in Go). Execute closes that gap with a single coarse
// read/write lock around the whole statement: SELECT/EXPLAIN/PRAGMA take a
// shared read lock (so concurrent reads still run in parallel with each
// other), everything else takes an exclusive write lock. This is coarser
// than per-table locking — a write to table A blocks a concurrent read of
// unrelated table B — but it is correct and simple to audit, which matters
// more for a safety fix than maximum parallelism.
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (rs *ResultSet, err error) {
	if err := checkPermission(ctx, db, stmt); err != nil {
		// A denied attempt is itself a security-relevant event — arguably
		// more worth auditing than a routine success — so it's logged even
		// though nothing ran and no lock was ever taken.
		recordAudit(ctx, db, tenant, stmt, err)
		return nil, err
	}
	if isReadOnlyStatement(stmt) {
		db.LockContentForRead()
		defer db.UnlockContentForRead()
	} else {
		db.LockContentForWrite()
		defer db.UnlockContentForWrite()
	}
	// Registered before the recover defer below so it runs *after* it
	// during unwind (defers run LIFO): recover finalizes err first, then
	// this reads the now-final value — including errors turned from a
	// panic — rather than an err that's still mid-update.
	defer func() {
		recordAudit(ctx, db, tenant, stmt, err)
	}()
	// cmd/server and cmd/tinysqld already recover from panics at their own
	// request boundary, but an application embedding tinySQL directly (via
	// this function or the database/sql driver) has no such guard — a bug
	// anywhere in the evaluator would otherwise crash the whole host
	// process over a single bad query. Recovering here turns that into an
	// ordinary error instead.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error executing statement: %v", r)
		}
	}()
	return execStmt(ExecEnv{ctx: ctx, tenant: tenant, db: db}, stmt)
}

// recordAudit appends one entry to db's audit log, if one is attached
// (see storage.DB.AttachAuditLog). A no-op — including skipping the
// auditTextFromContext/UserFromContext lookups — when none is configured,
// so callers that never enable auditing pay no cost per statement.
func recordAudit(ctx context.Context, db *storage.DB, tenant string, stmt Statement, err error) {
	log := db.AuditLog()
	if log == nil {
		return
	}
	text, ok := auditTextFromContext(ctx)
	if !ok {
		text = fmt.Sprintf("<%T>", stmt)
	}
	user, _ := UserFromContext(ctx)
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	log.Append(tenant, user, text, err == nil, errMsg)
}

// execStmt is Execute's dispatch logic without the content lock. It exists
// so that statements which recursively execute a nested statement on the
// same goroutine (CREATE TABLE AS SELECT, materialized view refresh) can
// call back into the dispatcher without re-acquiring db's content lock —
// sync.RWMutex is not reentrant, so a second Lock/RLock call from the same
// goroutine that already holds the write lock would deadlock.
func execStmt(env ExecEnv, stmt Statement) (*ResultSet, error) {
	if env.db.IsReadOnly() {
		if err := rejectIfMutating(stmt); err != nil {
			return nil, err
		}
	}
	switch s := stmt.(type) {
	case *Explain:
		return executeExplain(env, s)
	case *Pragma:
		return executePragma(env, s)
	case *CreateTable:
		return executeCreateTable(env, s)
	case *DropTable:
		return executeDropTable(env, s)
	case *CreateIndex:
		return executeCreateIndex(env, s)
	case *DropIndex:
		return executeDropIndex(env, s)
	case *CreateView:
		return executeCreateView(env, s)
	case *DropView:
		return executeDropView(env, s)
	case *CreateMaterializedView:
		return executeCreateMaterializedView(env, s)
	case *DropMaterializedView:
		return executeDropMaterializedView(env, s)
	case *RefreshMaterializedView:
		return executeRefreshMaterializedView(env, s)
	case *AlterViewMaterialize:
		return executeAlterViewMaterialize(env, s)
	case *AlterMaterializedViewToView:
		return executeAlterMaterializedViewToView(env, s)
	case *AlterTable:
		return executeAlterTable(env, s)
	case *Insert:
		return executeInsert(env, s)
	case *Update:
		return executeUpdate(env, s)
	case *Delete:
		return executeDelete(env, s)
	case *CallProcedure:
		return executeCallProcedure(env, s)
	case *Select:
		return executeSelect(env, s)
	case *CreateJob:
		return executeCreateJob(env, s)
	case *AlterJob:
		return executeAlterJob(env, s)
	case *DropJob:
		return executeDropJob(env, s)
	case *CreateTrigger:
		return executeCreateTrigger(env, s)
	case *DropTrigger:
		return executeDropTrigger(env, s)
	case *CreateUser:
		return executeCreateUser(env, s)
	case *DropUser:
		return executeDropUser(env, s)
	case *AlterUser:
		return executeAlterUser(env, s)
	case *CreateRole:
		return executeCreateRole(env, s)
	case *DropRole:
		return executeDropRole(env, s)
	case *GrantPrivilege:
		return executeGrantPrivilege(env, s)
	case *RevokePrivilege:
		return executeRevokePrivilege(env, s)
	case *GrantRoleStmt:
		return executeGrantRoleStmt(env, s)
	case *RevokeRoleStmt:
		return executeRevokeRoleStmt(env, s)
	}
	return nil, fmt.Errorf("unknown statement")
}

// isReadOnlyStatement reports whether stmt can never mutate table rows or
// schema. Kept in sync with rejectIfMutating's classification (used for
// read-only-mode enforcement) since both need the same answer: only
// SELECT/EXPLAIN/PRAGMA are safe to run under a shared read lock.
func isReadOnlyStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *Select, *Explain, *Pragma:
		return true
	default:
		return false
	}
}

// rejectIfMutating returns an error for any statement that would modify data
// or schema. Only SELECT, EXPLAIN, and PRAGMA are permitted in read-only mode.
func rejectIfMutating(stmt Statement) error {
	switch stmt.(type) {
	case *Select, *Explain, *Pragma:
		return nil
	default:
		return fmt.Errorf("database is in read-only mode: %T statements are not allowed", stmt)
	}
}

// -------------------- Statement Handlers --------------------

func executeCreateTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	// Check IF NOT EXISTS
	if s.IfNotExists {
		_, err := env.db.Get(env.tenant, s.Name)
		if err == nil {
			// Table already exists, silently succeed
			return nil, nil
		}
	}

	// Handle CREATE VIRTUAL TABLE ... USING fts(...)
	if s.VirtualTable && s.Using == "fts" {
		return executeCreateFTSTable(env, s)
	}

	if s.AsSelect == nil {
		t := storage.NewTable(s.Name, s.Cols, s.IsTemp)
		err := env.db.Put(env.tenant, t)
		return nil, err
	}
	// Recurses via execStmt, not Execute: this runs inside executeCreateTable,
	// already inside Execute's write lock on the same goroutine.
	rs, err := execStmt(env, s.AsSelect)
	if err != nil {
		return nil, err
	}
	cols := make([]storage.Column, len(rs.Cols))
	if len(rs.Rows) > 0 {
		for i, c := range rs.Cols {
			cols[i] = storage.Column{Name: c, Type: inferType(rs.Rows[0][strings.ToLower(c)])}
		}
	} else {
		for i, c := range rs.Cols {
			cols[i] = storage.Column{Name: c, Type: storage.TextType}
		}
	}
	t := storage.NewTable(s.Name, cols, s.IsTemp)
	for _, r := range rs.Rows {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = r[strings.ToLower(c.Name)]
		}
		t.Rows = append(t.Rows, row)
	}
	err = env.db.Put(env.tenant, t)
	return nil, err
}

func executeDropTable(env ExecEnv, s *DropTable) (*ResultSet, error) {
	// Check IF EXISTS
	t, err := env.db.Get(env.tenant, s.Name)
	if err != nil {
		if s.IfExists {
			// Table doesn't exist, silently succeed
			return nil, nil
		}
	} else {
		// Release cached constraint indexes; otherwise a create/drop/create
		// cycle under the same table name leaks one stale entry per
		// constrained column per drop (the old *storage.Table pointer is
		// never looked up again, but its map entries live until process exit).
		invalidateConstraintIndexes(t)
		// Same for the vector-search caches: a stale column-cache/ANN-index
		// entry pins the dropped table's entire row data until the same
		// (tenant, table, column) key is rebuilt — which, for a dropped
		// name, may be never. The vector caches normalize an empty tenant
		// to "default", so match that here.
		tenant := env.tenant
		if tenant == "" {
			tenant = "default"
		}
		purgeVectorCachesFor(tenant, t.Name)
		// Index metadata is independent of the table storage and must be
		// removed explicitly so catalog introspection cannot report indexes
		// for a table that no longer exists.
		env.db.Catalog().DeleteIndexesForTable(s.Name)
	}
	return nil, env.db.Drop(env.tenant, s.Name)
}

func executeCreateIndex(env ExecEnv, s *CreateIndex) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	if _, exists := env.db.Catalog().GetIndex(schema, name); exists {
		if s.IfNotExists {
			return nil, nil
		}
		return nil, fmt.Errorf("index %q already exists", s.Name)
	}
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	for _, col := range s.Columns {
		if _, err := t.ColIndex(col); err != nil {
			return nil, err
		}
	}
	if err := t.CreateSecondaryIndex(name, s.Columns, s.Unique); err != nil {
		return nil, err
	}
	if err := env.db.Catalog().RegisterIndex(&storage.CatalogIndex{
		Schema:    schema,
		Name:      name,
		Table:     s.Table,
		Columns:   append([]string(nil), s.Columns...),
		Unique:    s.Unique,
		CreatedAt: time.Now(),
	}); err != nil {
		t.DropSecondaryIndex(name)
		return nil, err
	}
	// A CREATE INDEX changes the executable shape of the table. Bumping the
	// table version invalidates any prepared plan that captured the old shape
	// and makes disk/hybrid backends persist the materialized entries.
	t.Version++
	t.MarkDirtyFrom(-1)
	return nil, nil
}

func executeDropIndex(env ExecEnv, s *DropIndex) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	idx, exists := env.db.Catalog().GetIndex(schema, name)
	if !exists {
		if s.IfExists {
			return nil, nil
		}
		return nil, fmt.Errorf("index %q not found", s.Name)
	}
	if s.Table != "" && !strings.EqualFold(idx.Table, s.Table) {
		return nil, fmt.Errorf("index %q is on table %q, not %q", s.Name, idx.Table, s.Table)
	}
	if t, err := env.db.Get(env.tenant, idx.Table); err == nil {
		t.DropSecondaryIndex(name)
		t.Version++
		t.MarkDirtyFrom(-1)
	}
	return nil, env.db.Catalog().DeleteIndex(schema, name)
}

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
	if _, err := env.db.Get(env.tenant, mv.CacheTableName); err == nil {
		if err := env.db.Drop(env.tenant, mv.CacheTableName); err != nil {
			return err
		}
	}
	return env.db.Put(env.tenant, cache)
}

func executeAlterTable(env ExecEnv, s *AlterTable) (*ResultSet, error) {
	// Get the table
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}

	// Handle ADD COLUMN
	if s.AddColumn != nil {
		// Check if column already exists
		for _, col := range t.Cols {
			if col.Name == s.AddColumn.Name {
				return nil, fmt.Errorf("column %q already exists", s.AddColumn.Name)
			}
		}

		// Add the new column to table schema
		t.Cols = append(t.Cols, *s.AddColumn)

		// Add NULL values for existing rows
		for i := range t.Rows {
			t.Rows[i] = append(t.Rows[i], nil)
		}

		// Update the table
		if err := env.db.Put(env.tenant, t); err != nil {
			return nil, fmt.Errorf("alter table: %w", err)
		}
	}

	return nil, nil
}

func executeInsert(env ExecEnv, s *Insert) (*ResultSet, error) {
	if len(s.Rows) == 0 {
		return nil, fmt.Errorf("INSERT requires at least one VALUES clause")
	}
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	tmp := Row{}
	if len(s.Cols) == 0 {
		return executeInsertAllColumns(env, s, t, tmp)
	}
	return executeInsertSpecificColumns(env, s, t, tmp)
}

func executeInsertAllColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	expected := len(t.Cols)
	returningRows := make([]Row, 0, len(s.Rows))
	// buildTableRow allocates a map(2*len(cols)) per call; GetTriggers takes
	// a catalog RLock per call. Both were previously paid unconditionally on
	// every inserted row even when there were no triggers and no RETURNING
	// to consume the built row — hoist the trigger-existence check and
	// column-name slice out of the loop, and skip the row-map build entirely
	// when nothing will use it.
	tablePrefix := strings.ToLower(s.Table) + "."
	tableColNames := colNames(t.Cols)
	hasBefore := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("INSERT"))) > 0
	hasAfter := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("INSERT"))) > 0
	needsRow := hasBefore || hasAfter || len(s.Returning) > 0
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for _, vals := range s.Rows {
		if len(vals) != expected {
			return nil, fmt.Errorf("INSERT expects %d values", expected)
		}
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := make([]any, expected)
		for i, e := range vals {
			v, err := evalExpr(env, e, tmp)
			if err != nil {
				return nil, err
			}
			cv, err := coerceColumnValue(v, t.Cols[i])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
			}
			row[i] = cv
		}
		if err := validateRowConstraints(env, t, row, -1); err != nil {
			return nil, err
		}
		if err := t.CheckSecondaryIndexConstraints(row, -1); err != nil {
			return nil, err
		}
		var newRow Row
		if needsRow {
			newRow = buildTableRow(t.Cols, tablePrefix, row)
		}
		if hasBefore {
			if err := fireTriggers(env, s.Table, "BEFORE", "INSERT", newRow, nil); err != nil {
				return nil, err
			}
		}
		t.Rows = append(t.Rows, row)
		if err := wal.logInsert(env, len(t.Rows)-1, row, t.Cols); err != nil {
			return nil, err
		}
		if hasAfter {
			if err := fireTriggers(env, s.Table, "AFTER", "INSERT", newRow, nil); err != nil {
				return nil, err
			}
		}
		// FTS index is updated after all trigger hooks so it reflects final row data.
		ftsIndexRow(env.tenant+"/"+s.Table, s.Table, len(t.Rows)-1, nil, row, tableColNames)
		if len(s.Returning) > 0 {
			returningRows = append(returningRows, newRow)
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	t.Version++
	if err := t.RebuildSecondaryIndexes(); err != nil {
		return nil, err
	}
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
	markDependentMaterializedViewsStale(env, s.Table)
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return nil, nil
}

func executeInsertSpecificColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	colIdx := make([]int, len(s.Cols))
	for i, name := range s.Cols {
		idx, err := t.ColIndex(name)
		if err != nil {
			return nil, err
		}
		colIdx[i] = idx
	}
	returningRows := make([]Row, 0, len(s.Rows))
	tablePrefix := strings.ToLower(s.Table) + "."
	tableColNames := colNames(t.Cols)
	hasBefore := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("INSERT"))) > 0
	hasAfter := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("INSERT"))) > 0
	needsRow := hasBefore || hasAfter || len(s.Returning) > 0
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for _, vals := range s.Rows {
		if len(vals) != len(s.Cols) {
			return nil, fmt.Errorf("INSERT column/value mismatch")
		}
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := make([]any, len(t.Cols))
		for i := range row {
			row[i] = nil
		}
		for i, idx := range colIdx {
			v, err := evalExpr(env, vals[i], tmp)
			if err != nil {
				return nil, err
			}
			cv, err := coerceColumnValue(v, t.Cols[idx])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
			}
			row[idx] = cv
		}
		if err := validateRowConstraints(env, t, row, -1); err != nil {
			return nil, err
		}
		if err := t.CheckSecondaryIndexConstraints(row, -1); err != nil {
			return nil, err
		}
		var newRow Row
		if needsRow {
			newRow = buildTableRow(t.Cols, tablePrefix, row)
		}
		if hasBefore {
			if err := fireTriggers(env, s.Table, "BEFORE", "INSERT", newRow, nil); err != nil {
				return nil, err
			}
		}
		t.Rows = append(t.Rows, row)
		if err := wal.logInsert(env, len(t.Rows)-1, row, t.Cols); err != nil {
			return nil, err
		}
		if hasAfter {
			if err := fireTriggers(env, s.Table, "AFTER", "INSERT", newRow, nil); err != nil {
				return nil, err
			}
		}
		// FTS index is updated after all trigger hooks so it reflects final row data.
		ftsIndexRow(env.tenant+"/"+s.Table, s.Table, len(t.Rows)-1, nil, row, tableColNames)
		if len(s.Returning) > 0 {
			returningRows = append(returningRows, newRow)
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	t.Version++
	if err := t.RebuildSecondaryIndexes(); err != nil {
		return nil, err
	}
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
	markDependentMaterializedViewsStale(env, s.Table)
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return nil, nil
}

func validateRowConstraints(env ExecEnv, t *storage.Table, row []any, excludeRow int) error {
	for colIdx, col := range t.Cols {
		if col.Constraint == storage.NoConstraint {
			continue
		}
		if colIdx >= len(row) {
			return fmt.Errorf("row missing constrained column %q", col.Name)
		}
		val := row[colIdx]
		switch col.Constraint {
		case storage.PrimaryKey:
			if isNull(val) {
				return fmt.Errorf("PRIMARY KEY column %q cannot be NULL", col.Name)
			}
			if constraintValueExists(t, colIdx, val, excludeRow) {
				return fmt.Errorf("duplicate PRIMARY KEY value for column %q", col.Name)
			}
		case storage.Unique:
			if isNull(val) {
				continue
			}
			if constraintValueExists(t, colIdx, val, excludeRow) {
				return fmt.Errorf("duplicate UNIQUE value for column %q", col.Name)
			}
		case storage.ForeignKey:
			if isNull(val) {
				continue
			}
			if col.ForeignKey == nil {
				return fmt.Errorf("FOREIGN KEY column %q has no reference target", col.Name)
			}
			refTable, err := env.db.Get(env.tenant, col.ForeignKey.Table)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY column %q references missing table %q", col.Name, col.ForeignKey.Table)
			}
			refIdx, err := refTable.ColIndex(col.ForeignKey.Column)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY column %q references missing column %q.%q", col.Name, col.ForeignKey.Table, col.ForeignKey.Column)
			}
			if !constraintValueExists(refTable, refIdx, val, -1) {
				return fmt.Errorf("FOREIGN KEY violation on column %q: value %v not found in %s.%s", col.Name, val, col.ForeignKey.Table, col.ForeignKey.Column)
			}
		}
	}
	return nil
}

// constraintIndexes caches, per (table, column), a hash map from an
// already-used column value to the row indices holding it. This turns
// PRIMARY KEY / UNIQUE / FOREIGN KEY existence checks from an O(n) scan of
// the whole table (paid on every INSERT/UPDATE) into an O(1) lookup — the
// difference between ~10s and ~10ms when bulk-inserting 10k rows into a
// table that already has 100k.
//
// Maintenance is incremental rather than invalidate-and-rebuild-on-any-
// change, because a naive "rebuild when table.Version changes" cache would
// pay the full O(n) rebuild on literally every row of a multi-row INSERT
// (each row bumps Version), erasing the benefit entirely:
//   - INSERT only appends, so getConstraintIndex just indexes whatever rows
//     have been added since rowCount was last recorded — including rows
//     added earlier in the very same multi-row INSERT statement.
//   - UPDATE overwrites a row in place without changing the row count, so
//     it can't be detected by growth; patchConstraintIndexRow moves that
//     one row from its old value's bucket to its new one directly.
//   - DELETE removes rows and shifts every subsequent row's index, which
//     the incremental scheme can't reconcile cheaply, so
//     invalidateConstraintIndexes drops the cache outright and the next
//     check rebuilds it from scratch.
type constraintIndexKey struct {
	table  *storage.Table
	colIdx int
}
type constraintIndexEntry struct {
	rowCount int // rows already reflected in `rows`, i.e. t.Rows[:rowCount]
	rows     map[any][]int
}

var (
	constraintIndexMu sync.Mutex
	constraintIndexes = make(map[constraintIndexKey]*constraintIndexEntry)
)

func getConstraintIndex(t *storage.Table, colIdx int) *constraintIndexEntry {
	key := constraintIndexKey{table: t, colIdx: colIdx}
	constraintIndexMu.Lock()
	defer constraintIndexMu.Unlock()

	e, ok := constraintIndexes[key]
	if !ok || e.rowCount > len(t.Rows) {
		// First use for this column, or the table shrank (DELETE already
		// invalidates explicitly; this is a defensive fallback in case some
		// row-removing path doesn't).
		e = &constraintIndexEntry{rows: make(map[any][]int, len(t.Rows))}
		constraintIndexes[key] = e
	}
	for i := e.rowCount; i < len(t.Rows); i++ {
		r := t.Rows[i]
		if colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		k := comparableKeyPart(r[colIdx])
		e.rows[k] = append(e.rows[k], i)
	}
	e.rowCount = len(t.Rows)
	return e
}

// invalidateConstraintIndexes drops every cached constraint index for a
// table. Call before any operation that can remove or reorder existing rows
// (DELETE) or replace the table wholesale (DROP TABLE) — the incremental
// index only knows how to grow by appending or patch a single row in place.
func invalidateConstraintIndexes(t *storage.Table) {
	constraintIndexMu.Lock()
	for k := range constraintIndexes {
		if k.table == t {
			delete(constraintIndexes, k)
		}
	}
	constraintIndexMu.Unlock()
}

// patchConstraintIndexRow updates every cached constraint index for a table
// after row rowIdx is overwritten in place (UPDATE), moving it from its old
// value's bucket to its new one instead of invalidating the whole cache.
func patchConstraintIndexRow(t *storage.Table, rowIdx int, oldRow, newRow []any) {
	constraintIndexMu.Lock()
	defer constraintIndexMu.Unlock()
	for k, e := range constraintIndexes {
		if k.table != t || k.colIdx >= len(oldRow) || k.colIdx >= len(newRow) {
			continue
		}
		oldVal, newVal := oldRow[k.colIdx], newRow[k.colIdx]
		if rawEqual(oldVal, newVal) {
			continue
		}
		if oldVal != nil {
			ok := comparableKeyPart(oldVal)
			bucket := e.rows[ok]
			for i, ri := range bucket {
				if ri == rowIdx {
					e.rows[ok] = append(bucket[:i], bucket[i+1:]...)
					break
				}
			}
		}
		if newVal != nil {
			nk := comparableKeyPart(newVal)
			e.rows[nk] = append(e.rows[nk], rowIdx)
		}
	}
}

func constraintValueExists(t *storage.Table, colIdx int, val any, excludeRow int) bool {
	idx := getConstraintIndex(t, colIdx)
	for _, rowIdx := range idx.rows[comparableKeyPart(val)] {
		if rowIdx != excludeRow {
			return true
		}
	}
	return false
}

func executeUpdate(env ExecEnv, s *Update) (*ResultSet, error) {
	if !tenantHasAnyForeignKeys(env) {
		if rs, ok, err := executeSimpleUpdateFastPath(env, s); ok || err != nil {
			return rs, err
		}
	}

	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	if err := checkForeignKeysBeforeUpdate(env, t, s); err != nil {
		return nil, err
	}
	setIdx := map[int]Expr{}
	for name, ex := range s.Sets {
		i, err := t.ColIndex(name)
		if err != nil {
			return nil, err
		}
		setIdx[i] = ex
	}
	n := 0
	returningRows := make([]Row, 0)
	tablePrefix := strings.ToLower(s.Table) + "."
	columnNames := colNames(t.Cols)
	hasBefore := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("UPDATE"))) > 0
	hasAfter := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("UPDATE"))) > 0
	needsNewRow := hasAfter || len(s.Returning) > 0
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for ri, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(t.Cols, tablePrefix, r)
		ok := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			ok = (toTri(v) == tvTrue)
		}
		if ok {
			// r == t.Rows[ri] before any mutation below, so oldRow is
			// identical to row — reuse it instead of rebuilding the same
			// map from the same data.
			oldRow := row
			if hasBefore {
				if err := fireTriggers(env, s.Table, "BEFORE", "UPDATE", row, oldRow); err != nil {
					return nil, err
				}
			}
			nextRow := append([]any(nil), t.Rows[ri]...)
			for i, ex := range setIdx {
				v, err := evalExpr(env, ex, row)
				if err != nil {
					return nil, err
				}
				cv, err := coerceColumnValue(v, t.Cols[i])
				if err != nil {
					return nil, err
				}
				nextRow[i] = cv
			}
			if err := validateRowConstraints(env, t, nextRow, ri); err != nil {
				return nil, err
			}
			if err := t.CheckSecondaryIndexConstraints(nextRow, ri); err != nil {
				return nil, err
			}
			patchConstraintIndexRow(t, ri, t.Rows[ri], nextRow)
			before := r
			t.Rows[ri] = nextRow
			if err := wal.logUpdate(env, ri, before, nextRow, t.Cols); err != nil {
				return nil, err
			}
			var newRow Row
			if needsNewRow {
				newRow = buildTableRow(t.Cols, tablePrefix, t.Rows[ri])
			}
			ftsIndexRow(env.tenant+"/"+s.Table, s.Table, ri, nil, t.Rows[ri], columnNames)
			if hasAfter {
				if err := fireTriggers(env, s.Table, "AFTER", "UPDATE", newRow, oldRow); err != nil {
					return nil, err
				}
			}
			if len(s.Returning) > 0 {
				returningRows = append(returningRows, newRow)
			}
			n++
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	t.Version++
	if err := t.RebuildSecondaryIndexes(); err != nil {
		return nil, err
	}
	if n > 0 {
		t.MarkDirtyFrom(-1) // UPDATE is non-append; force full-table WAL
		markDependentMaterializedViewsStale(env, s.Table)
	}
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
}

type simpleUpdatePlan struct {
	table    *storage.Table
	colIndex map[string]int
	where    Expr
	sets     []simpleUpdateSet
}

type simpleUpdateSet struct {
	col  int
	expr Expr
}

func executeSimpleUpdateFastPath(env ExecEnv, s *Update) (*ResultSet, bool, error) {
	if len(s.Returning) > 0 {
		return nil, false, nil
	}
	plan, ok, err := buildSimpleUpdatePlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rawPlan := &simpleSelectPlan{colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where)}
	updated := 0
	values := make([]any, len(plan.sets))
	columnNames := colNames(plan.table.Cols)
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, true, err
	}
	for ri, raw := range plan.table.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if ri&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(rawPlan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}

		for i, set := range plan.sets {
			v, err := evalRawExpr(rawPlan, raw, set.expr)
			if err != nil {
				return nil, true, err
			}
			cv, err := coerceColumnValue(v, plan.table.Cols[set.col])
			if err != nil {
				return nil, true, err
			}
			values[i] = cv
		}
		nextRow := append([]any(nil), raw...)
		for i, set := range plan.sets {
			nextRow[set.col] = values[i]
		}
		if err := validateRowConstraints(env, plan.table, nextRow, ri); err != nil {
			return nil, true, err
		}
		patchConstraintIndexRow(plan.table, ri, plan.table.Rows[ri], nextRow)
		before := raw
		plan.table.Rows[ri] = nextRow
		if err := wal.logUpdate(env, ri, before, nextRow, plan.table.Cols); err != nil {
			return nil, true, err
		}
		ftsIndexRow(env.tenant+"/"+s.Table, s.Table, ri, nil, plan.table.Rows[ri], columnNames)
		updated++
	}
	if err := wal.commit(); err != nil {
		return nil, true, err
	}

	plan.table.Version++
	if updated > 0 {
		plan.table.MarkDirtyFrom(-1)
		markDependentMaterializedViewsStale(env, s.Table)
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": updated}}}, true, nil
}

func buildSimpleUpdatePlan(env ExecEnv, s *Update) (*simpleUpdatePlan, bool, error) {
	if len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("UPDATE"))) > 0 ||
		len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("UPDATE"))) > 0 {
		return nil, false, nil
	}
	if !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	table, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, true, err
	}
	colIndex := simpleColumnIndex(table, s.Table)
	sets := make([]simpleUpdateSet, 0, len(s.Sets))
	for name, expr := range s.Sets {
		if !isSimpleRawExpr(expr) {
			return nil, false, nil
		}
		col, err := table.ColIndex(name)
		if err != nil {
			return nil, true, err
		}
		sets = append(sets, simpleUpdateSet{col: col, expr: expr})
	}
	return &simpleUpdatePlan{
		table:    table,
		colIndex: colIndex,
		where:    s.Where,
		sets:     sets,
	}, true, nil
}

func executeDelete(env ExecEnv, s *Delete) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	// Removing rows shifts every subsequent row's index, which the
	// incremental constraint index can't reconcile cheaply — drop it and
	// let the next INSERT/UPDATE rebuild it from scratch.
	invalidateConstraintIndexes(t)
	if err := checkForeignKeysBeforeDelete(env, t, s.Where); err != nil {
		return nil, err
	}
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	if s.Where == nil {
		del := len(t.Rows)
		if len(s.Returning) > 0 {
			tablePrefix := strings.ToLower(s.Table) + "."
			returningRows := make([]Row, 0, len(t.Rows))
			for _, r := range t.Rows {
				returningRows = append(returningRows, buildTableRow(t.Cols, tablePrefix, r))
			}
			if del > 0 {
				for i, r := range t.Rows {
					if err := wal.logDelete(env, i, r, t.Cols); err != nil {
						return nil, err
					}
				}
				if err := wal.commit(); err != nil {
					return nil, err
				}
				t.Rows = nil
				t.Version++
				if err := t.RebuildSecondaryIndexes(); err != nil {
					return nil, err
				}
				t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
				markDependentMaterializedViewsStale(env, s.Table)
			}
			return projectReturningRows(env, t.Cols, s.Returning, returningRows)
		}
		if del > 0 {
			for i, r := range t.Rows {
				if err := wal.logDelete(env, i, r, t.Cols); err != nil {
					return nil, err
				}
			}
			if err := wal.commit(); err != nil {
				return nil, err
			}
			t.Rows = nil
			t.Version++
			if err := t.RebuildSecondaryIndexes(); err != nil {
				return nil, err
			}
			t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
			markDependentMaterializedViewsStale(env, s.Table)
		}
		return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
	}

	// Fast path: no triggers and a simple predicate – skip the full Row map allocation.
	hasTriggers := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("DELETE"))) > 0 ||
		len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("DELETE"))) > 0
	if !hasTriggers && len(s.Returning) == 0 && isSimpleRawPredicate(s.Where) {
		colIndex := simpleColumnIndex(t, s.Table)
		rawPlan := &simpleSelectPlan{colIndex: colIndex, where: s.Where, filter: buildRawFilter(colIndex, s.Where)}
		kept := make([][]any, 0, len(t.Rows))
		del := 0
		for i, r := range t.Rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			match, err := evalRawWhere(rawPlan, r)
			if err != nil {
				return nil, err
			}
			if match {
				if err := wal.logDelete(env, i, r, t.Cols); err != nil {
					return nil, err
				}
				ftsDeleteRow(env.tenant+"/"+s.Table, del+len(kept))
				del++
			} else {
				kept = append(kept, r)
			}
		}
		if err := wal.commit(); err != nil {
			return nil, err
		}
		t.Rows = kept
		t.Version++
		if err := t.RebuildSecondaryIndexes(); err != nil {
			return nil, err
		}
		if del > 0 {
			t.MarkDirtyFrom(-1)
			markDependentMaterializedViewsStale(env, s.Table)
		}
		return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
	}

	// Slow path: triggers present or complex predicate – build full Row maps.
	kept := make([][]any, 0, len(t.Rows))
	del := 0
	returningRows := make([]Row, 0)
	tablePrefix := strings.ToLower(s.Table) + "."
	hasBeforeDel := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("DELETE"))) > 0
	hasAfterDel := len(env.db.Catalog().GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("DELETE"))) > 0
	for i, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(t.Cols, tablePrefix, r)
		v, err := evalExpr(env, s.Where, row)
		if err != nil {
			return nil, err
		}
		if toTri(v) != tvTrue {
			kept = append(kept, r)
		} else {
			if hasBeforeDel {
				if err := fireTriggers(env, s.Table, "BEFORE", "DELETE", nil, row); err != nil {
					return nil, err
				}
			}
			if err := wal.logDelete(env, i, r, t.Cols); err != nil {
				return nil, err
			}
			ftsDeleteRow(env.tenant+"/"+s.Table, del+len(kept))
			if hasAfterDel {
				if err := fireTriggers(env, s.Table, "AFTER", "DELETE", nil, row); err != nil {
					return nil, err
				}
			}
			if len(s.Returning) > 0 {
				returningRows = append(returningRows, row)
			}
			del++
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	t.Rows = kept
	t.Version++
	if err := t.RebuildSecondaryIndexes(); err != nil {
		return nil, err
	}
	if del > 0 {
		t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
		markDependentMaterializedViewsStale(env, s.Table)
	}
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
}

func buildTableRow(cols []storage.Column, tablePrefix string, values []any) Row {
	row := make(Row, len(cols)*2)
	for i, c := range cols {
		key := strings.ToLower(c.Name)
		val := values[i]
		row[key] = val
		row[tablePrefix+key] = val
	}
	return row
}

func markDependentMaterializedViewsStale(env ExecEnv, tableName string) {
	if strings.HasPrefix(strings.ToLower(tableName), "__mv_") {
		return
	}
	schema, name := splitObjectName(tableName)
	if schema == "" {
		schema = "main"
	}
	_ = env.db.Catalog().MarkMaterializedViewsStaleByDependency(schema, name)
}

func projectReturningRows(env ExecEnv, cols []storage.Column, projs []SelectItem, rows []Row) (*ResultSet, error) {
	outRows := make([]Row, 0, len(rows))
	outCols := returningOutputCols(cols, projs)

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		out := Row{}
		for i, it := range projs {
			if it.Star {
				for _, c := range cols {
					name := c.Name
					lowerName := strings.ToLower(name)
					val, _ := getValLower(r, lowerName)
					putVal(out, name, val)
				}
				continue
			}
			val, err := evalExpr(env, it.Expr, r)
			if err != nil {
				return nil, err
			}
			putVal(out, projName(it, i), val)
		}
		outRows = append(outRows, out)
	}

	return &ResultSet{Cols: outCols, Rows: outRows}, nil
}

func returningOutputCols(cols []storage.Column, projs []SelectItem) []string {
	outCols := make([]string, 0, len(projs))
	seen := make(map[string]struct{}, len(projs))
	for i, it := range projs {
		if it.Star {
			for _, c := range cols {
				key := strings.ToLower(c.Name)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				outCols = append(outCols, c.Name)
			}
			continue
		}
		name := projName(it, i)
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		outCols = append(outCols, name)
	}
	return outCols
}

// resolveFromClause resolves the FROM clause of a SELECT statement and returns the initial rows.
// It handles: no FROM (dummy row), subqueries, table functions, CTEs, catalog tables, sys tables, and regular tables.
func resolveFromClause(env ExecEnv, cteEnv ExecEnv, s *Select) ([]Row, error) {
	// Check if FROM clause exists
	if s.From.Table == "" && s.From.Subquery == nil && s.From.TableFunc == nil {
		// No FROM clause - create a single dummy row for expression evaluation
		return []Row{make(Row)}, nil
	}

	if s.From.Subquery != nil {
		return resolveSubquery(env, s)
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
	leftRows := make([]Row, len(subResult.Rows))
	for i, row := range subResult.Rows {
		leftRows[i] = make(Row)
		for k, v := range row {
			// Immer unqualifiziert (für Outer-Select), lower-case
			leftRows[i][strings.ToLower(k)] = v
			// Zusätzlich mit Alias-Präfix, falls Alias gesetzt
			if s.From.Alias != "" {
				leftRows[i][strings.ToLower(s.From.Alias+"."+k)] = v
			}
		}
	}
	return leftRows, nil
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
	// Convert ResultSet to rows with alias handling
	leftRows := make([]Row, len(rs.Rows))
	for i, row := range rs.Rows {
		leftRows[i] = make(Row)
		for k, v := range row {
			leftRows[i][strings.ToLower(k)] = v
			if s.From.Alias != "" {
				leftRows[i][strings.ToLower(s.From.Alias+"."+k)] = v
			}
		}
	}
	return leftRows, nil
}

// resolveTableSource handles CTE, catalog, sys, or regular table resolution
func resolveTableSource(cteEnv ExecEnv, env ExecEnv, s *Select) ([]Row, error) {
	// Prefer CTE binding first
	if cteEnv.ctes != nil {
		if cteResult, exists := cteEnv.ctes[s.From.Table]; exists {
			leftRows := make([]Row, len(cteResult.Rows))
			for i, row := range cteResult.Rows {
				leftRows[i] = make(Row)
				for k, v := range row {
					leftRows[i][k] = v
					leftRows[i][s.From.Table+"."+k] = v
				}
			}
			return leftRows, nil
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
	for _, r := range outRows {
		var parts []string
		for _, e := range s.DistinctOn {
			v, err := evalExpr(env, e, r)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(parts, "|")
		if !seen[key] {
			seen[key] = true
			res = append(res, r)
		}
	}
	return res, nil
}

// applySortOrder applies ORDER BY sorting to rows. Sort keys are extracted
// once per row up front (same helper as applySortOrderWithLimit's TopN path)
// instead of re-looking them up from the row map on every comparator call —
// a map lookup per column per comparison adds up fast under sort.SliceStable's
// O(n log n) comparisons.
func applySortOrder(orderBy []OrderItem, outRows []Row) []Row {
	if len(orderBy) == 0 || len(outRows) <= 1 {
		return outRows
	}
	lcOrdCols := make([]string, len(orderBy))
	for idx, oi := range orderBy {
		lcOrdCols[idx] = strings.ToLower(oi.Col)
	}
	items := make([]orderedValueRow, len(outRows))
	for i, row := range outRows {
		items[i] = buildOrderByValues(row, lcOrdCols)
	}
	sort.SliceStable(items, func(i, j int) bool {
		return compareOrderedValueRows(orderBy, items[i], items[j]) < 0
	})
	for i, item := range items {
		outRows[i] = item.row
	}
	return outRows
}

type orderedValueRow struct {
	row  Row
	keys []any
}

type orderedValueRowHeap struct {
	orderBy []OrderItem
	items   []orderedValueRow
}

func (h orderedValueRowHeap) Len() int { return len(h.items) }

func (h orderedValueRowHeap) Less(i, j int) bool {
	return compareOrderedValueRows(h.orderBy, h.items[i], h.items[j]) > 0
}

func (h orderedValueRowHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *orderedValueRowHeap) Push(x any) {
	h.items = append(h.items, x.(orderedValueRow))
}

func (h *orderedValueRowHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

func (h *orderedValueRowHeap) pushBounded(item orderedValueRow, keepCount int) {
	if keepCount <= 0 {
		return
	}
	if len(h.items) < keepCount {
		heap.Push(h, item)
		return
	}
	if compareOrderedValueRows(h.orderBy, h.items[0], item) > 0 {
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

func compareOrderedValueRows(orderBy []OrderItem, a, b orderedValueRow) int {
	for i, oi := range orderBy {
		cmp := compareOrderedValue(a.keys[i], b.keys[i], oi.Desc)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func buildOrderByValues(row Row, lcOrdCols []string) orderedValueRow {
	keys := make([]any, len(lcOrdCols))
	for i, col := range lcOrdCols {
		keys[i] = row[col]
	}
	return orderedValueRow{row: row, keys: keys}
}

func applySortOrderWithLimit(orderBy []OrderItem, outRows []Row, limit, offset *int) []Row {
	if len(orderBy) == 0 || len(outRows) <= 1 {
		return outRows
	}
	if limit != nil && *limit <= 0 {
		return []Row{}
	}
	if limit == nil && offset == nil {
		return applySortOrder(orderBy, outRows)
	}

	lcOrdCols := make([]string, len(orderBy))
	for idx, oi := range orderBy {
		lcOrdCols[idx] = strings.ToLower(oi.Col)
	}

	keepCount := len(outRows)
	if limit != nil {
		keepCount = *limit
		if offset != nil {
			keepCount += *offset
		}
		if keepCount > len(outRows) {
			keepCount = len(outRows)
		}
	}
	if keepCount <= 0 {
		return []Row{}
	}

	items := make([]orderedValueRow, 0, min(cap(outRows), keepCount))
	var topRows orderedValueRowHeap
	useTopN := limit != nil && keepCount > 0 && keepCount < len(outRows)
	if useTopN {
		topRows = orderedValueRowHeap{
			orderBy: orderBy,
			items:   make([]orderedValueRow, 0, min(cap(outRows), keepCount)),
		}
	}

	for _, row := range outRows {
		item := buildOrderByValues(row, lcOrdCols)
		if useTopN {
			topRows.pushBounded(item, keepCount)
		} else {
			items = append(items, item)
		}
	}
	if useTopN {
		items = topRows.items
	}

	sort.SliceStable(items, func(i, j int) bool {
		return compareOrderedValueRows(orderBy, items[i], items[j]) < 0
	})

	sorted := make([]Row, len(items))
	for i, item := range items {
		sorted[i] = item.row
	}
	return sorted
}

func executeSelect(env ExecEnv, s *Select) (*ResultSet, error) {
	if rs, ok, err := executeSimpleJoinFastPath(env, s); ok || err != nil {
		return rs, err
	}
	if rs, ok, err := executeSimpleAggregateFastPath(env, s); ok || err != nil {
		return rs, err
	}
	if rs, ok, err := executeSimpleSelectFastPath(env, s); ok || err != nil {
		return rs, err
	}

	// Process CTEs first
	cteEnv, err := processCTEs(env, s)
	if err != nil {
		return nil, err
	}

	// FROM (Tabelle, CTE oder Subselect) - now optional
	leftRows, err := resolveFromClause(env, cteEnv, s)
	if err != nil {
		return nil, err
	}

	cur := leftRows

	// JOINs
	cur, err = processJoins(cteEnv, s.Joins, cur)
	if err != nil {
		return nil, err
	}

	// WHERE
	filtered, err := applyWhereClause(cteEnv, s.Where, cur)
	if err != nil {
		return nil, err
	}

	// GROUP/HAVING
	outRows, outCols, err := processGroupByHaving(env, s, filtered)
	if err != nil {
		return nil, err
	}

	// DISTINCT
	if s.Distinct {
		// If DISTINCT ON (...) was used, apply DISTINCT ON semantics: keep first
		// row per distinct-on key. The ORDER BY clause controls which row is
		// considered "first"; so apply ORDER BY first if present.
		if len(s.DistinctOn) > 0 {
			var err error
			outRows, err = applyDistinctOn(env, s, outRows)
			if err != nil {
				return nil, err
			}
		} else {
			outRows = distinctRows(outRows, outCols)
		}
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		outRows = applySortOrderWithLimit(s.OrderBy, outRows, s.Limit, s.Offset)
	}

	// OFFSET/LIMIT (applied before UNION to each individual SELECT)
	baseRows := applyOffsetLimit(s, outRows)

	// Handle UNION operations
	resultRows := baseRows
	resultCols := outCols

	if s.Union != nil {
		var err error
		resultRows, resultCols, err = processUnionClauses(env, s.Union, resultRows, resultCols)
		if err != nil {
			return nil, err
		}
	}

	if len(resultCols) == 0 {
		resultCols = columnsFromRows(resultRows)
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
}

type simpleSelectPlan struct {
	table      *storage.Table
	colIndex   map[string]int
	projs      []simpleProjection
	orderBy    []OrderItem
	orderExprs []Expr
	where      Expr
	// filter is a pre-compiled, allocation-free version of where for the most
	// common patterns (col op literal, boolean column, AND/OR of those). When
	// non-nil it replaces the recursive evalRawWhere call in the hot scan loop.
	filter     func([]any) (bool, error)
	limit      *int
	offset     *int
	outputCols []string
	rowMapCap  int
	// rowIDs is nil for a table scan. A non-nil slice is a materialized
	// secondary-index point/prefix seek and contains table row positions.
	rowIDs          []int
	scanType        string
	indexName       string
	indexPredicates []string
	residualFilter  bool
	coveringIndex   bool
	estimatedRows   int
}

// simpleSelectPlanCache stores the parameter-independent shape of a parsed
// simple SELECT. It deliberately excludes RowIDs and access-path estimates:
// those depend on the current bound values and current index contents. The
// cache lives on the AST, so its lifetime is bounded by the existing parsed
// statement cache or database/sql prepared statement.
type simpleSelectPlanCache struct {
	mu       sync.Mutex
	table    *storage.Table
	colCount int
	plan     *simpleSelectPlan
}

// simpleProjection describes a single SELECT item in the raw fast-path.
// When colIdx >= 0 the projection is a direct column reference: the value is
// taken from raw[colIdx] without going through evalRawExpr, saving a type
// switch, a strings.ToLower call, and a map lookup per row per column.
// key is the pre-lowercased name used as the Row map key (avoids putVal's
// strings.ToLower on every output row).
// side is only meaningful for join projections: 0=left table, 1=right table,
// -1=single-table context or expression that could not be resolved to a simple
// column reference (use expr instead).
type simpleProjection struct {
	name   string // output column name (original case for ResultSet.Cols)
	key    string // strings.ToLower(name) – pre-computed Row map key
	altKey string // optional second Row map key (e.g. "alias.col" for SELECT *,
	// matching rowsFromTable's qualified+unqualified dual keys); empty if unused
	side   int  // 0=left, 1=right (join), -1=single-table or expression
	colIdx int  // >= 0: direct array index into raw/left/right; -1: use expr
	expr   Expr // used when colIdx < 0
}

type simpleJoinPlan struct {
	left       *storage.Table
	right      *storage.Table
	leftIndex  map[string]int
	rightIndex map[string]int
	leftKey    int
	rightKey   int
	where      Expr
	projs      []simpleProjection
	outputCols []string
}

func executeSimpleJoinFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleJoinPlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rightByKey := make(map[any][][]any, len(plan.right.Rows))
	for _, right := range plan.right.Rows {
		key := comparableKeyPart(right[plan.rightKey])
		rightByKey[key] = append(rightByKey[key], right)
	}

	outRows := make([]Row, 0, min(len(plan.left.Rows), len(plan.right.Rows)))
	for i, left := range plan.left.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		matches := rightByKey[comparableKeyPart(left[plan.leftKey])]
		for _, right := range matches {
			match, err := evalJoinRawWhere(plan, left, right)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
			out, err := projectJoinRawRow(plan, left, right)
			if err != nil {
				return nil, true, err
			}
			outRows = append(outRows, out)
		}
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

func buildSimpleJoinPlan(env ExecEnv, s *Select) (*simpleJoinPlan, bool, error) {
	if !simpleJoinSelectEligible(s) {
		return nil, false, nil
	}
	if isCatalogViewSource(env, s.From.Table) || isCatalogViewSource(env, s.Joins[0].Right.Table) {
		return nil, false, nil
	}
	if anyAggInSelect(s.Projs) || anyWindowInSelect(s.Projs) || !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	left, right, err := loadSimpleJoinTables(env, s)
	if err != nil {
		return nil, true, err
	}

	leftIndex := simpleColumnIndex(left, aliasOr(s.From))
	rightIndex := simpleColumnIndex(right, aliasOr(s.Joins[0].Right))
	leftKey, rightKey, ok := simpleJoinKeys(s.Joins[0].On, leftIndex, rightIndex)
	if !ok {
		return nil, false, nil
	}

	projs, outputCols, ok := buildSimpleJoinProjections(s.Projs, leftIndex, rightIndex)
	if !ok {
		return nil, false, nil
	}
	if !simpleJoinExprResolvable(s.Where, leftIndex, rightIndex) {
		return nil, false, nil
	}

	return &simpleJoinPlan{
		left:       left,
		right:      right,
		leftIndex:  leftIndex,
		rightIndex: rightIndex,
		leftKey:    leftKey,
		rightKey:   rightKey,
		where:      s.Where,
		projs:      projs,
		outputCols: outputCols,
	}, true, nil
}

func simpleJoinSelectEligible(s *Select) bool {
	return !(s.Distinct || len(s.DistinctOn) > 0 || len(s.CTEs) > 0 || len(s.GroupBy) > 0 ||
		s.Having != nil || s.Union != nil || len(s.OrderBy) > 0 || s.Limit != nil || s.Offset != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil || len(s.Joins) != 1 ||
		s.Joins[0].Type != JoinInner || s.Joins[0].Right.Table == "" || s.Pivot != nil ||
		s.Joins[0].Right.Subquery != nil || s.Joins[0].Right.TableFunc != nil ||
		isSQLiteSchemaTable(s.From.Table) || isSQLiteSchemaTable(s.Joins[0].Right.Table))
}

func loadSimpleJoinTables(env ExecEnv, s *Select) (*storage.Table, *storage.Table, error) {
	left, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		return nil, nil, err
	}
	right, err := env.db.Get(env.tenant, s.Joins[0].Right.Table)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

func buildSimpleJoinProjections(items []SelectItem, leftIndex, rightIndex map[string]int) ([]simpleProjection, []string, bool) {
	projs := make([]simpleProjection, 0, len(items))
	outputCols := make([]string, 0, len(items))
	for i, it := range items {
		if it.Star || !isSimpleRawExpr(it.Expr) || !simpleJoinExprResolvable(it.Expr, leftIndex, rightIndex) {
			return nil, nil, false
		}
		name := projName(it, i)
		side, colIdx := resolveSimpleJoinProjectionRef(it.Expr, leftIndex, rightIndex)
		projs = append(projs, simpleProjection{
			name:   name,
			key:    strings.ToLower(name),
			side:   side,
			colIdx: colIdx,
			expr:   it.Expr,
		})
		outputCols = append(outputCols, name)
	}
	return projs, outputCols, true
}

func resolveSimpleJoinProjectionRef(e Expr, leftIndex, rightIndex map[string]int) (int, int) {
	ref, ok := e.(*VarRef)
	if !ok {
		return -1, -1
	}
	refName := strings.ToLower(ref.Name)
	if li, lok := leftIndex[refName]; lok {
		if _, ambig := rightIndex[refName]; !ambig {
			return 0, li
		}
	}
	if ri, rok := rightIndex[refName]; rok {
		return 1, ri
	}
	return -1, -1
}

func simpleJoinKeys(on Expr, leftIndex, rightIndex map[string]int) (int, int, bool) {
	bin, ok := on.(*Binary)
	if !ok || bin.Op != "=" {
		return 0, 0, false
	}
	leftRef, leftOK := bin.Left.(*VarRef)
	rightRef, rightOK := bin.Right.(*VarRef)
	if !leftOK || !rightOK {
		return 0, 0, false
	}
	leftKey := leftRef.Lower
	if leftKey == "" {
		leftKey = strings.ToLower(leftRef.Name)
	}
	rightKey := rightRef.Lower
	if rightKey == "" {
		rightKey = strings.ToLower(rightRef.Name)
	}
	if li, lok := leftIndex[leftKey]; lok {
		if ri, rok := rightIndex[rightKey]; rok {
			return li, ri, true
		}
	}
	if li, lok := leftIndex[rightKey]; lok {
		if ri, rok := rightIndex[leftKey]; rok {
			return li, ri, true
		}
	}
	return 0, 0, false
}

func simpleJoinExprResolvable(e Expr, leftIndex, rightIndex map[string]int) bool {
	switch ex := e.(type) {
	case nil, *Literal:
		return true
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		_, lok := leftIndex[key]
		_, rok := rightIndex[key]
		return lok != rok
	case *IsNull:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex)
	case *Unary:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex)
	case *Binary:
		return simpleJoinExprResolvable(ex.Left, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Right, leftIndex, rightIndex)
	case *LikeExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Pattern, leftIndex, rightIndex) &&
			(ex.Escape == nil || simpleJoinExprResolvable(ex.Escape, leftIndex, rightIndex))
	case *RegexpExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Pattern, leftIndex, rightIndex)
	case *BetweenExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Lo, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Hi, leftIndex, rightIndex)
	case *InExpr:
		if !simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) {
			return false
		}
		for _, v := range ex.Values {
			if !simpleJoinExprResolvable(v, leftIndex, rightIndex) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func evalJoinRawWhere(plan *simpleJoinPlan, left, right []any) (bool, error) {
	if plan.where == nil {
		return true, nil
	}
	v, err := evalJoinRawExpr(plan, left, right, plan.where)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

func projectJoinRawRow(plan *simpleJoinPlan, left, right []any) (Row, error) {
	out := make(Row, len(plan.projs))
	for _, p := range plan.projs {
		if p.colIdx >= 0 {
			// Direct column reference: read from pre-resolved side and index.
			switch p.side {
			case 0:
				out[p.key] = left[p.colIdx]
			case 1:
				out[p.key] = right[p.colIdx]
			default:
				v, err := evalJoinRawExpr(plan, left, right, p.expr)
				if err != nil {
					return nil, err
				}
				out[p.key] = v
			}
		} else {
			v, err := evalJoinRawExpr(plan, left, right, p.expr)
			if err != nil {
				return nil, err
			}
			out[p.key] = v
		}
	}
	return out, nil
}

func evalJoinRawExpr(plan *simpleJoinPlan, left, right []any, e Expr) (any, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		return evalJoinRawVarRef(plan, left, right, ex)
	case *IsNull:
		return evalJoinRawIsNull(plan, left, right, ex)
	case *Unary:
		return evalJoinRawUnary(plan, left, right, ex)
	case *Binary:
		return evalJoinRawBinary(plan, left, right, ex)
	case *LikeExpr:
		return evalJoinRawLike(plan, left, right, ex)
	case *RegexpExpr:
		return evalJoinRawRegexp(plan, left, right, ex)
	case *BetweenExpr:
		return evalJoinRawBetween(plan, left, right, ex)
	case *InExpr:
		return evalJoinRawIn(plan, left, right, ex)
	default:
		return nil, fmt.Errorf("unsupported join fast-path expression %T", e)
	}
}

// evalJoinRawBetween evaluates BETWEEN in the join fast path with a single
// evaluation of the comparand.
func evalJoinRawBetween(plan *simpleJoinPlan, left, right []any, ex *BetweenExpr) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	lo, err := evalJoinRawExpr(plan, left, right, ex.Lo)
	if err != nil {
		return nil, err
	}
	hi, err := evalJoinRawExpr(plan, left, right, ex.Hi)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

func evalJoinRawVarRef(plan *simpleJoinPlan, left, right []any, ex *VarRef) (any, error) {
	name := ex.Lower
	if name == "" {
		name = strings.ToLower(ex.Name)
	}
	if i, ok := plan.leftIndex[name]; ok {
		if _, ambiguous := plan.rightIndex[name]; ambiguous {
			return nil, fmt.Errorf("ambiguous column %q", ex.Name)
		}
		return left[i], nil
	}
	if i, ok := plan.rightIndex[name]; ok {
		return right[i], nil
	}
	return nil, fmt.Errorf("unknown column %q", ex.Name)
}

func evalJoinRawIsNull(plan *simpleJoinPlan, left, right []any, ex *IsNull) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	is := isNull(v)
	if ex.Negate {
		return !is, nil
	}
	return is, nil
}

func evalJoinRawUnary(plan *simpleJoinPlan, left, right []any, ex *Unary) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	return evalRawUnary(&simpleSelectPlan{}, nil, &Unary{Op: ex.Op, Expr: &Literal{Val: v}})
}

func evalJoinRawLike(plan *simpleJoinPlan, left, right []any, ex *LikeExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalJoinRawExpr(plan, left, right, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}
	pattern, ok := patVal.(string)
	if !ok {
		pattern = fmt.Sprintf("%v", patVal)
	}
	matched, err := evalJoinRawLikeMatch(plan, left, right, ex, str, pattern)
	if err != nil {
		return nil, err
	}
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

func evalJoinRawLikeMatch(plan *simpleJoinPlan, left, right []any, ex *LikeExpr, str, pattern string) (bool, error) {
	if ex.GlobStyle {
		if ex.CaseInsensitive {
			return matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern)), nil
		}
		return matchGlobPattern(str, pattern), nil
	}
	escapeChar := '\\'
	if ex.Escape != nil {
		escVal, err := evalJoinRawExpr(plan, left, right, ex.Escape)
		if err != nil {
			return false, err
		}
		if escStr, ok := escVal.(string); ok && len(escStr) == 1 {
			escapeChar = rune(escStr[0])
		}
	}
	if ex.CaseInsensitive {
		return matchLikePattern(strings.ToLower(str), strings.ToLower(pattern), escapeChar), nil
	}
	return matchLikePattern(str, pattern, escapeChar), nil
}

func evalJoinRawRegexp(plan *simpleJoinPlan, left, right []any, ex *RegexpExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalJoinRawExpr(plan, left, right, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str := fmt.Sprintf("%v", val)
	pat := fmt.Sprintf("%v", patVal)
	if ex.SimilarTo {
		pat = similarToRegexp(pat)
	}
	re, err := compileCachedRegexp(pat)
	if err != nil {
		return nil, fmt.Errorf("REGEXP: invalid pattern %q: %v", pat, err)
	}
	matched := re.MatchString(str)
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

func evalJoinRawIn(plan *simpleJoinPlan, left, right []any, ex *InExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	for _, valExpr := range ex.Values {
		listVal, err := evalJoinRawExpr(plan, left, right, valExpr)
		if err != nil {
			return nil, err
		}
		if rawEqual(val, listVal) {
			if ex.Negate {
				return false, nil
			}
			return true, nil
		}
	}
	if ex.Negate {
		return true, nil
	}
	return false, nil
}

func evalJoinRawBinary(plan *simpleJoinPlan, left, right []any, ex *Binary) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		lv, err := evalJoinRawExpr(plan, left, right, ex.Left)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" && toTri(lv) == tvFalse {
			return false, nil
		}
		if ex.Op == "OR" && toTri(lv) == tvTrue {
			return true, nil
		}
		rv, err := evalJoinRawExpr(plan, left, right, ex.Right)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" {
			return triToValue(triAnd(toTri(lv), toTri(rv))), nil
		}
		return triToValue(triOr(toTri(lv), toTri(rv))), nil
	}
	lv, err := evalJoinRawExpr(plan, left, right, ex.Left)
	if err != nil {
		return nil, err
	}
	rv, err := evalJoinRawExpr(plan, left, right, ex.Right)
	if err != nil {
		return nil, err
	}
	if isArithmeticOp(ex.Op) {
		return evalArithmeticBinary(ex.Op, lv, rv)
	}
	if isComparisonOp(ex.Op) {
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}

type simpleAggregatePlan struct {
	table      *storage.Table
	colIndex   map[string]int
	groupCol   int
	groupName  string
	where      Expr
	projs      []simpleAggregateProjection
	outputCols []string
}

// aggKind identifies which aggregate a simpleAggregateProjection computes in
// the raw fast path. Kept as a small enum (rather than a string) so the hot
// per-row switch in executeSimpleAggregateFastPath stays branch-cheap.
type aggKind byte

const (
	aggGroupCol aggKind = iota
	aggCount
	aggSum
	aggAvg
	aggMin
	aggMax
)

type simpleAggregateProjection struct {
	name string
	kind aggKind
	arg  Expr // nil for the group-by column and for COUNT(*)
}

// simpleAggregateState accumulates one group's aggregates directly (SUM as a
// running float/rational, MIN/MAX as a running best value) instead of
// buffering every matching row and re-scanning it once per aggregate
// expression, which is what the general (non-fast-path) GROUP BY evaluator
// does. sumRat/useRat mirror evalAggregateSumAvg's float->big.Rat promotion
// so SUM/AVG over DECIMAL/MONEY columns stays exact; they're left nil for
// groups that never see a decimal value, avoiding the allocation entirely
// for the common all-numeric case.
type simpleAggregateState struct {
	groupValue any
	counts     []int // COUNT result, or non-null sample count for SUM/AVG
	sumFloat   []float64
	sumRat     []*big.Rat
	useRat     []bool
	minmax     []any
	haveMinMax []bool
}

func executeSimpleAggregateFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleAggregatePlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rawPlan := &simpleSelectPlan{colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where)}
	groups := make(map[any]*simpleAggregateState)
	order := make([]any, 0)
	for i, raw := range plan.table.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(rawPlan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}

		groupValue := raw[plan.groupCol]
		key := comparableKeyPart(groupValue)
		state, exists := groups[key]
		if !exists {
			state = &simpleAggregateState{
				groupValue: groupValue,
				counts:     make([]int, len(plan.projs)),
				sumFloat:   make([]float64, len(plan.projs)),
				minmax:     make([]any, len(plan.projs)),
				haveMinMax: make([]bool, len(plan.projs)),
			}
			groups[key] = state
			order = append(order, key)
		}
		for i, proj := range plan.projs {
			switch proj.kind {
			case aggGroupCol:
				continue
			case aggCount:
				if proj.arg == nil {
					state.counts[i]++
					continue
				}
				v, err := evalRawExpr(rawPlan, raw, proj.arg)
				if err != nil {
					return nil, true, err
				}
				if v != nil {
					state.counts[i]++
				}
			case aggSum, aggAvg:
				v, err := evalRawExpr(rawPlan, raw, proj.arg)
				if err != nil {
					return nil, true, err
				}
				if v == nil {
					continue
				}
				if f, ok := numeric(v); ok {
					if state.useRat != nil && state.useRat[i] {
						state.sumRat[i].Add(state.sumRat[i], new(big.Rat).SetFloat64(f))
					} else {
						state.sumFloat[i] += f
					}
					state.counts[i]++
					continue
				}
				if rv, ok := storage.DecimalFromAny(v); ok {
					if state.useRat == nil {
						state.useRat = make([]bool, len(plan.projs))
						state.sumRat = make([]*big.Rat, len(plan.projs))
					}
					if !state.useRat[i] {
						state.sumRat[i] = new(big.Rat)
						if state.counts[i] > 0 {
							state.sumRat[i].SetFloat64(state.sumFloat[i])
						}
						state.useRat[i] = true
					}
					state.sumRat[i].Add(state.sumRat[i], new(big.Rat).Set(rv))
					state.counts[i]++
				}
			case aggMin, aggMax:
				v, err := evalRawExpr(rawPlan, raw, proj.arg)
				if err != nil {
					return nil, true, err
				}
				if v == nil {
					continue
				}
				if !state.haveMinMax[i] {
					state.minmax[i] = v
					state.haveMinMax[i] = true
					continue
				}
				cmp, err := compare(v, state.minmax[i])
				if err != nil {
					continue
				}
				if (proj.kind == aggMin && cmp < 0) || (proj.kind == aggMax && cmp > 0) {
					state.minmax[i] = v
				}
			}
		}
	}

	outRows := make([]Row, 0, len(order))
	for _, key := range order {
		state := groups[key]
		out := make(Row, len(plan.projs))
		for i, proj := range plan.projs {
			switch proj.kind {
			case aggGroupCol:
				putVal(out, proj.name, state.groupValue)
			case aggCount:
				putVal(out, proj.name, state.counts[i])
			case aggSum:
				if state.useRat != nil && state.useRat[i] {
					putVal(out, proj.name, state.sumRat[i])
				} else {
					putVal(out, proj.name, state.sumFloat[i])
				}
			case aggAvg:
				if state.counts[i] == 0 {
					putVal(out, proj.name, nil)
				} else if state.useRat != nil && state.useRat[i] {
					putVal(out, proj.name, new(big.Rat).Quo(state.sumRat[i], big.NewRat(int64(state.counts[i]), 1)))
				} else {
					putVal(out, proj.name, state.sumFloat[i]/float64(state.counts[i]))
				}
			case aggMin, aggMax:
				if state.haveMinMax[i] {
					putVal(out, proj.name, state.minmax[i])
				} else {
					putVal(out, proj.name, nil)
				}
			}
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

func buildSimpleAggregatePlan(env ExecEnv, s *Select) (*simpleAggregatePlan, bool, error) {
	if !simpleAggregateEligibleSelect(s) {
		return nil, false, nil
	}
	groupRef, ok := s.GroupBy[0].(*VarRef)
	if !ok {
		return nil, false, nil
	}
	if !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	table, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		schema, name := splitObjectName(s.From.Table)
		if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
			table, err = ensureMaterializedViewCache(env, s.From.Table, mv)
			if err != nil {
				return nil, true, err
			}
		} else if isCatalogViewSource(env, s.From.Table) {
			return nil, false, nil
		} else {
			return nil, true, err
		}
	}
	colIndex := simpleColumnIndex(table, aliasOr(s.From))
	groupCol, ok := colIndex[strings.ToLower(groupRef.Name)]
	if !ok {
		return nil, true, fmt.Errorf("unknown column %q", groupRef.Name)
	}

	projs, outputCols, hasAgg, eligible, err := buildSimpleAggregateProjections(s, colIndex, groupCol)
	if err != nil {
		return nil, true, err
	}
	if !eligible || !hasAgg {
		return nil, false, nil
	}
	return &simpleAggregatePlan{
		table:      table,
		colIndex:   colIndex,
		groupCol:   groupCol,
		groupName:  groupRef.Name,
		where:      s.Where,
		projs:      projs,
		outputCols: outputCols,
	}, true, nil
}

func simpleAggregateEligibleSelect(s *Select) bool {
	return !(s.Distinct || len(s.DistinctOn) > 0 || len(s.CTEs) > 0 || len(s.Joins) > 0 ||
		s.Having != nil || s.Union != nil || len(s.OrderBy) > 0 || s.Limit != nil || s.Offset != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil || len(s.GroupBy) != 1 ||
		s.Pivot != nil || isSQLiteSchemaTable(s.From.Table))
}

func buildSimpleAggregateProjections(s *Select, colIndex map[string]int, groupCol int) ([]simpleAggregateProjection, []string, bool, bool, error) {
	projs := make([]simpleAggregateProjection, 0, len(s.Projs))
	outputCols := make([]string, 0, len(s.Projs))
	hasAgg := false

	for i, it := range s.Projs {
		proj, name, isAgg, eligible, err := buildSimpleAggregateProjection(it, i, colIndex, groupCol)
		if err != nil {
			return nil, nil, false, false, err
		}
		if !eligible {
			return nil, nil, false, false, nil
		}
		if isAgg {
			hasAgg = true
		}
		projs = append(projs, proj)
		outputCols = append(outputCols, name)
	}
	return projs, outputCols, hasAgg, true, nil
}

// simpleAggFuncKinds maps the aggregate function names supported by the raw
// GROUP BY fast path (executeSimpleAggregateFastPath) to their aggKind.
// SUM/AVG/MIN/MAX join COUNT here so simple single-table GROUP BY queries
// using any of these no longer fall back to the general row-map evaluator.
var simpleAggFuncKinds = map[string]aggKind{
	"SUM": aggSum,
	"AVG": aggAvg,
	"MIN": aggMin,
	"MAX": aggMax,
}

func buildSimpleAggregateProjection(it SelectItem, idx int, colIndex map[string]int, groupCol int) (simpleAggregateProjection, string, bool, bool, error) {
	if it.Star {
		return simpleAggregateProjection{}, "", false, false, nil
	}
	name := projName(it, idx)
	if ref, ok := it.Expr.(*VarRef); ok {
		refCol, ok := colIndex[strings.ToLower(ref.Name)]
		if !ok {
			return simpleAggregateProjection{}, "", false, false, fmt.Errorf("unknown column %q", ref.Name)
		}
		if refCol != groupCol {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: aggGroupCol}, name, false, true, nil
	}

	fc, ok := it.Expr.(*FuncCall)
	if !ok || fc.Distinct || fc.Over != nil {
		return simpleAggregateProjection{}, "", false, false, nil
	}

	if fc.Name == "COUNT" {
		if fc.Star {
			return simpleAggregateProjection{name: name, kind: aggCount}, name, true, true, nil
		}
		if len(fc.Args) != 1 || !isSimpleRawExpr(fc.Args[0]) {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: aggCount, arg: fc.Args[0]}, name, true, true, nil
	}

	if kind, ok := simpleAggFuncKinds[fc.Name]; ok {
		if fc.Star || len(fc.Args) != 1 || !isSimpleRawExpr(fc.Args[0]) {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: kind, arg: fc.Args[0]}, name, true, true, nil
	}

	return simpleAggregateProjection{}, "", false, false, nil
}

func executeSimpleSelectFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleSelectPlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}
	if len(plan.orderBy) > 0 {
		return executeSimpleSelectOrderedFastPath(env, plan)
	}

	outRows := make([]Row, 0, simpleSelectInitialCap(plan))
	stopAfter := -1
	if plan.limit != nil {
		stopAfter = *plan.limit
		if plan.offset != nil {
			stopAfter += *plan.offset
		}
	}

	rowCount := len(plan.table.Rows)
	if plan.rowIDs != nil {
		rowCount = len(plan.rowIDs)
	}
	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(plan.table.Rows) {
			return nil, true, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		raw := plan.table.Rows[rowID]
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(plan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}
		out, err := projectRawRow(plan, raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
		if stopAfter >= 0 && len(outRows) >= stopAfter {
			break
		}
	}

	outRows = applyOffsetLimit(&Select{Limit: plan.limit, Offset: plan.offset}, outRows)
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

type orderedRawRow struct {
	raw  []any
	key  any
	keys []any
}

func executeSimpleSelectOrderedFastPath(env ExecEnv, plan *simpleSelectPlan) (*ResultSet, bool, error) {
	if plan.limit != nil && *plan.limit == 0 {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}

	keepCount := -1
	if plan.limit != nil {
		keepCount = *plan.limit
		if plan.offset != nil {
			keepCount += *plan.offset
		}
		if keepCount > len(plan.table.Rows) {
			keepCount = len(plan.table.Rows)
		}
	}

	rows := make([]orderedRawRow, 0, simpleSelectInitialCap(plan))
	var topRows orderedRawRowHeap
	useTopN := keepCount > 0
	if useTopN {
		topRows = orderedRawRowHeap{
			plan:  plan,
			items: make([]orderedRawRow, 0, simpleSelectInitialCap(plan)),
		}
	}
	for i, raw := range plan.table.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(plan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}
		if len(plan.orderExprs) == 1 {
			key, err := evalRawExpr(plan, raw, plan.orderExprs[0])
			if err != nil {
				return nil, true, err
			}
			item := orderedRawRow{raw: raw, key: key}
			if useTopN {
				topRows.pushBounded(item, keepCount)
			} else {
				rows = append(rows, item)
			}
			continue
		}
		keys := make([]any, len(plan.orderExprs))
		for i, expr := range plan.orderExprs {
			v, err := evalRawExpr(plan, raw, expr)
			if err != nil {
				return nil, true, err
			}
			keys[i] = v
		}
		item := orderedRawRow{raw: raw, keys: keys}
		if useTopN {
			topRows.pushBounded(item, keepCount)
		} else {
			rows = append(rows, item)
		}
	}
	if useTopN {
		rows = topRows.items
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return compareOrderedRawRows(plan, rows[i], rows[j]) < 0
	})

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(rows) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	rows = rows[start:]
	if plan.limit != nil && *plan.limit < len(rows) {
		rows = rows[:*plan.limit]
	}

	outRows := make([]Row, 0, len(rows))
	for _, item := range rows {
		out, err := projectRawRow(plan, item.raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

type orderedRawRowHeap struct {
	plan  *simpleSelectPlan
	items []orderedRawRow
}

func (h orderedRawRowHeap) Len() int { return len(h.items) }

func (h orderedRawRowHeap) Less(i, j int) bool {
	return compareOrderedRawRows(h.plan, h.items[i], h.items[j]) > 0
}

func (h orderedRawRowHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *orderedRawRowHeap) Push(x any) {
	h.items = append(h.items, x.(orderedRawRow))
}

func (h *orderedRawRowHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

func (h *orderedRawRowHeap) pushBounded(item orderedRawRow, keepCount int) {
	if keepCount <= 0 {
		return
	}
	if len(h.items) < keepCount {
		heap.Push(h, item)
		return
	}
	if compareOrderedRawRows(h.plan, h.items[0], item) > 0 {
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

func compareOrderedRawRows(plan *simpleSelectPlan, a, b orderedRawRow) int {
	if len(plan.orderBy) == 1 {
		return compareOrderedValue(a.key, b.key, plan.orderBy[0].Desc)
	}
	for i, oi := range plan.orderBy {
		cmp := compareOrderedValue(a.keys[i], b.keys[i], oi.Desc)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareOrderedValue(a, b any, desc bool) int {
	cmp := compareForOrder(a, b, desc)
	switch {
	case cmp == 0:
		return 0
	case desc && cmp > 0:
		return -1
	case desc:
		return 1
	case cmp < 0:
		return -1
	default:
		return 1
	}
}

func buildSimpleSelectPlan(env ExecEnv, s *Select) (*simpleSelectPlan, bool, error) {
	if !simpleSelectEligible(s) {
		return nil, false, nil
	}

	table, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		schema, name := splitObjectName(s.From.Table)
		if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
			table, err = ensureMaterializedViewCache(env, s.From.Table, mv)
			if err != nil {
				return nil, true, err
			}
		} else if isCatalogViewSource(env, s.From.Table) {
			return nil, false, nil
		} else {
			return nil, true, err
		}
	}
	template, ok, err := loadSimpleSelectPlanTemplate(table, s)
	if !ok || err != nil {
		return nil, ok, err
	}
	plan := *template
	// Access paths are value- and data-dependent. Never retain them in the
	// cached shape: a subsequent prepared execution may bind another key and
	// DML may have rebuilt the index entry arrays in the meantime.
	plan.rowIDs = nil
	plan.scanType = "TABLE SCAN"
	plan.indexName = ""
	plan.indexPredicates = nil
	plan.residualFilter = false
	plan.coveringIndex = false
	plan.estimatedRows = len(table.Rows)
	if idx, values, predicates, residual := selectSecondaryIndex(table, plan.colIndex, s.Where); idx != nil {
		var rowIDs []int
		var seekErr error
		if len(values) == len(idx.Columns) {
			rowIDs, seekErr = table.LookupSecondaryIndexPoint(idx, values)
		} else {
			rowIDs, seekErr = table.LookupSecondaryIndexPrefix(idx, values)
		}
		if seekErr != nil {
			return nil, true, seekErr
		}
		plan.rowIDs = rowIDs
		plan.scanType = "INDEX " + seekKind(len(values), len(idx.Columns))
		plan.indexName = idx.Name
		plan.indexPredicates = predicates
		plan.residualFilter = residual
		plan.coveringIndex = projectionsCoveredByIndex(plan.projs, idx, table)
		plan.estimatedRows = len(rowIDs)
	}
	return &plan, true, nil
}

func loadSimpleSelectPlanTemplate(table *storage.Table, s *Select) (*simpleSelectPlan, bool, error) {
	cache := s.simplePlanCache
	cacheable := cache != nil && simplePlanCacheSafe(s.Where)
	if cacheable {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if cache.plan != nil && cache.table == table && cache.colCount == len(table.Cols) {
			return cache.plan, true, nil
		}
	}

	colIndex := simpleColumnIndex(table, aliasOr(s.From))

	var projs []simpleProjection
	var outputCols []string
	if len(s.Projs) == 1 && s.Projs[0].Star && s.Projs[0].Alias == "" {
		// SELECT * FROM t (single table, no join — guaranteed by
		// simpleSelectEligible): expand directly into one raw-column
		// projection per table column instead of falling back to the
		// general Row-map path. Star previously disqualified the fast path
		// entirely, making "SELECT *" tens of times slower than an
		// equivalent narrow SELECT on the same table.
		projs, outputCols = buildSimpleSelectStarProjections(table, aliasOr(s.From))
	} else {
		var projOk bool
		projs, outputCols, projOk = buildSimpleSelectProjections(s.Projs, colIndex)
		if !projOk {
			return nil, false, nil
		}
	}
	orderExprs, ok := buildSimpleSelectOrderExprs(s.OrderBy, projs)
	if !ok {
		return nil, false, nil
	}
	filter := buildRawFilter(colIndex, s.Where)
	if filter == nil {
		return nil, false, nil
	}

	plan := &simpleSelectPlan{
		table:         table,
		colIndex:      colIndex,
		projs:         projs,
		orderBy:       s.OrderBy,
		orderExprs:    orderExprs,
		where:         s.Where,
		filter:        filter,
		limit:         s.Limit,
		offset:        s.Offset,
		outputCols:    outputCols,
		rowMapCap:     simpleProjectionMapCap(projs),
		scanType:      "TABLE SCAN",
		estimatedRows: len(table.Rows),
	}
	if cacheable {
		cache.table = table
		cache.colCount = len(table.Cols)
		cache.plan = plan
	}
	return plan, true, nil
}

// simplePlanCacheSafe rejects bound forms whose compiled filter captures the
// current parameter value (LIKE/IN/regexp). Plain comparisons use a dynamic
// bound-literal filter and are safe to reuse.
func simplePlanCacheSafe(expr Expr) bool {
	switch ex := expr.(type) {
	case nil, *VarRef:
		return true
	case *Literal:
		return !ex.Parameter
	case *Unary:
		return simplePlanCacheSafe(ex.Expr)
	case *IsNull:
		return simplePlanCacheSafe(ex.Expr)
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" || isComparisonOp(ex.Op) {
			return simplePlanCacheSafeComparisonSide(ex.Left) && simplePlanCacheSafeComparisonSide(ex.Right)
		}
		return !exprContainsBoundParameter(expr)
	default:
		return !exprContainsBoundParameter(expr)
	}
}

func simplePlanCacheSafeComparisonSide(expr Expr) bool {
	switch ex := expr.(type) {
	case *Literal:
		return true // dynamic comparison filters dereference Parameter literals
	case *Binary:
		return simplePlanCacheSafe(ex)
	default:
		return !exprContainsBoundParameter(expr)
	}
}

func exprContainsBoundParameter(expr Expr) bool {
	switch ex := expr.(type) {
	case nil:
		return false
	case *Literal:
		return ex.Parameter
	case *Unary:
		return exprContainsBoundParameter(ex.Expr)
	case *Binary:
		return exprContainsBoundParameter(ex.Left) || exprContainsBoundParameter(ex.Right)
	case *IsNull:
		return exprContainsBoundParameter(ex.Expr)
	case *LikeExpr:
		return exprContainsBoundParameter(ex.Expr) || exprContainsBoundParameter(ex.Pattern) || exprContainsBoundParameter(ex.Escape)
	case *RegexpExpr:
		return exprContainsBoundParameter(ex.Expr) || exprContainsBoundParameter(ex.Pattern)
	case *InExpr:
		if exprContainsBoundParameter(ex.Expr) {
			return true
		}
		for _, value := range ex.Values {
			if exprContainsBoundParameter(value) {
				return true
			}
		}
	case *FuncCall:
		for _, arg := range ex.Args {
			if exprContainsBoundParameter(arg) {
				return true
			}
		}
	}
	return false
}

func seekKind(prefixCols, allCols int) string {
	if prefixCols == allCols {
		return "POINT SEEK"
	}
	return "PREFIX SEEK"
}

// selectSecondaryIndex extracts equality terms from a simple WHERE tree and
// chooses the longest matching composite-index prefix. Other predicates stay
// as residual filters and are still evaluated by the normal raw evaluator.
func selectSecondaryIndex(table *storage.Table, colIndex map[string]int, where Expr) (*storage.SecondaryIndex, []any, []string, bool) {
	if where == nil || len(table.Indexes) == 0 {
		return nil, nil, nil, false
	}
	equalities := make(map[int]any)
	totalTerms := collectEqualityTerms(where, colIndex, equalities)
	var chosen *storage.SecondaryIndex
	var values []any
	var predicates []string
	for _, idx := range table.Indexes {
		candidate := make([]any, 0, len(idx.Columns))
		candidatePredicates := make([]string, 0, len(idx.Columns))
		for _, column := range idx.Columns {
			pos, err := table.ColIndex(column)
			if err != nil {
				break
			}
			value, ok := equalities[pos]
			if !ok {
				break
			}
			candidate = append(candidate, value)
			candidatePredicates = append(candidatePredicates, column+" = ?")
		}
		if len(candidate) == 0 || (chosen != nil && len(candidate) <= len(values)) {
			continue
		}
		chosen, values, predicates = idx, candidate, candidatePredicates
	}
	if chosen == nil {
		return nil, nil, nil, false
	}
	return chosen, values, predicates, totalTerms != len(values)
}

func collectEqualityTerms(expr Expr, colIndex map[string]int, out map[int]any) int {
	b, ok := expr.(*Binary)
	if !ok {
		return 0
	}
	if b.Op == "AND" {
		return collectEqualityTerms(b.Left, colIndex, out) + collectEqualityTerms(b.Right, colIndex, out)
	}
	if b.Op != "=" {
		return 0
	}
	if ref, ok := b.Left.(*VarRef); ok {
		if lit, ok := b.Right.(*Literal); ok {
			if pos, found := colIndex[ref.Lower]; found {
				out[pos] = lit.Val
				return 1
			}
		}
	}
	if ref, ok := b.Right.(*VarRef); ok {
		if lit, ok := b.Left.(*Literal); ok {
			if pos, found := colIndex[ref.Lower]; found {
				out[pos] = lit.Val
				return 1
			}
		}
	}
	return 0
}

func projectionsCoveredByIndex(projs []simpleProjection, idx *storage.SecondaryIndex, table *storage.Table) bool {
	covered := make(map[int]struct{}, len(idx.Columns))
	for _, column := range idx.Columns {
		if pos, err := table.ColIndex(column); err == nil {
			covered[pos] = struct{}{}
		}
	}
	for _, proj := range projs {
		if proj.colIdx < 0 {
			return false
		}
		if _, ok := covered[proj.colIdx]; !ok {
			return false
		}
	}
	return true
}

func isCatalogViewSource(env ExecEnv, name string) bool {
	if name == "" {
		return false
	}
	schema, objectName := splitObjectName(name)
	if _, ok := env.db.Catalog().GetView(schema, objectName); ok {
		return true
	}
	if _, ok := env.db.Catalog().GetMaterializedView(schema, objectName); ok {
		return true
	}
	return false
}

func simpleSelectEligible(s *Select) bool {
	if s.Distinct || len(s.DistinctOn) > 0 || len(s.CTEs) > 0 || len(s.Joins) > 0 ||
		len(s.GroupBy) > 0 || s.Having != nil || s.Union != nil || s.Pivot != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil {
		return false
	}
	if strings.Contains(strings.ToLower(s.From.Table), ".") {
		return false
	}
	if isSQLiteSchemaTable(s.From.Table) {
		return false
	}
	return !(anyAggInSelect(s.Projs) || anyWindowInSelect(s.Projs))
}

func buildSimpleSelectProjections(items []SelectItem, colIndex map[string]int) ([]simpleProjection, []string, bool) {
	projs := make([]simpleProjection, 0, len(items))
	outputCols := make([]string, 0, len(items))
	for i, it := range items {
		proj, ok := buildSimpleSelectProjection(it, i, colIndex)
		if !ok {
			return nil, nil, false
		}
		projs = append(projs, proj)
		outputCols = append(outputCols, proj.name)
	}
	return projs, outputCols, true
}

func simpleProjectionMapCap(projs []simpleProjection) int {
	capHint := len(projs)
	for _, p := range projs {
		if p.altKey != "" {
			capHint++
		}
	}
	return capHint
}

// buildSimpleSelectStarProjections builds one direct-column-reference
// projection per table column, in schema order — the raw fast-path
// equivalent of "expand * to all columns". Each projection has colIdx >= 0,
// so projectRawRow copies straight from raw[] with no evalRawExpr call.
//
// Both the unqualified ("col") and qualified ("alias.col") Row map keys are
// populated via altKey, matching rowsFromTable's dual-key output — callers
// may look up either form (e.g. tsql.GetVal(row, "orders.id")), and several
// tests/public-API examples rely on the qualified form being present even
// for SELECT *.
//
// expr is also populated (as a plain VarRef) purely so ORDER BY on a star
// query can resolve a column name via findSimpleSelectOrderExpr, which
// returns .expr regardless of whether the fast colIdx path was used for
// projection itself.
func buildSimpleSelectStarProjections(table *storage.Table, alias string) ([]simpleProjection, []string) {
	projs := make([]simpleProjection, len(table.Cols))
	outputCols := make([]string, len(table.Cols))
	for i, c := range table.Cols {
		lower := strings.ToLower(c.Name)
		projs[i] = simpleProjection{
			name:   c.Name,
			key:    lower,
			altKey: strings.ToLower(alias) + "." + lower,
			side:   -1,
			colIdx: i,
			expr:   &VarRef{Name: c.Name, Lower: lower},
		}
		outputCols[i] = c.Name
	}
	return projs, outputCols
}

func buildSimpleSelectProjection(it SelectItem, idx int, colIndex map[string]int) (simpleProjection, bool) {
	if it.Star || !isSimpleRawExpr(it.Expr) {
		return simpleProjection{}, false
	}
	name := projName(it, idx)
	if name == "" {
		return simpleProjection{}, false
	}
	colIdx := -1
	if ref, ok := it.Expr.(*VarRef); ok {
		if idx, ok2 := colIndex[strings.ToLower(ref.Name)]; ok2 {
			colIdx = idx
		}
	}
	return simpleProjection{
		name:   name,
		key:    strings.ToLower(name),
		side:   -1,
		colIdx: colIdx,
		expr:   it.Expr,
	}, true
}

func buildSimpleSelectOrderExprs(orderBy []OrderItem, projs []simpleProjection) ([]Expr, bool) {
	orderExprs := make([]Expr, 0, len(orderBy))
	for _, oi := range orderBy {
		expr, ok := findSimpleSelectOrderExpr(oi.Col, projs)
		if !ok {
			return nil, false
		}
		orderExprs = append(orderExprs, expr)
	}
	return orderExprs, true
}

func findSimpleSelectOrderExpr(col string, projs []simpleProjection) (Expr, bool) {
	for _, p := range projs {
		if strings.EqualFold(p.name, col) {
			return p.expr, true
		}
	}
	return nil, false
}

func simpleColumnIndex(t *storage.Table, alias string) map[string]int {
	idx := make(map[string]int, len(t.Cols)*3)
	tableName := strings.ToLower(t.Name)
	aliasName := strings.ToLower(alias)
	for i, c := range t.Cols {
		col := strings.ToLower(c.Name)
		idx[col] = i
		idx[tableName+"."+col] = i
		if aliasName != "" {
			idx[aliasName+"."+col] = i
		}
	}
	return idx
}

func simpleSelectInitialCap(plan *simpleSelectPlan) int {
	if plan.limit != nil {
		capHint := *plan.limit
		if plan.offset != nil {
			capHint += *plan.offset
		}
		if capHint > 0 && capHint < len(plan.table.Rows) {
			return capHint
		}
	}
	if plan.where == nil && len(plan.table.Rows) > 0 {
		return len(plan.table.Rows)
	}
	if len(plan.table.Rows) < 64 {
		return len(plan.table.Rows)
	}
	return 64
}

func isSimpleRawExpr(e Expr) bool {
	switch ex := e.(type) {
	case nil:
		return true
	case *Literal, *VarRef:
		return true
	case *Unary:
		return (ex.Op == "+" || ex.Op == "-" || ex.Op == "NOT") && isSimpleRawExpr(ex.Expr)
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" || isComparisonOp(ex.Op) || isArithmeticOp(ex.Op) {
			return isSimpleRawExpr(ex.Left) && isSimpleRawExpr(ex.Right)
		}
		return false
	case *IsNull:
		return isSimpleRawExpr(ex.Expr)
	case *LikeExpr:
		// LIKE/ILIKE/GLOB with a literal pattern and no dynamic escape is safe in the fast path.
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Pattern) &&
			(ex.Escape == nil || isSimpleRawExpr(ex.Escape))
	case *RegexpExpr:
		// REGEXP/RLIKE/SIMILAR TO with literal pattern is safe in the fast path.
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Pattern)
	case *BetweenExpr:
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Lo) && isSimpleRawExpr(ex.Hi)
	case *InExpr:
		if !isSimpleRawExpr(ex.Expr) {
			return false
		}
		for _, v := range ex.Values {
			if !isSimpleRawExpr(v) {
				return false
			}
		}
		return true
	case *FuncCall:
		if ex.Over != nil {
			return false
		}
		if rowAwareFuncNames[ex.Name] {
			// Reads the ambient Row directly; the raw path pre-evaluates
			// args and substitutes an empty Row, which would silently
			// always return "". Must go through the general evaluator.
			return false
		}
		for _, arg := range ex.Args {
			if !isSimpleRawExpr(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// rowAwareFuncNames lists scalar functions that read the ambient Row map
// directly rather than only their own evaluated arguments (e.g. ROW_TO_TEXT).
// Such calls must never execute through a raw ([]any + evalRawExpr) fast
// path, which resolves each arg into a Literal and then substitutes an
// empty Row — see evalRawFuncCall. Checked both by isSimpleRawExpr (gates
// whole-plan eligibility) and exprHasRowAwareFuncCall (gates the AND/OR
// raw-filter fallback in buildRawFilter, a separate mechanism that can
// invoke evalRawExpr on a sub-expression even when the overall plan looked
// eligible).
var rowAwareFuncNames = map[string]bool{
	"ROW_TO_TEXT": true,
}

// exprHasRowAwareFuncCall reports whether e, or any sub-expression reachable
// through the node kinds evalRawExpr supports, calls a row-aware function.
func exprHasRowAwareFuncCall(e Expr) bool {
	switch ex := e.(type) {
	case nil, *VarRef, *Literal:
		return false
	case *Unary:
		return exprHasRowAwareFuncCall(ex.Expr)
	case *Binary:
		return exprHasRowAwareFuncCall(ex.Left) || exprHasRowAwareFuncCall(ex.Right)
	case *IsNull:
		return exprHasRowAwareFuncCall(ex.Expr)
	case *LikeExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Pattern) || exprHasRowAwareFuncCall(ex.Escape)
	case *RegexpExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Pattern)
	case *BetweenExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Lo) || exprHasRowAwareFuncCall(ex.Hi)
	case *InExpr:
		if exprHasRowAwareFuncCall(ex.Expr) {
			return true
		}
		for _, v := range ex.Values {
			if exprHasRowAwareFuncCall(v) {
				return true
			}
		}
		return false
	case *FuncCall:
		if rowAwareFuncNames[ex.Name] {
			return true
		}
		for _, arg := range ex.Args {
			if exprHasRowAwareFuncCall(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isSimpleRawPredicate(e Expr) bool {
	if e == nil {
		return true
	}
	return isSimpleRawExpr(e)
}

func isComparisonOp(op string) bool {
	switch op {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func isArithmeticOp(op string) bool {
	switch op {
	case "+", "-", "*", "/":
		return true
	default:
		return false
	}
}

func evalRawWhere(plan *simpleSelectPlan, raw []any) (bool, error) {
	if plan.filter != nil {
		return plan.filter(raw)
	}
	if plan.where == nil {
		return true, nil
	}
	v, err := evalRawExpr(plan, raw, plan.where)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

// buildRawFilter attempts to compile a WHERE expression into a closure that
// operates directly on raw row slices ([]any) without going through the
// general evalRawExpr machinery.  It handles the most common patterns:
//   - col op literal   (equality, inequality, ordering)
//   - boolean_col      (truthy column reference)
//   - NOT boolean_col
//   - AND / OR of the above
//
// Returns nil when the expression is too complex to compile, in which case
// evalRawExpr is used as the fallback.
func buildRawFilter(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	if f := buildRawFilterSpecialized(colIndex, e); f != nil {
		return f
	}
	// General fallback: predicates the specialized builders don't compile —
	// e.g. function-call comparisons like
	// VEC_COSINE_SIMILARITY(embedding, ...) > 0.5 — can still run on raw
	// rows through evalRawExpr, provided the expression only uses node
	// kinds the raw evaluator supports and no row-aware functions.
	// Previously such WHERE clauses disqualified the entire plan and forced
	// the general Row-map evaluator, which allocates two map entries per
	// column per row; on scoring-heavy RAG scans that map traffic — not the
	// predicate itself — dominated the query cost.
	return buildRawExprFilter(colIndex, e)
}

// buildRawFilterSpecialized compiles the predicate forms with dedicated,
// type-specialized closures (column/literal comparisons, LIKE with literal
// pattern, IN lists, ...). Returns nil for anything else. AND/OR use this
// distinction as a cost signal: a specialized side is cheap (a raw slice
// access plus a comparison) and runs first, while an expression that only
// evaluates through the evalRawExpr fallback (typically a function call
// such as a vector distance) runs second, only on rows that survive.
func buildRawFilterSpecialized(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	if e == nil {
		return func([]any) (bool, error) { return true, nil }
	}
	switch ex := e.(type) {
	case *VarRef:
		return buildRawFilterVarRef(colIndex, ex)
	case *Unary:
		return buildRawFilterUnary(colIndex, ex)
	case *IsNull:
		return buildRawFilterIsNull(colIndex, ex)
	case *Binary:
		return buildRawFilterBinary(colIndex, ex)
	case *LikeExpr:
		return buildRawFilterLike(colIndex, ex)
	case *RegexpExpr:
		return buildRawFilterRegexp(colIndex, ex)
	case *InExpr:
		return buildRawFilterIn(colIndex, ex)
	}
	return nil
}

// buildRawExprFilter wraps an arbitrary raw-evaluable expression as a filter,
// using the same truthiness conversion (toTri == tvTrue) as the AND/OR
// fallback paths so three-valued logic matches the general evaluator.
// Returns nil when the expression cannot run on raw rows.
func buildRawExprFilter(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	if !isSimpleRawExpr(e) || exprHasRowAwareFuncCall(e) {
		return nil
	}
	plan := &simpleSelectPlan{colIndex: colIndex}
	return func(raw []any) (bool, error) {
		v, err := evalRawExpr(plan, raw, e)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawFilterVarRef(colIndex map[string]int, ex *VarRef) func([]any) (bool, error) {
	key := ex.Lower
	if key == "" {
		key = strings.ToLower(ex.Name)
	}
	colIdx, ok := colIndex[key]
	if !ok {
		return nil
	}
	return func(raw []any) (bool, error) { return truthy(raw[colIdx]), nil }
}

func buildRawFilterUnary(colIndex map[string]int, ex *Unary) func([]any) (bool, error) {
	if ex.Op != "NOT" {
		return nil
	}
	inner := buildRawFilter(colIndex, ex.Expr)
	if inner == nil {
		return nil
	}
	return func(raw []any) (bool, error) {
		b, err := inner(raw)
		return !b, err
	}
}

func buildRawFilterIsNull(colIndex map[string]int, ex *IsNull) func([]any) (bool, error) {
	innerRef, ok := ex.Expr.(*VarRef)
	if !ok {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(innerRef.Name)]
	if !ok {
		return nil
	}
	if ex.Negate {
		return func(raw []any) (bool, error) { return raw[colIdx] != nil, nil }
	}
	return func(raw []any) (bool, error) { return raw[colIdx] == nil, nil }
}

func buildRawFilterBinary(colIndex map[string]int, ex *Binary) func([]any) (bool, error) {
	switch ex.Op {
	case "AND":
		return buildRawAndFilter(colIndex, ex.Left, ex.Right)
	case "OR":
		return buildRawOrFilter(colIndex, ex.Left, ex.Right)
	default:
		if isComparisonOp(ex.Op) {
			return buildRawComparisonFilter(colIndex, ex)
		}
		return nil
	}
}

func buildRawAndFilter(colIndex map[string]int, leftExpr, rightExpr Expr) func([]any) (bool, error) {
	left := buildRawFilterSpecialized(colIndex, leftExpr)
	right := buildRawFilterSpecialized(colIndex, rightExpr)
	if left != nil && right != nil {
		return func(raw []any) (bool, error) {
			l, err := left(raw)
			if err != nil || !l {
				return false, err
			}
			return right(raw)
		}
	}
	if left == nil && right == nil {
		// Neither side compiles to a specialized filter (e.g. two
		// function-call predicates). Run both through the expression
		// fallback, left first, so the plan still avoids the Row-map
		// evaluator.
		lf := buildRawExprFilter(colIndex, leftExpr)
		rf := buildRawExprFilter(colIndex, rightExpr)
		if lf == nil || rf == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			l, err := lf(raw)
			if err != nil || !l {
				return false, err
			}
			return rf(raw)
		}
	}

	// Exactly one side compiled. The specialized side is cheap (raw slice
	// access + comparison), so it runs first regardless of written order;
	// the expression side runs only on surviving rows. The fallback
	// evaluates via evalRawExpr(plan, raw, expr), which substitutes an
	// empty Row for any FuncCall — wrong for a row-aware function like
	// ROW_TO_TEXT. Refuse to build a raw filter at all in that case so the
	// caller falls back to the general (correct) Row-based evaluator.
	if left == nil {
		if exprHasRowAwareFuncCall(leftExpr) {
			return nil
		}
		return buildRawAndFilterWithFallback(colIndex, leftExpr, right)
	}
	if exprHasRowAwareFuncCall(rightExpr) {
		return nil
	}
	return buildRawAndFilterWithFallback(colIndex, rightExpr, left)
}

func buildRawAndFilterWithFallback(colIndex map[string]int, expr Expr, fastFilter func([]any) (bool, error)) func([]any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: colIndex}
	return func(raw []any) (bool, error) {
		fast, err := fastFilter(raw)
		if err != nil || !fast {
			return false, err
		}
		v, err := evalRawExpr(plan, raw, expr)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawOrFilter(colIndex map[string]int, leftExpr, rightExpr Expr) func([]any) (bool, error) {
	left := buildRawFilterSpecialized(colIndex, leftExpr)
	right := buildRawFilterSpecialized(colIndex, rightExpr)
	if left != nil && right != nil {
		return func(raw []any) (bool, error) {
			l, err := left(raw)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return right(raw)
		}
	}
	if left == nil && right == nil {
		// See the matching comment in buildRawAndFilter.
		lf := buildRawExprFilter(colIndex, leftExpr)
		rf := buildRawExprFilter(colIndex, rightExpr)
		if lf == nil || rf == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			l, err := lf(raw)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return rf(raw)
		}
	}

	// See the matching comment in buildRawAndFilter: the specialized (cheap)
	// side short-circuits first; the fallback evaluates the unbuildable side
	// via evalRawExpr with an empty Row, which is wrong for row-aware
	// functions.
	if left == nil {
		if exprHasRowAwareFuncCall(leftExpr) {
			return nil
		}
		return buildRawOrFilterWithFallback(colIndex, leftExpr, right)
	}
	if exprHasRowAwareFuncCall(rightExpr) {
		return nil
	}
	return buildRawOrFilterWithFallback(colIndex, rightExpr, left)

}

func buildRawOrFilterWithFallback(colIndex map[string]int, expr Expr, fastFilter func([]any) (bool, error)) func([]any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: colIndex}
	return func(raw []any) (bool, error) {
		fast, err := fastFilter(raw)
		if err != nil || fast {
			return fast, err
		}
		v, err := evalRawExpr(plan, raw, expr)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawComparisonFilter(colIndex map[string]int, ex *Binary) func([]any) (bool, error) {
	if ref, ok := ex.Left.(*VarRef); ok {
		if lit, ok := ex.Right.(*Literal); ok {
			if colIdx, ok := colIndex[strings.ToLower(ref.Name)]; ok {
				if lit.Parameter {
					return buildBoundLiteralFilter(colIdx, ex.Op, lit)
				}
				return buildColLiteralFilter(colIdx, ex.Op, lit.Val)
			}
		}
	}
	if lit, ok := ex.Left.(*Literal); ok {
		if ref, ok := ex.Right.(*VarRef); ok {
			if colIdx, ok := colIndex[strings.ToLower(ref.Name)]; ok {
				if lit.Parameter {
					return buildBoundLiteralFilter(colIdx, reverseComparisonOp(ex.Op), lit)
				}
				return buildColLiteralFilter(colIdx, reverseComparisonOp(ex.Op), lit.Val)
			}
		}
	}
	if lRef, ok := ex.Left.(*VarRef); ok {
		if rRef, ok := ex.Right.(*VarRef); ok {
			lIdx, lok := colIndex[strings.ToLower(lRef.Name)]
			rIdx, rok := colIndex[strings.ToLower(rRef.Name)]
			if lok && rok {
				return buildColColFilter(lIdx, ex.Op, rIdx)
			}
		}
	}
	return nil
}

// buildBoundLiteralFilter reads the current parameter value on every call.
// Prepared statements mutate Literal.Val under their statement mutex, while
// the cached plan shape and closure stay immutable.
func buildBoundLiteralFilter(colIdx int, op string, literal *Literal) func([]any) (bool, error) {
	return func(raw []any) (bool, error) {
		a, b := raw[colIdx], literal.Val
		if a == nil || b == nil {
			return false, nil
		}
		switch op {
		case "=":
			return rawEqual(a, b), nil
		case "!=", "<>":
			return !rawEqual(a, b), nil
		}
		comparison, err := compare(a, b)
		if err != nil {
			return false, err
		}
		switch op {
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		case ">=":
			return comparison >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator %q", op)
		}
	}
}

func buildRawFilterLike(colIndex map[string]int, ex *LikeExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	pat, isLit := ex.Pattern.(*Literal)
	if !isLit {
		return nil
	}
	pattern, isStr := pat.Val.(string)
	if !isStr {
		return nil
	}
	if ex.GlobStyle {
		return buildCompiledGlobFilter(colIdx, pattern, ex.CaseInsensitive, ex.Negate)
	}
	if ex.Escape != nil {
		return nil
	}
	if ex.CaseInsensitive {
		return buildCompiledILikeFilter(colIdx, pattern, ex.Negate)
	}
	return buildCompiledLikeFilter(colIdx, pattern, ex.Negate)
}

func buildRawFilterRegexp(colIndex map[string]int, ex *RegexpExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	pat, isLit := ex.Pattern.(*Literal)
	if !isLit {
		return nil
	}
	pattern, isStr := pat.Val.(string)
	if !isStr {
		return nil
	}
	if ex.SimilarTo {
		pattern = similarToRegexp(pattern)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	negate := ex.Negate
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			if raw[colIdx] == nil {
				return false, nil
			}
			s = fmt.Sprintf("%v", raw[colIdx])
		}
		matched := re.MatchString(s)
		if negate {
			return !matched, nil
		}
		return matched, nil
	}
}

func buildRawFilterIn(colIndex map[string]int, ex *InExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	litVals := make([]any, 0, len(ex.Values))
	for _, v := range ex.Values {
		lit, ok := v.(*Literal)
		if !ok {
			return nil
		}
		litVals = append(litVals, lit.Val)
	}
	return buildInFilter(colIdx, litVals, ex.Negate)
}

// reverseComparisonOp reverses the direction of a comparison operator,
// so that "literal op col" can be treated as "col reversed_op literal".
func reverseComparisonOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op // "=" and "!=" / "<>" are symmetric
	}
}

// buildColLiteralFilter builds a fast comparison closure for "raw[colIdx] op litVal".
// The returned function is type-specialized for int, float64, and string to avoid
// the overhead of the generic compare() path.
func buildColLiteralFilter(colIdx int, op string, litVal any) func([]any) (bool, error) {
	switch op {
	case "=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			return rawEqual(a, litVal), nil
		}
	case "!=", "<>":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			return !rawEqual(a, litVal), nil
		}
	case "<", "<=", ">", ">=":
		// Specialize for the three common literal types to avoid compare().
		switch lv := litVal.(type) {
		case int:
			return buildIntCmpFilter(colIdx, op, lv)
		case int64:
			return buildInt64CmpFilter(colIdx, op, lv)
		case float64:
			return buildFloat64CmpFilter(colIdx, op, lv)
		case string:
			return buildStringCmpFilter(colIdx, op, lv)
		}
		// Generic fallback via compare().
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			cmp, err := compare(a, litVal)
			if err != nil {
				return false, err
			}
			switch op {
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">":
				return cmp > 0, nil
			default: // ">="
				return cmp >= 0, nil
			}
		}
	}
	return nil
}

// buildIntCmpFilter builds a specialized ordering filter for an int literal.
func buildIntCmpFilter(colIdx int, op string, lit int) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av < lit, nil
			case int64:
				return av < int64(lit), nil
			case float64:
				return av < float64(lit), nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av <= lit, nil
			case int64:
				return av <= int64(lit), nil
			case float64:
				return av <= float64(lit), nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av > lit, nil
			case int64:
				return av > int64(lit), nil
			case float64:
				return av > float64(lit), nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av >= lit, nil
			case int64:
				return av >= int64(lit), nil
			case float64:
				return av >= float64(lit), nil
			}
			return false, nil
		}
	}
	return nil
}

// buildInt64CmpFilter builds a specialized ordering filter for an int64 literal.
func buildInt64CmpFilter(colIdx int, op string, lit int64) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) < lit, nil
			case int64:
				return av < lit, nil
			case float64:
				return av < float64(lit), nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) <= lit, nil
			case int64:
				return av <= lit, nil
			case float64:
				return av <= float64(lit), nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) > lit, nil
			case int64:
				return av > lit, nil
			case float64:
				return av > float64(lit), nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) >= lit, nil
			case int64:
				return av >= lit, nil
			case float64:
				return av >= float64(lit), nil
			}
			return false, nil
		}
	}
	return nil
}

// buildFloat64CmpFilter builds a specialized ordering filter for a float64 literal.
func buildFloat64CmpFilter(colIdx int, op string, lit float64) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f < lit, nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f <= lit, nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f > lit, nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f >= lit, nil
			}
			return false, nil
		}
	}
	return nil
}

// buildStringCmpFilter builds a specialized ordering filter for a string literal.
func buildStringCmpFilter(colIdx int, op string, lit string) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s < lit, nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s <= lit, nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s > lit, nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s >= lit, nil
			}
			return false, nil
		}
	}
	return nil
}

// buildColColFilter builds a filter for "raw[lIdx] op raw[rIdx]" (col op col).
func buildColColFilter(lIdx int, op string, rIdx int) func([]any) (bool, error) {
	return func(raw []any) (bool, error) {
		a, b := raw[lIdx], raw[rIdx]
		if a == nil || b == nil {
			return false, nil
		}
		switch op {
		case "=":
			return rawEqual(a, b), nil
		case "!=", "<>":
			return !rawEqual(a, b), nil
		}
		cmp, err := compare(a, b)
		if err != nil {
			return false, err
		}
		switch op {
		case "<":
			return cmp < 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">":
			return cmp > 0, nil
		default: // ">="
			return cmp >= 0, nil
		}
	}
}

// numericFast converts the common numeric types to float64 without the decimal
// or string-parsing branches of the general numeric() helper.
func numericFast(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// buildCompiledLikeFilter compiles a SQL LIKE pattern (with default escape '\\')
// into a specialized closure. Common patterns are reduced to library calls:
//   - 'exact'       →  s == "exact"
//   - 'prefix%'     →  strings.HasPrefix(s, "prefix")
//   - '%suffix'     →  strings.HasSuffix(s, "suffix")
//   - '%middle%'    →  strings.Contains(s, "middle")
//
// Patterns containing '_' wildcards or multiple '%' anchors fall back to the
// general matchLikePattern function.
func buildCompiledLikeFilter(colIdx int, pattern string, negate bool) func([]any) (bool, error) {
	matchFn := func(match func(string) bool) func([]any) (bool, error) {
		if negate {
			return func(raw []any) (bool, error) {
				s, ok := raw[colIdx].(string)
				if !ok {
					return false, nil
				}
				return !match(s), nil
			}
		}
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return match(s), nil
		}
	}

	// No wildcards → exact match.
	if !strings.ContainsAny(pattern, "%_") {
		return matchFn(func(s string) bool { return s == pattern })
	}

	// prefix% – no other wildcards.
	if strings.HasSuffix(pattern, "%") && !strings.ContainsAny(pattern[:len(pattern)-1], "%_") {
		prefix := pattern[:len(pattern)-1]
		return matchFn(func(s string) bool { return strings.HasPrefix(s, prefix) })
	}

	// %suffix – no other wildcards.
	if strings.HasPrefix(pattern, "%") && !strings.ContainsAny(pattern[1:], "%_") {
		suffix := pattern[1:]
		return matchFn(func(s string) bool { return strings.HasSuffix(s, suffix) })
	}

	// %middle% – no other wildcards.
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") &&
		len(pattern) >= 2 && !strings.ContainsAny(pattern[1:len(pattern)-1], "%_") {
		middle := pattern[1 : len(pattern)-1]
		return matchFn(func(s string) bool { return strings.Contains(s, middle) })
	}

	// Fall back to the general matcher.
	return matchFn(func(s string) bool { return matchLikePattern(s, pattern, '\\') })
}

// buildCompiledILikeFilter compiles an ILIKE / NOT ILIKE pattern into a closure.
// It pre-lowercases the pattern and lowercases each column value at match time,
// without any per-row heap allocations.
func buildCompiledILikeFilter(colIdx int, pattern string, negate bool) func([]any) (bool, error) {
	// Pre-lowercase the pattern once.
	lowerPat := strings.ToLower(pattern)

	// Pick the cheapest sub-matcher based on the lowercased pattern shape.
	var match func(string) bool
	switch {
	case !strings.ContainsAny(lowerPat, "%_"):
		match = func(s string) bool { return strings.ToLower(s) == lowerPat }
	case strings.HasSuffix(lowerPat, "%") && !strings.ContainsAny(lowerPat[:len(lowerPat)-1], "%_"):
		prefix := lowerPat[:len(lowerPat)-1]
		match = func(s string) bool { return strings.HasPrefix(strings.ToLower(s), prefix) }
	case strings.HasPrefix(lowerPat, "%") && !strings.ContainsAny(lowerPat[1:], "%_"):
		suffix := lowerPat[1:]
		match = func(s string) bool { return strings.HasSuffix(strings.ToLower(s), suffix) }
	case strings.HasPrefix(lowerPat, "%") && strings.HasSuffix(lowerPat, "%") &&
		len(lowerPat) >= 2 && !strings.ContainsAny(lowerPat[1:len(lowerPat)-1], "%_"):
		mid := lowerPat[1 : len(lowerPat)-1]
		match = func(s string) bool { return strings.Contains(strings.ToLower(s), mid) }
	default:
		match = func(s string) bool { return matchLikePattern(strings.ToLower(s), lowerPat, '\\') }
	}

	if negate {
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return !match(s), nil
		}
	}
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			return false, nil
		}
		return match(s), nil
	}
}

// buildCompiledGlobFilter compiles a GLOB / NOT GLOB pattern into a closure.
// GLOB wildcards: * matches any sequence, ? matches any single character.
func buildCompiledGlobFilter(colIdx int, pattern string, caseInsensitive, negate bool) func([]any) (bool, error) {
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
	}
	matchFn := func(s string) bool {
		if caseInsensitive {
			s = strings.ToLower(s)
		}
		return matchGlobPattern(s, pattern)
	}
	if negate {
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return !matchFn(s), nil
		}
	}
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			return false, nil
		}
		return matchFn(s), nil
	}
}

// buildInFilter builds a fast set-membership closure for col IN (litVals).
// It pre-builds typed maps for all-int and all-string value sets for O(1) lookup.
func buildInFilter(colIdx int, litVals []any, negate bool) func([]any) (bool, error) {
	// Try to build typed sets for O(1) lookup.
	allInt, allStr := true, true
	for _, v := range litVals {
		if _, ok := v.(int); !ok {
			allInt = false
		}
		if _, ok := v.(string); !ok {
			allStr = false
		}
	}

	if allInt {
		set := make(map[int]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(int)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				a := raw[colIdx]
				if a == nil {
					return false, nil
				}
				if ai, ok := a.(int); ok {
					_, found := set[ai]
					return !found, nil
				}
				// Fall back for type mismatches (e.g., stored as int64).
				for _, v := range litVals {
					if rawEqual(a, v) {
						return false, nil
					}
				}
				return true, nil
			}
		}
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil {
				return false, nil
			}
			if ai, ok := a.(int); ok {
				_, found := set[ai]
				return found, nil
			}
			for _, v := range litVals {
				if rawEqual(a, v) {
					return true, nil
				}
			}
			return false, nil
		}
	}

	if allStr {
		set := make(map[string]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(string)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				s, ok := raw[colIdx].(string)
				if !ok {
					return false, nil
				}
				_, found := set[s]
				return !found, nil
			}
		}
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			_, found := set[s]
			return found, nil
		}
	}

	// Generic fallback using rawEqual.
	if negate {
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			for _, v := range litVals {
				if rawEqual(a, v) {
					return false, nil
				}
			}
			return true, nil
		}
	}
	return func(raw []any) (bool, error) {
		a := raw[colIdx]
		for _, v := range litVals {
			if rawEqual(a, v) {
				return true, nil
			}
		}
		return false, nil
	}
}

// rawEqual performs a type-aware equality check between two interface values
// without going through the generic compare() function.  It covers the value
// types that tinySQL stores in table rows (int, int64, float64, string, bool).
func rawEqual(a, b any) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av == bv
		case int64:
			return int64(av) == bv
		case float64:
			return float64(av) == bv
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av == int64(bv)
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av == float64(bv)
		case int64:
			return av == float64(bv)
		case float64:
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	return false
}

func projectRawRow(plan *simpleSelectPlan, raw []any) (Row, error) {
	out := make(Row, plan.rowMapCap)
	for _, p := range plan.projs {
		var v any
		if p.colIdx >= 0 {
			// Direct column reference: skip type switch, map lookup, and ToLower.
			v = raw[p.colIdx]
		} else {
			var err error
			v, err = evalRawExpr(plan, raw, p.expr)
			if err != nil {
				return nil, err
			}
		}
		out[p.key] = v
		if p.altKey != "" {
			out[p.altKey] = v
		}
	}
	return out, nil
}

func evalRawExpr(plan *simpleSelectPlan, raw []any, e Expr) (any, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		i, ok := plan.colIndex[key]
		if !ok {
			if strings.Contains(ex.Name, ".") {
				return nil, fmt.Errorf("unknown column reference %q", ex.Name)
			}
			return nil, fmt.Errorf("unknown column %q", ex.Name)
		}
		if i < 0 || i >= len(raw) {
			return nil, fmt.Errorf("column %q is out of range", ex.Name)
		}
		return raw[i], nil
	case *IsNull:
		v, err := evalRawExpr(plan, raw, ex.Expr)
		if err != nil {
			return nil, err
		}
		is := isNull(v)
		if ex.Negate {
			return !is, nil
		}
		return is, nil
	case *Unary:
		return evalRawUnary(plan, raw, ex)
	case *Binary:
		return evalRawBinary(plan, raw, ex)
	case *LikeExpr:
		return evalRawLike(plan, raw, ex)
	case *InExpr:
		return evalRawIn(plan, raw, ex)
	case *RegexpExpr:
		return evalRawRegexp(plan, raw, ex)
	case *BetweenExpr:
		return evalRawBetween(plan, raw, ex)
	case *FuncCall:
		return evalRawFuncCall(plan, raw, ex)
	default:
		return nil, fmt.Errorf("unsupported fast-path expression %T", e)
	}
}

// evalRawBetween evaluates BETWEEN in the raw fast path with a single
// evaluation of the comparand.
func evalRawBetween(plan *simpleSelectPlan, raw []any, ex *BetweenExpr) (any, error) {
	v, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	lo, err := evalRawExpr(plan, raw, ex.Lo)
	if err != nil {
		return nil, err
	}
	hi, err := evalRawExpr(plan, raw, ex.Hi)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

// rawCallScratch holds the reusable argument wrappers for one
// evalRawFuncCall invocation. The raw fast path evaluates function calls
// once per row; allocating the args slice, one Literal per argument, the
// FuncCall copy (which escapes through the map-dispatched handler) and an
// empty Row map on every row made the allocator — not the function body —
// the dominant cost of expression-heavy scans (e.g. per-row
// VEC_COSINE_SIMILARITY in RAG queries). Handlers only read the wrappers to
// extract argument values and never retain them, so recycling the backing
// structs through a pool is safe; nested calls simply draw their own
// scratch instance.
type rawCallScratch struct {
	call FuncCall
	args []Expr
	lits []Literal
}

var rawCallScratchPool = sync.Pool{
	New: func() any { return new(rawCallScratch) },
}

// rawEmptyRow is the shared no-columns row passed to handlers on the raw
// fast path. All arguments arrive pre-evaluated as literals, so handlers
// never look anything up in it (ROW_TO_TEXT, which reads the ambient row,
// is excluded from this path by isSimpleRawExpr).
var rawEmptyRow = Row{}

func evalRawFuncCall(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	if ex.Over != nil {
		return nil, fmt.Errorf("window function %s is not supported in raw expression evaluation", ex.Name)
	}
	sc := rawCallScratchPool.Get().(*rawCallScratch)
	defer rawCallScratchPool.Put(sc)
	if cap(sc.args) < len(ex.Args) {
		sc.args = make([]Expr, len(ex.Args))
		sc.lits = make([]Literal, len(ex.Args))
	}
	args := sc.args[:len(ex.Args)]
	lits := sc.lits[:len(ex.Args)]
	for i, arg := range ex.Args {
		if lit, ok := arg.(*Literal); ok {
			args[i] = lit
			continue
		}
		v, err := evalRawExpr(plan, raw, arg)
		if err != nil {
			return nil, err
		}
		lits[i].Val = v
		args[i] = &lits[i]
	}
	sc.call = FuncCall{Name: ex.Name, Args: args, Star: ex.Star, Distinct: ex.Distinct}
	return evalFuncCall(ExecEnv{}, &sc.call, rawEmptyRow)
}

func evalRawUnary(plan *simpleSelectPlan, raw []any, ex *Unary) (any, error) {
	v, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
	}
}

func evalRawBinary(plan *simpleSelectPlan, raw []any, ex *Binary) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		lv, err := evalRawExpr(plan, raw, ex.Left)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" && toTri(lv) == tvFalse {
			return false, nil
		}
		if ex.Op == "OR" && toTri(lv) == tvTrue {
			return true, nil
		}
		rv, err := evalRawExpr(plan, raw, ex.Right)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" {
			return triToValue(triAnd(toTri(lv), toTri(rv))), nil
		}
		return triToValue(triOr(toTri(lv), toTri(rv))), nil
	}

	lv, err := evalRawExpr(plan, raw, ex.Left)
	if err != nil {
		return nil, err
	}
	rv, err := evalRawExpr(plan, raw, ex.Right)
	if err != nil {
		return nil, err
	}
	if isArithmeticOp(ex.Op) {
		return evalArithmeticBinary(ex.Op, lv, rv)
	}
	if isComparisonOp(ex.Op) {
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}

// evalRawLike evaluates a LIKE / NOT LIKE expression in the fast raw path.
// Both the subject and the pattern must be simple raw-path expressions.
func evalRawLike(plan *simpleSelectPlan, raw []any, ex *LikeExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalRawExpr(plan, raw, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}
	pattern, ok := patVal.(string)
	if !ok {
		pattern = fmt.Sprintf("%v", patVal)
	}
	var matched bool
	if ex.GlobStyle {
		if ex.CaseInsensitive {
			matched = matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern))
		} else {
			matched = matchGlobPattern(str, pattern)
		}
	} else {
		escapeChar := '\\'
		if ex.Escape != nil {
			escVal, err := evalRawExpr(plan, raw, ex.Escape)
			if err != nil {
				return nil, err
			}
			if escStr, ok := escVal.(string); ok && len(escStr) == 1 {
				escapeChar = rune(escStr[0])
			}
		}
		if ex.CaseInsensitive {
			matched = matchLikePattern(strings.ToLower(str), strings.ToLower(pattern), escapeChar)
		} else {
			matched = matchLikePattern(str, pattern, escapeChar)
		}
	}
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalRawRegexp evaluates REGEXP / RLIKE / SIMILAR TO in the raw fast path.
func evalRawRegexp(plan *simpleSelectPlan, raw []any, ex *RegexpExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalRawExpr(plan, raw, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str := fmt.Sprintf("%v", val)
	pattern := fmt.Sprintf("%v", patVal)
	if ex.SimilarTo {
		pattern = similarToRegexp(pattern)
	}
	re, err := compileCachedRegexp(pattern)
	if err != nil {
		return nil, fmt.Errorf("REGEXP: invalid pattern %q: %v", pattern, err)
	}
	matched := re.MatchString(str)
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalRawIn evaluates an IN / NOT IN expression in the raw fast path.
func evalRawIn(plan *simpleSelectPlan, raw []any, ex *InExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	for _, valExpr := range ex.Values {
		listVal, err := evalRawExpr(plan, raw, valExpr)
		if err != nil {
			return nil, err
		}
		if rawEqual(val, listVal) {
			if ex.Negate {
				return false, nil
			}
			return true, nil
		}
	}
	if ex.Negate {
		return true, nil
	}
	return false, nil
}

func processUnionClauses(env ExecEnv, union *UnionClause, leftRows []Row, leftCols []string) ([]Row, []string, error) {
	resultRows := leftRows
	resultCols := leftCols

	current := union
	for current != nil {
		// Execute the right-hand SELECT
		rightResult, err := executeSelect(env, current.Right)
		if err != nil {
			return nil, nil, err
		}

		// Validate column compatibility
		if len(rightResult.Cols) != len(resultCols) {
			return nil, nil, fmt.Errorf("UNION: column count mismatch between queries (%d vs %d)",
				len(resultCols), len(rightResult.Cols))
		}

		// Process the union based on type
		switch current.Type {
		case UnionAll:
			// UNION ALL: Just append all rows
			resultRows = append(resultRows, rightResult.Rows...)

		case UnionDistinct:
			// UNION: Append and then remove duplicates
			resultRows = append(resultRows, rightResult.Rows...)
			resultRows = distinctRows(resultRows, resultCols)

		case Except:
			// EXCEPT: Remove rows that exist in the right result
			resultRows = exceptRows(resultRows, rightResult.Rows, resultCols)

		case Intersect:
			// INTERSECT: Keep only rows that exist in both results
			resultRows = intersectRows(resultRows, rightResult.Rows, resultCols)
		}

		current = current.Next
	}

	return resultRows, resultCols, nil
}

func exceptRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are not in the right set
	var result []Row
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if !rightSet[key] {
			result = append(result, l)
		}
	}
	return result
}

func intersectRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are also in the right set
	var result []Row
	seen := make(map[string]bool)
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if rightSet[key] && !seen[key] {
			result = append(result, l)
			seen[key] = true
		}
	}
	return result
}

func rowSignature(row Row, cols []string) string {
	var buf strings.Builder
	for i, col := range cols {
		if i > 0 {
			buf.WriteByte('|')
		}
		val, _ := getVal(row, col)
		writeFmtKeyPart(&buf, val)
	}
	return buf.String()
}

func processJoins(env ExecEnv, joins []JoinClause, cur []Row) ([]Row, error) {
	for _, j := range joins {
		var rightRows []Row
		var rightTable *storage.Table
		var err error

		if j.Right.Subquery != nil {
			subRs, err := executeSelect(env, j.Right.Subquery)
			if err != nil {
				return nil, err
			}
			rightRows = make([]Row, len(subRs.Rows))
			for i, row := range subRs.Rows {
				rightRows[i] = make(Row)
				for k, v := range row {
					rightRows[i][strings.ToLower(k)] = v
					if j.Right.Alias != "" {
						rightRows[i][strings.ToLower(j.Right.Alias+"."+k)] = v
					}
				}
			}
			// build synthetic table metadata
			cols := make([]storage.Column, 0, len(subRs.Cols))
			for _, c := range subRs.Cols {
				cols = append(cols, storage.Column{Name: c})
			}
			rightTable = &storage.Table{Name: j.Right.Alias, Cols: cols}
		} else if j.Right.TableFunc != nil {
			tf := j.Right.TableFunc
			fn, ok := GetTableFunc(tf.Name)
			if !ok {
				return nil, fmt.Errorf("unknown table function: %s", tf.Name)
			}
			if err := fn.ValidateArgs(tf.Args); err != nil {
				return nil, err
			}
			rs, err := fn.Execute(env.ctx, tf.Args, env, nil)
			if err != nil {
				return nil, err
			}
			rightRows = make([]Row, len(rs.Rows))
			for i, row := range rs.Rows {
				rightRows[i] = make(Row)
				for k, v := range row {
					rightRows[i][strings.ToLower(k)] = v
					if j.Right.Alias != "" {
						rightRows[i][strings.ToLower(j.Right.Alias+"."+k)] = v
					}
				}
			}
			// synthetic table metadata from ResultSet columns
			cols := make([]storage.Column, 0, len(rs.Cols))
			for _, c := range rs.Cols {
				cols = append(cols, storage.Column{Name: c})
			}
			rightTable = &storage.Table{Name: j.Right.Alias, Cols: cols}
		} else {
			rt, err := env.db.Get(env.tenant, j.Right.Table)
			if err != nil {
				return nil, err
			}
			rightRows, _ = rowsFromTable(rt, aliasOr(j.Right))
			rightTable = rt
		}

		switch j.Type {
		case JoinInner:
			cur, err = processInnerJoin(env, cur, rightRows, j.On)
		case JoinLeft:
			cur, err = processLeftJoin(env, cur, rightRows, j.On, aliasOr(j.Right), rightTable)
		case JoinRight:
			cur, err = processRightJoin(env, cur, rightRows, j.On)
		case JoinFull:
			cur, err = processFullOuterJoin(env, cur, rightRows, j.On, aliasOr(j.Right), rightTable)
		case JoinCross:
			optimizer := &HashJoinOptimizer{env: env}
			cur, err = optimizer.processCrossJoin(cur, rightRows, OptimizedJoinTypeInner)
		}
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func processInnerJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	// Use hash join optimization for large datasets
	if len(leftRows) > 500 || len(rightRows) > 500 {
		optimizer := &HashJoinOptimizer{env: env}
		return optimizer.ProcessOptimizedJoin(leftRows, rightRows, onCondition, OptimizedJoinTypeInner)
	}

	// Fall back to original nested loop for small datasets
	joined := make([]Row, 0, len(leftRows)*len(rightRows)/4) // Estimate result size
	for _, l := range leftRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		for _, r := range rightRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
			}
		}
	}
	return joined, nil
}

func processLeftJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr, rightAlias string, rightTable *storage.Table) ([]Row, error) {
	// Use hash join optimization for large datasets
	if len(leftRows) > 500 || len(rightRows) > 500 {
		optimizer := &HashJoinOptimizer{env: env}
		result, err := optimizer.ProcessOptimizedJoin(leftRows, rightRows, onCondition, OptimizedJoinTypeLeft)
		if err != nil {
			return nil, err
		}

		// Add right nulls for unmatched rows (hash join might not handle all cases)
		for _, row := range result {
			addRightNulls(row, rightAlias, rightTable)
		}
		return result, nil
	}

	// Fall back to original nested loop for small datasets
	joined := make([]Row, 0, len(leftRows)) // At least one row per left row
	for _, l := range leftRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		matched := false
		for _, r := range rightRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
				matched = true
			}
		}
		if !matched {
			m := cloneRow(l)
			addRightNulls(m, rightAlias, rightTable)
			joined = append(joined, m)
		}
	}
	return joined, nil
}

func processRightJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	joined := make([]Row, 0, len(rightRows)) // At least one row per right row
	var leftKeys []string
	if len(leftRows) > 0 {
		leftKeys = keysOfRow(leftRows[0])
	}
	for _, r := range rightRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		matched := false
		for _, l := range leftRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
				matched = true
			}
		}
		if !matched {
			m := cloneRow(r)
			for _, k := range leftKeys {
				m[k] = nil
			}
			joined = append(joined, m)
		}
	}
	return joined, nil
}

// processFullOuterJoin combines every left row (matched or, like LEFT JOIN,
// paired with right-side NULLs when unmatched) with every right row that
// never matched any left row (paired with left-side NULLs, like RIGHT
// JOIN's unmatched case). This was previously entirely unimplemented: FULL
// and CROSS were not lexer keywords, so "FULL OUTER JOIN" silently
// mis-parsed as a table aliased "FULL" with the rest of the clause dropped
// — a query that looked like a two-table join silently ran as a one-table
// scan with no error.
func processFullOuterJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr, rightAlias string, rightTable *storage.Table) ([]Row, error) {
	matchedRight := make([]bool, len(rightRows))
	joined := make([]Row, 0, len(leftRows)+len(rightRows))

	var leftKeys []string
	if len(leftRows) > 0 {
		leftKeys = keysOfRow(leftRows[0])
	}

	for _, l := range leftRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		matchedAny := false
		for ri, r := range rightRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
				matchedAny = true
				matchedRight[ri] = true
			}
		}
		if !matchedAny {
			m := cloneRow(l)
			addRightNulls(m, rightAlias, rightTable)
			joined = append(joined, m)
		}
	}

	for ri, r := range rightRows {
		if matchedRight[ri] {
			continue
		}
		m := cloneRow(r)
		for _, k := range leftKeys {
			m[k] = nil
		}
		joined = append(joined, m)
	}
	return joined, nil
}

func applyWhereClause(env ExecEnv, where Expr, rows []Row) ([]Row, error) {
	if where == nil {
		return rows, nil
	}
	filtered := make([]Row, 0, len(rows)/2) // Estimate half will match
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, where, r)
		if err != nil {
			return nil, err
		}
		if toTri(v) == tvTrue {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func processGroupByHaving(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	if s.Pivot != nil {
		pivotRows, err := processPivot(env, s.Pivot, filtered)
		if err != nil {
			return nil, nil, err
		}
		// Pivoted rows are a plain row set keyed by group columns + one key
		// per pivot value; run them through the normal (non-aggregate)
		// projection path so SELECT * / explicit column lists / window
		// functions work exactly as they would on any other row set.
		return processNonAggregateQuery(env, s, pivotRows)
	}

	needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)

	if needAgg {
		return processAggregateQuery(env, s, filtered)
	}
	return processNonAggregateQuery(env, s, filtered)
}

// processPivot reshapes filtered rows per a PIVOT clause: every column not
// used as the pivot column or the aggregated value column becomes an
// implicit GROUP BY key, and each literal in the PIVOT's IN (...) list
// becomes its own output column holding agg(value_expr) over the rows in
// that group matching that pivot value.
func processPivot(env ExecEnv, pv *PivotClause, filtered []Row) ([]Row, error) {
	pivotColLower := strings.ToLower(pv.PivotCol)
	exclude := map[string]bool{pivotColLower: true}
	for name := range collectVarRefNames(pv.ValueExpr) {
		exclude[name] = true
	}

	// Group-by columns: every unqualified column present in the source rows
	// except the pivot column and the value expression's own column(s).
	var groupCols []string
	if len(filtered) > 0 {
		seen := map[string]bool{}
		for k := range filtered[0] {
			if strings.Contains(k, ".") || exclude[k] || seen[k] {
				continue
			}
			seen[k] = true
			groupCols = append(groupCols, k)
		}
		sort.Strings(groupCols)
	}

	// Evaluate each IN (...) entry once (they must be constant expressions)
	// and determine its output column name.
	type pivotOut struct {
		key  any
		name string
	}
	outs := make([]pivotOut, len(pv.Values))
	for i, v := range pv.Values {
		val, err := evalExpr(env, v.Expr, Row{})
		if err != nil {
			return nil, fmt.Errorf("PIVOT: evaluating IN-list value: %w", err)
		}
		name := v.Alias
		if name == "" {
			name = fmt.Sprint(val)
		}
		outs[i] = pivotOut{key: val, name: name}
	}

	// Group source rows by their group-by column values, preserving first-
	// seen order (matches GROUP BY's existing behavior elsewhere).
	type group struct {
		values []any
		rows   []Row
	}
	groups := make(map[string]*group)
	var order []string
	var keyBuf strings.Builder
	for _, r := range filtered {
		keyBuf.Reset()
		values := make([]any, len(groupCols))
		for i, c := range groupCols {
			v := r[c]
			values[i] = v
			if i > 0 {
				keyBuf.WriteByte('\x1f')
			}
			writeFmtKeyPart(&keyBuf, v)
		}
		gk := keyBuf.String()
		g, ok := groups[gk]
		if !ok {
			g = &group{values: values}
			groups[gk] = g
			order = append(order, gk)
		}
		g.rows = append(g.rows, r)
	}

	outRows := make([]Row, 0, len(order))
	for _, gk := range order {
		g := groups[gk]
		out := Row{}
		for i, c := range groupCols {
			putVal(out, c, g.values[i])
		}
		for _, o := range outs {
			var matching []Row
			for _, r := range g.rows {
				pcv, _ := getValLower(r, pivotColLower)
				cmp, err := compare(pcv, o.key)
				if err == nil && cmp == 0 {
					matching = append(matching, r)
				}
			}
			aggCall := &FuncCall{Name: pv.AggFunc, Args: []Expr{pv.ValueExpr}}
			val, err := evalAggregateFuncCall(env, aggCall, matching)
			if err != nil {
				return nil, fmt.Errorf("PIVOT: %s: %w", pv.AggFunc, err)
			}
			putVal(out, o.name, val)
		}
		outRows = append(outRows, out)
	}
	return outRows, nil
}

// collectVarRefNames returns the lowercased names of every column reference
// reachable within e — used by PIVOT to exclude the value expression's own
// column(s) from the implicit GROUP BY key set.
func collectVarRefNames(e Expr) map[string]bool {
	names := make(map[string]bool)
	var walk func(Expr)
	walk = func(e Expr) {
		switch ex := e.(type) {
		case nil:
		case *VarRef:
			key := ex.Lower
			if key == "" {
				key = strings.ToLower(ex.Name)
			}
			names[key] = true
		case *Unary:
			walk(ex.Expr)
		case *Binary:
			walk(ex.Left)
			walk(ex.Right)
		case *IsNull:
			walk(ex.Expr)
		case *FuncCall:
			for _, a := range ex.Args {
				walk(a)
			}
		case *CaseExpr:
			walk(ex.Operand)
			for _, w := range ex.Whens {
				walk(w.When)
				walk(w.Then)
			}
			walk(ex.Else)
		}
	}
	walk(e)
	return names
}

//nolint:gocyclo // Aggregation flow must cover grouping, HAVING, and projection variants.
func processAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	groups := make(map[string][]Row, len(filtered)/2) // Estimate group count
	orderKeys := make([]string, 0, len(filtered)/2)
	outRows := make([]Row, 0, len(filtered)/2)
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))

	// Build the composite group key directly into a reused builder instead of
	// collecting per-column strings into a slice and strings.Join-ing them —
	// avoids one slice + one string allocation per group-by expression, per row.
	var keyBuf strings.Builder
	for _, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}
		keyBuf.Reset()
		for i, g := range s.GroupBy {
			v, err := evalExpr(env, g, r)
			if err != nil {
				return nil, nil, err
			}
			if i > 0 {
				keyBuf.WriteByte('\x1f')
			}
			writeFmtKeyPart(&keyBuf, v)
		}
		ks := keyBuf.String()
		if _, ok := groups[ks]; !ok {
			orderKeys = append(orderKeys, ks)
		}
		groups[ks] = append(groups[ks], r)
	}

	// A whole-table aggregate (no GROUP BY) always produces exactly one row,
	// even over zero matching input rows — "SELECT COUNT(*) FROM t" on an
	// empty (or fully filtered-out) table must return one row with count 0,
	// not zero rows. Only synthesize that implicit empty group when there's
	// no GROUP BY at all; a real "GROUP BY x" correctly produces zero rows
	// when there's no data to group.
	if len(s.GroupBy) == 0 && len(orderKeys) == 0 {
		orderKeys = append(orderKeys, "")
		groups[""] = nil
	}

	for _, k := range orderKeys {
		rows := groups[k]
		if s.Having != nil {
			hv, err := evalAggregate(env, s.Having, rows)
			if err != nil {
				return nil, nil, err
			}
			if toTri(hv) != tvTrue {
				continue
			}
		}
		out := Row{}
		for i, it := range s.Projs {
			if it.Star {
				if len(rows) > 0 {
					for col, v := range rows[0] {
						putVal(out, col, v)
						if strings.Contains(col, ".") {
							last := strings.LastIndex(col, ".")
							base := col[last+1:]
							putVal(out, base, v)
							if _, seen := colSet[base]; !seen {
								colSet[base] = struct{}{}
								outCols = append(outCols, base)
							}
						} else {
							if _, seen := colSet[col]; !seen {
								colSet[col] = struct{}{}
								outCols = append(outCols, col)
							}
						}
					}
				}
				continue
			}
			name := projName(it, i)
			var val any
			var err error
			if isAggregate(it.Expr) || len(s.GroupBy) > 0 {
				val, err = evalAggregate(env, it.Expr, rows)
			} else if len(rows) > 0 {
				val, err = evalExpr(env, it.Expr, rows[0])
			} else {
				// The implicit empty group for a whole-table aggregate over
				// zero rows (see above): a non-aggregate projection has no
				// row to evaluate against. A literal still evaluates fine;
				// a real column reference will error instead of panicking
				// on rows[0], which is the right failure mode here since
				// such a reference is not meaningfully defined without a
				// GROUP BY or a row to pull it from.
				val, err = evalExpr(env, it.Expr, Row{})
			}
			if err != nil {
				return nil, nil, err
			}
			putVal(out, name, val)
			if _, seen := colSet[name]; !seen {
				colSet[name] = struct{}{}
				outCols = append(outCols, name)
			}
		}
		outRows = append(outRows, out)
	}
	return outRows, outCols, nil
}

func processNonAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	outRows := make([]Row, 0, len(filtered))
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))

	// Check if any window functions are used
	hasWindowFunctions := anyWindowInSelect(s.Projs)

	// If window functions are present, set up window context
	if hasWindowFunctions {
		env.windowRows = filtered
	}

	for rowIdx, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}

		// Set window index for current row
		if hasWindowFunctions {
			env.windowIndex = rowIdx
		}

		out := Row{}
		for i, it := range s.Projs {
			if it.Star {
				for col, v := range r {
					putVal(out, col, v)
					if strings.Contains(col, ".") {
						last := strings.LastIndex(col, ".")
						base := col[last+1:]
						putVal(out, base, v)
						if _, seen := colSet[base]; !seen {
							colSet[base] = struct{}{}
							outCols = append(outCols, base)
						}
					} else {
						if _, seen := colSet[col]; !seen {
							colSet[col] = struct{}{}
							outCols = append(outCols, col)
						}
					}
				}
				continue
			}
			val, err := evalExpr(env, it.Expr, r)
			if err != nil {
				return nil, nil, err
			}
			name := projName(it, i)
			putVal(out, name, val)
			if _, seen := colSet[name]; !seen {
				colSet[name] = struct{}{}
				outCols = append(outCols, name)
			}
		}
		outRows = append(outRows, out)
	}
	return outRows, outCols, nil
}

func applyOffsetLimit(s *Select, rows []Row) []Row {
	start := 0
	if s.Offset != nil && *s.Offset > 0 {
		start = *s.Offset
	}
	if start > len(rows) {
		return []Row{}
	}
	rows = rows[start:]

	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}
	return rows
}

// processCTEs extracts and evaluates CTEs (including simple recursive CTEs)
// and returns an ExecEnv with any CTE results bound.
func processCTEs(env ExecEnv, s *Select) (ExecEnv, error) {
	cteEnv := env
	if len(s.CTEs) == 0 {
		return cteEnv, nil
	}

	cteEnv = ExecEnv{
		ctx:    env.ctx,
		db:     env.db,
		tenant: env.tenant,
		ctes:   make(map[string]*ResultSet),
	}

	for _, cte := range s.CTEs {
		if !cte.Recursive {
			rs, err := evalNonRecursiveCTE(cteEnv, &cte)
			if err != nil {
				return env, err
			}
			cteEnv.ctes[cte.Name] = rs
			continue
		}

		rs, err := evalRecursiveCTE(cteEnv, &cte)
		if err != nil {
			return env, err
		}
		cteEnv.ctes[cte.Name] = rs
	}

	return cteEnv, nil
}

// evalNonRecursiveCTE evaluates a simple (non-recursive) CTE and returns its ResultSet.
func evalNonRecursiveCTE(env ExecEnv, cte *CTE) (*ResultSet, error) {
	if cte.Select == nil {
		return nil, fmt.Errorf("CTE %s: missing select", cte.Name)
	}
	rs, err := executeSelect(env, cte.Select)
	if err != nil {
		return nil, fmt.Errorf("CTE %s: %v", cte.Name, err)
	}
	return rs, nil
}

// evalRecursiveCTE evaluates a recursive CTE (WITH RECURSIVE) by executing the
// anchor and iteratively applying the recursive part until stabilization or limit.
// alignRecursiveCTERows aligns the rows from recursive part to match anchor columns
func alignRecursiveCTERows(accRs *ResultSet, nextRs *ResultSet, cteName string) []Row {
	// If columns don't match or aren't available, return as-is
	if accRs == nil || accRs.Cols == nil || nextRs == nil || nextRs.Cols == nil || len(nextRs.Cols) != len(accRs.Cols) {
		return nextRs.Rows
	}

	alignedRows := make([]Row, 0, len(nextRs.Rows))
	for _, r := range nextRs.Rows {
		nr := make(Row)
		for i := range accRs.Cols {
			src := nextRs.Cols[i]
			var val any
			if v, ok := r[strings.ToLower(src)]; ok {
				val = v
			} else if v, ok := r[src]; ok {
				val = v
			}
			tgt := strings.ToLower(accRs.Cols[i])
			nr[tgt] = val
			nr[cteName+"."+tgt] = val
		}
		alignedRows = append(alignedRows, nr)
	}
	return alignedRows
}

// addNewRowsToRecursiveCTE adds new rows to accumulator, tracking duplicates
func addNewRowsToRecursiveCTE(accRows []Row, newRows []Row, targetCols []string, seen map[string]bool) ([]Row, int) {
	newAdded := 0
	for _, r := range newRows {
		sig := rowSignature(r, targetCols)
		if !seen[sig] {
			seen[sig] = true
			accRows = append(accRows, r)
			newAdded++
		}
	}
	return accRows, newAdded
}

func evalRecursiveCTE(env ExecEnv, cte *CTE) (*ResultSet, error) {
	if cte.Select == nil || cte.Select.Union == nil {
		return nil, fmt.Errorf("recursive CTE %s must be a UNION of anchor and recursive part", cte.Name)
	}

	anchor := *cte.Select
	anchor.Union = nil

	recursiveSel := cte.Select.Union.Right
	if recursiveSel == nil {
		return nil, fmt.Errorf("recursive CTE %s missing recursive part", cte.Name)
	}

	accRs, err := executeSelect(env, &anchor)
	if err != nil {
		return nil, fmt.Errorf("CTE %s anchor: %v", cte.Name, err)
	}

	seen := make(map[string]bool)
	var accRows []Row
	if accRs != nil {
		accRows = append(accRows, accRs.Rows...)
		if accRs.Cols != nil {
			for _, r := range accRs.Rows {
				seen[rowSignature(r, accRs.Cols)] = true
			}
		}
	}

	iterLimit := 1024
	for iter := 0; iter < iterLimit; iter++ {
		colsForBind := []string{}
		if accRs != nil && accRs.Cols != nil {
			colsForBind = accRs.Cols
		}
		env.ctes[cte.Name] = &ResultSet{Cols: colsForBind, Rows: accRows}

		nextRs, err := executeSelect(env, recursiveSel)
		if err != nil {
			return nil, fmt.Errorf("CTE %s recursive eval: %v", cte.Name, err)
		}
		if nextRs == nil || len(nextRs.Rows) == 0 {
			break
		}

		targetCols := nextRs.Cols
		if accRs != nil && accRs.Cols != nil {
			targetCols = accRs.Cols
		}

		alignedRows := alignRecursiveCTERows(accRs, nextRs, cte.Name)

		var newAdded int
		accRows, newAdded = addNewRowsToRecursiveCTE(accRows, alignedRows, targetCols, seen)

		if accRs == nil && nextRs != nil {
			accRs = &ResultSet{Cols: nextRs.Cols}
		}
		if newAdded == 0 {
			break
		}
	}

	finalCols := []string{}
	if accRs != nil && accRs.Cols != nil {
		finalCols = accRs.Cols
	}
	return &ResultSet{Cols: finalCols, Rows: accRows}, nil
}

// -------------------- Eval, Aggregates, Helpers --------------------

func getVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }
func getValLower(row Row, lowerName string) (any, bool) {
	v, ok := row[lowerName]
	return v, ok
}
func putVal(row Row, key string, val any) { row[strings.ToLower(key)] = val }
func isNull(v any) bool                   { return v == nil }
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
func coerceToFloat(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to FLOAT", x)
		}
		return f, nil
	case bool:
		if x {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to FLOAT", v)
	}
}
func isStringValue(v any) bool {
	_, ok := v.(string)
	return ok
}
func stringifySQLValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

// tri-state
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v any) int {
	if v == nil {
		return tvUnknown
	}
	if truthy(v) {
		return tvTrue
	}
	return tvFalse
}
func triNot(t int) int {
	if t == tvTrue {
		return tvFalse
	}
	if t == tvFalse {
		return tvTrue
	}
	return tvUnknown
}
func triAnd(a, b int) int {
	if a == tvFalse || b == tvFalse {
		return tvFalse
	}
	if a == tvTrue && b == tvTrue {
		return tvTrue
	}
	return tvUnknown
}
func triOr(a, b int) int {
	if a == tvTrue || b == tvTrue {
		return tvTrue
	}
	if a == tvFalse && b == tvFalse {
		return tvFalse
	}
	return tvUnknown
}

func compare(a, b any) (int, error) {
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case *big.Rat:
		return compareBigRat(ax, b)
	case big.Rat:
		return compareBigRat(&ax, b)
	case int:
		return compareInt(ax, b)
	case float64:
		return compareFloat(ax, b)
	case string:
		return compareString(ax, b)
	case bool:
		return compareBool(ax, b)
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable %T and %T", a, b)
}

func compareBigRat(ax *big.Rat, b any) (int, error) {
	// Try to convert b to big.Rat
	if bx, ok := storage.DecimalFromAny(b); ok {
		rb := new(big.Rat).Set(bx)
		return ax.Cmp(rb), nil
	}
	// If b is numeric (int/float), convert
	switch bx := b.(type) {
	case int:
		rb := new(big.Rat).SetInt64(int64(bx))
		return ax.Cmp(rb), nil
	case int64:
		rb := new(big.Rat).SetInt64(bx)
		return ax.Cmp(rb), nil
	case float64:
		rb := new(big.Rat).SetFloat64(bx)
		return ax.Cmp(rb), nil
	}
	return 0, fmt.Errorf("incomparable decimal and %T", b)
}

func compareInt(ax int, b any) (int, error) {
	// fast path: avoid float64 conversion for same-type comparisons.
	switch bv := b.(type) {
	case int:
		if ax < bv {
			return -1, nil
		}
		if ax > bv {
			return 1, nil
		}
		return 0, nil
	case int64:
		ai := int64(ax)
		if ai < bv {
			return -1, nil
		}
		if ai > bv {
			return 1, nil
		}
		return 0, nil
	}
	if f, ok := numeric(b); ok {
		af := float64(ax)
		if af < f {
			return -1, nil
		}
		if af > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable int and %T", b)
}

func compareFloat(ax float64, b any) (int, error) {
	if f, ok := numeric(b); ok {
		if ax < f {
			return -1, nil
		}
		if ax > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable float64 and %T", b)
}

func compareString(ax string, b any) (int, error) {
	if bs, ok := b.(string); ok {
		if ax < bs {
			return -1, nil
		}
		if ax > bs {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable string and %T", b)
}

func compareBool(ax bool, b any) (int, error) {
	if bb, ok := b.(bool); ok {
		if !ax && bb {
			return -1, nil
		}
		if ax && !bb {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable bool and %T", b)
}
func compareForOrder(a, b any, desc bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if desc {
			return -1
		}
		return 1
	}
	if b == nil {
		if desc {
			return 1
		}
		return -1
	}
	c, err := compare(a, b)
	if err != nil {
		return 0
	}
	return c
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func evalExpr(env ExecEnv, e Expr, row Row) (any, error) {
	// Context cancellation is checked at row-level loop boundaries
	// (applyWhereClause, processNonAggregateQuery, UPDATE/DELETE loops, etc.),
	// not per expression node. This avoids O(nodes_per_row) channel selects.
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		return evalVarRef(env, ex, row)
	case *IsNull:
		return evalIsNull(env, ex, row)
	case *Unary:
		return evalUnary(env, ex, row)
	case *Binary:
		return evalBinary(env, ex, row)
	case *FuncCall:
		return evalFuncCall(env, ex, row)
	case *InExpr:
		return evalIn(env, ex, row)
	case *LikeExpr:
		return evalLike(env, ex, row)
	case *RegexpExpr:
		return evalRegexpExpr(env, ex, row)
	case *BetweenExpr:
		return evalBetween(env, ex, row)
	case *ExistsExpr:
		return evalExistsExpr(env, ex)
	case *CaseExpr:
		return evalCaseExpr(env, ex, row)
	case *SubqueryExpr:
		return evalSubqueryExpr(env, ex)
	}
	return nil, fmt.Errorf("unknown expression")
}

// evalBetween evaluates "expr [NOT] BETWEEN lo AND hi" with a single
// evaluation of expr, using the same three-valued comparison semantics as
// the desugared AND/OR form.
func evalBetween(env ExecEnv, ex *BetweenExpr, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	lo, err := evalExpr(env, ex.Lo, row)
	if err != nil {
		return nil, err
	}
	hi, err := evalExpr(env, ex.Hi, row)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

// betweenResult combines the boundary comparisons exactly like the desugared
// forms: BETWEEN → (v >= lo AND v <= hi), NOT BETWEEN → (v < lo OR v > hi).
func betweenResult(v, lo, hi any, negate bool) (any, error) {
	if negate {
		lt, err := evalComparisonBinary("<", v, lo)
		if err != nil {
			return nil, err
		}
		gt, err := evalComparisonBinary(">", v, hi)
		if err != nil {
			return nil, err
		}
		return triToValue(triOr(toTri(lt), toTri(gt))), nil
	}
	ge, err := evalComparisonBinary(">=", v, lo)
	if err != nil {
		return nil, err
	}
	le, err := evalComparisonBinary("<=", v, hi)
	if err != nil {
		return nil, err
	}
	return triToValue(triAnd(toTri(ge), toTri(le))), nil
}

func evalVarRef(env ExecEnv, ex *VarRef, row Row) (any, error) {
	if ex.Lower != "" {
		if v, ok := getValLower(row, ex.Lower); ok {
			return v, nil
		}
	} else if v, ok := getVal(row, ex.Name); ok {
		return v, nil
	}
	// Trigger bodies reference NEW.col/OLD.col, which aren't part of the row
	// being built by the statement the trigger body itself is executing (an
	// INSERT into a different table, say) — env.triggerRow carries them
	// separately. See executeTrigger in triggers.go.
	if env.triggerRow != nil {
		lower := ex.Lower
		if lower == "" {
			lower = strings.ToLower(ex.Name)
		}
		if v, ok := getValLower(env.triggerRow, lower); ok {
			return v, nil
		}
	}
	if strings.Contains(ex.Name, ".") {
		return nil, fmt.Errorf("unknown column reference %q", ex.Name)
	}
	return nil, fmt.Errorf("unknown column %q", ex.Name)
}

func evalIsNull(env ExecEnv, ex *IsNull, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	is := isNull(v)
	if ex.Negate {
		return !is, nil
	}
	return is, nil
}

func evalIn(env ExecEnv, ex *InExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}

	// Check against each value in the list
	for _, valExpr := range ex.Values {
		listVal, err := evalExpr(env, valExpr, row)
		if err != nil {
			return nil, err
		}

		// Compare values
		cmp, err := compare(val, listVal)
		if err == nil && cmp == 0 {
			// Found a match
			if ex.Negate {
				return false, nil
			}
			return true, nil
		}
	}

	// No match found
	if ex.Negate {
		return true, nil
	}
	return false, nil
}

func evalLike(env ExecEnv, ex *LikeExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}

	patternVal, err := evalExpr(env, ex.Pattern, row)
	if err != nil {
		return nil, err
	}

	// SQL semantics: NULL LIKE ... and ... LIKE NULL are not matches
	// (previously nil was stringified to "<nil>" and could match '%').
	if val == nil || patternVal == nil {
		return false, nil
	}

	// Convert to strings
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	pattern, ok := patternVal.(string)
	if !ok {
		pattern = fmt.Sprintf("%v", patternVal)
	}

	var matched bool
	if ex.GlobStyle {
		// GLOB: case-sensitive, * matches any sequence, ? matches one char
		if ex.CaseInsensitive {
			matched = matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern))
		} else {
			matched = matchGlobPattern(str, pattern)
		}
	} else {
		// LIKE / ILIKE: get optional escape character
		escapeChar := '\\'
		if ex.Escape != nil {
			escapeVal, err := evalExpr(env, ex.Escape, row)
			if err != nil {
				return nil, err
			}
			escapeStr, ok := escapeVal.(string)
			if !ok || len(escapeStr) != 1 {
				return nil, fmt.Errorf("ESCAPE must be a single character")
			}
			escapeChar = rune(escapeStr[0])
		}
		if ex.CaseInsensitive {
			matched = matchLikePattern(strings.ToLower(str), strings.ToLower(pattern), escapeChar)
		} else {
			matched = matchLikePattern(str, pattern, escapeChar)
		}
	}

	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalRegexpExpr evaluates REGEXP / RLIKE / SIMILAR TO predicates.
func evalRegexpExpr(env ExecEnv, ex *RegexpExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	patternVal, err := evalExpr(env, ex.Pattern, row)
	if err != nil {
		return nil, err
	}
	if val == nil || patternVal == nil {
		return false, nil
	}
	str := fmt.Sprintf("%v", val)
	pattern := fmt.Sprintf("%v", patternVal)
	if ex.SimilarTo {
		pattern = similarToRegexp(pattern)
	}
	re, err := compileCachedRegexp(pattern)
	if err != nil {
		return nil, fmt.Errorf("REGEXP: invalid pattern %q: %v", pattern, err)
	}
	matched := re.MatchString(str)
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalExistsExpr evaluates EXISTS (subquery).
func evalExistsExpr(env ExecEnv, ex *ExistsExpr) (any, error) {
	rs, err := executeSelect(env, ex.Select)
	if err != nil {
		return nil, err
	}
	return rs != nil && len(rs.Rows) > 0, nil
}

// matchLikePattern matches a string against a SQL LIKE pattern.
// % matches zero or more characters, _ matches exactly one character.
//
// The matcher is rune-aware: _ consumes one Unicode code point, not one byte,
// so multi-byte characters (é, 日, …) match _ correctly. Wildcard backtracking
// uses the classic two-pointer greedy algorithm (O(len(str)*len(pattern))
// worst case, linear for typical patterns, zero allocations).
func matchLikePattern(str, pattern string, escape rune) bool {
	sIdx, pIdx := 0, 0
	sLen, pLen := len(str), len(pattern)
	star, match := -1, 0

	for sIdx < sLen {
		if pIdx < pLen {
			pChar, pw := utf8.DecodeRuneInString(pattern[pIdx:])

			switch {
			case pChar == escape && pIdx+pw < pLen:
				// Escaped character matches literally.
				lChar, lw := utf8.DecodeRuneInString(pattern[pIdx+pw:])
				sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
				if sChar == lChar {
					sIdx += sw
					pIdx += pw + lw
					continue
				}
				// Mismatch: fall through to % backtracking (a bare
				// "return false" here would wrongly reject e.g.
				// 'a_b' LIKE '%\_%' at the first position).
			case pChar == '%':
				star = pIdx
				match = sIdx
				pIdx += pw
				continue
			default:
				sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
				if pChar == '_' || sChar == pChar {
					sIdx += sw
					pIdx += pw
					continue
				}
			}
		}

		// No match, backtrack to last % and consume one more source rune.
		if star != -1 {
			pIdx = star + 1 // '%' is a single byte
			_, mw := utf8.DecodeRuneInString(str[match:])
			match += mw
			sIdx = match
			continue
		}
		return false
	}

	// Consume remaining % in pattern
	for pIdx < pLen && pattern[pIdx] == '%' {
		pIdx++
	}

	return pIdx == pLen
}

// matchGlobPattern matches a string against a GLOB pattern.
// * matches zero or more characters, ? matches exactly one character (one
// Unicode code point). Unlike LIKE, GLOB is case-sensitive by default
// (callers may lowercase both strings for case-insensitive behaviour).
func matchGlobPattern(str, pattern string) bool {
	sIdx, pIdx := 0, 0
	sLen, pLen := len(str), len(pattern)
	star, match := -1, 0

	for sIdx < sLen {
		if pIdx < pLen {
			pChar, pw := utf8.DecodeRuneInString(pattern[pIdx:])
			if pChar == '*' {
				star = pIdx
				match = sIdx
				pIdx += pw
				continue
			}
			sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
			if pChar == '?' || sChar == pChar {
				sIdx += sw
				pIdx += pw
				continue
			}
		}
		if star != -1 {
			pIdx = star + 1 // '*' is a single byte
			_, mw := utf8.DecodeRuneInString(str[match:])
			match += mw
			sIdx = match
			continue
		}
		return false
	}
	for pIdx < pLen && pattern[pIdx] == '*' {
		pIdx++
	}
	return pIdx == pLen
}

// similarToRegexp converts a SQL SIMILAR TO pattern to a Go regexp pattern.
// Rules:
//   - % matches any sequence of characters (like .* in regex)
//   - _ matches any single character (like . in regex)
//   - | * + ? ( ) [ ] { } \ work as standard regex metacharacters
//   - The match is anchored (whole string must match)
func similarToRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '|', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\':
			// Standard regex metacharacters – pass through as-is
			b.WriteByte(c)
		case '.', '^', '$':
			// Anchor/any-char metacharacters in regex that are literal in SIMILAR TO
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString("$")
	return b.String()
}

func evalCaseExpr(env ExecEnv, ex *CaseExpr, row Row) (any, error) {
	if ex.Operand != nil {
		target, err := evalExpr(env, ex.Operand, row)
		if err != nil {
			return nil, err
		}
		for _, w := range ex.Whens {
			whenVal, err := evalExpr(env, w.When, row)
			if err != nil {
				return nil, err
			}
			if cmp, err := compare(target, whenVal); err == nil && cmp == 0 {
				return evalExpr(env, w.Then, row)
			}
		}
	} else {
		for _, w := range ex.Whens {
			cond, err := evalExpr(env, w.When, row)
			if err != nil {
				return nil, err
			}
			if toTri(cond) == tvTrue {
				return evalExpr(env, w.Then, row)
			}
		}
	}
	if ex.Else != nil {
		return evalExpr(env, ex.Else, row)
	}
	return nil, nil
}

func evalSubqueryExpr(env ExecEnv, ex *SubqueryExpr) (any, error) {
	rs, err := executeSelect(env, ex.Select)
	if err != nil {
		return nil, err
	}
	if rs == nil || len(rs.Rows) == 0 {
		return nil, nil
	}
	if len(rs.Rows) > 1 {
		return nil, fmt.Errorf("scalar subquery returned %d rows", len(rs.Rows))
	}
	row := rs.Rows[0]
	if len(rs.Cols) == 1 {
		if v, ok := getValLower(row, strings.ToLower(rs.Cols[0])); ok {
			return v, nil
		}
	}
	if len(row) == 1 {
		for _, v := range row {
			return v, nil
		}
	}
	for _, col := range rs.Cols {
		if v, ok := getValLower(row, strings.ToLower(col)); ok {
			return v, nil
		}
	}
	for _, v := range row {
		return v, nil
	}
	return nil, nil
}

func evalUnary(env ExecEnv, ex *Unary, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			return new(big.Rat).Set(r), nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			neg := new(big.Rat).Set(r)
			neg.Mul(neg, big.NewRat(-1, 1))
			return neg, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		return evalLogicalBinary(env, ex, row)
	}

	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}

	switch ex.Op {
	case "+", "-", "*", "/":
		return evalArithmeticBinary(ex.Op, lv, rv)
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}

func evalLogicalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" && toTri(lv) == tvFalse {
		return false, nil
	}
	if ex.Op == "OR" && toTri(lv) == tvTrue {
		return true, nil
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" {
		return triToValue(triAnd(toTri(lv), toTri(rv))), nil
	}
	return triToValue(triOr(toTri(lv), toTri(rv))), nil
}

func evalArithmeticBinary(op string, lv, rv any) (any, error) {
	if op == "+" {
		if isStringValue(lv) || isStringValue(rv) {
			return stringifySQLValue(lv) + stringifySQLValue(rv), nil
		}
		if lv == nil || rv == nil {
			return nil, nil
		}
	} else if lv == nil || rv == nil {
		return nil, nil
	}
	// If either operand is a decimal (big.Rat), perform high-precision arithmetic
	// Only treat values as decimals for high-precision arithmetic when they
	// are already rational types (i.e. *big.Rat or big.Rat). This preserves
	// existing numeric semantics for plain ints/floats.
	if la, lok := storage.AsBigRat(lv); lok {
		if rb, rok := storage.AsBigRat(rv); rok {
			a := new(big.Rat).Set(la)
			b := new(big.Rat).Set(rb)
			switch op {
			case "+":
				return new(big.Rat).Add(a, b), nil
			case "-":
				return new(big.Rat).Sub(a, b), nil
			case "*":
				return new(big.Rat).Mul(a, b), nil
			case "/":
				if b.Sign() == 0 {
					return nil, errors.New("division by zero")
				}
				return new(big.Rat).Quo(a, b), nil
			}
		}
		return nil, fmt.Errorf("%s expects numeric", op)
	}

	lf, lok := numeric(lv)
	rf, rok := numeric(rv)
	if !(lok && rok) {
		return nil, fmt.Errorf("%s expects numeric", op)
	}
	switch op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, errors.New("division by zero")
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
}

func evalComparisonBinary(op string, lv, rv any) (any, error) {
	if lv == nil || rv == nil {
		return nil, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	}
	return nil, fmt.Errorf("unknown comparison operator: %s", op)
}

type funcHandler func(env ExecEnv, ex *FuncCall, row Row) (any, error)

func getBuiltinFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"ROW_TO_TEXT":       evalRowToTextFunc,
		"COALESCE":          evalCoalesceFunc,
		"NVL":               evalCoalesceFunc,
		"IFNULL":            evalCoalesceFunc,
		"NULLIF":            evalNullifFunc,
		"ISNULL":            evalIsNullFuncWrapper,
		"JSON_GET":          evalJSONGetFunc,
		"JSON_SET":          evalJSONExtendedFunc,
		"JSON_EXTRACT":      evalJSONExtendedFunc,
		"COUNT":             evalCountSingle,
		"SUM":               evalAggregateSingle,
		"AVG":               evalAggregateSingle,
		"MIN":               evalAggregateSingle,
		"MAX":               evalAggregateSingle,
		"NOW":               evalNowFunc,
		"GETDATE":           evalNowFunc,
		"CURRENT_TIME":      evalNowFunc,
		"CURRENT_TIMESTAMP": evalNowFunc,
		"CURRENT_DATE":      evalCurrentDateFunc,
		"TODAY":             evalTodayFunc,
		"FROM_TIMESTAMP":    evalFromTimestampFunc,
		"TIMESTAMP":         evalTimestampFunc,
		"DATEDIFF":          evalDateDiff,
		"LTRIM":             evalLTrimFunc,
		"RTRIM":             evalRTrimFunc,
		"TRIM":              evalTrimFunc,
		"UPPER":             evalUpperFunc,
		"LOWER":             evalLowerFunc,
		"CONCAT":            evalConcatFunc,
		"CONCAT_WS":         evalConcatWsFunc,
		"LENGTH":            evalLengthFunc,
		"LEN":               evalLengthFunc,
		"SUBSTRING":         evalSubstringFunc,
		"SUBSTR":            evalSubstringFunc,
		"MD5":               evalMD5Func,
		"SHA1":              evalSHA1Func,
		"SHA256":            evalSHA256Func,
		"SHA512":            evalSHA512Func,
		"BASE64":            evalBase64Func,
		"BASE64_DECODE":     evalBase64DecodeFunc,
		"LEFT":              evalLeftFunc,
		"RIGHT":             evalRightFunc,
		"CAST":              evalCastFunc,
		"REPLACE":           evalReplaceFunc,
		"INSTR":             evalInstrFunc,
		"LOCATE":            evalInstrFunc,
		"POSITION":          evalPositionFunc,
		"ABS":               evalAbsFunc,
		"ROUND":             evalRoundFunc,
		"FLOOR":             evalFloorFunc,
		"CEIL":              evalCeilFunc,
		"CEILING":           evalCeilFunc,
		"REVERSE":           evalReverseFunc,
		"REPEAT":            evalRepeatFunc,
		"PRINTF":            evalPrintfFunc,
		"FORMAT":            evalPrintfFunc,
		"CHAR_LENGTH":       evalLengthFunc,
		"LPAD":              evalLpadFunc,
		"RPAD":              evalRpadFunc,
		"GREATEST":          evalGreatestFunc,
		"LEAST":             evalLeastFunc,
		"IF":                evalIfFunc,
		"IIF":               evalIfFunc,
		"STRFTIME":          evalStrftimeFunc,
		"DATE":              evalDateFunc,
		"TIME":              evalTimeFunc,
		"YEAR":              evalYearFunc,
		"MONTH":             evalMonthFunc,
		"DAY":               evalDayFunc,
		"HOUR":              evalHourFunc,
		"MINUTE":            evalMinuteFunc,
		"SECOND":            evalSecondFunc,
		"RANDOM":            evalRandomFunc,
		"RAND":              evalRandomFunc,
		// Math functions
		"MOD":      evalModFunc,
		"POWER":    evalPowerFunc,
		"POW":      evalPowerFunc,
		"SQRT":     evalSqrtFunc,
		"LOG":      evalLogFunc,
		"LN":       evalLnFunc,
		"LOG10":    evalLog10Func,
		"LOG2":     evalLog2Func,
		"EXP":      evalExpFunc,
		"SIGN":     evalSignFunc,
		"TRUNCATE": evalTruncateFunc,
		"TRUNC":    evalTruncateFunc,
		"PI":       evalPiFunc,
		"SIN":      evalSinFunc,
		"COS":      evalCosFunc,
		"TAN":      evalTanFunc,
		"ASIN":     evalAsinFunc,
		"ACOS":     evalAcosFunc,
		"ATAN":     evalAtanFunc,
		"ATAN2":    evalAtan2Func,
		"DEGREES":  evalDegreesFunc,
		"RADIANS":  evalRadiansFunc,
		// String functions
		"SPACE":      evalSpaceFunc,
		"ASCII":      evalAsciiFunc,
		"CHAR":       evalCharFunc,
		"CHR":        evalCharFunc,
		"INITCAP":    evalInitcapFunc,
		"SPLIT_PART": evalSplitPartFunc,
		"SOUNDEX":    evalSoundexFunc,
		"QUOTE":      evalQuoteFunc,
		"HEX":        evalHexFunc,
		"UNHEX":      evalUnhexFunc,
		// Additional functions
		"UUID":       evalUuidFunc,
		"TYPEOF":     evalTypeofFunc,
		"VERSION":    evalVersionFunc,
		"DAYOFWEEK":  evalDayOfWeekFunc,
		"DAYOFYEAR":  evalDayOfYearFunc,
		"WEEKOFYEAR": evalWeekOfYearFunc,
		"QUARTER":    evalQuarterFunc,
		"DATE_ADD":   evalDateAddFunc,
		"DATE_SUB":   evalDateSubFunc,
		"DATEADD":    evalDateAddFunc,
		"DATESUB":    evalDateSubFunc,
	}
}

func evalFuncCall(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	// Check if this is a window function call
	if ex.Over != nil {
		// Window functions need access to all rows, not just current row
		// This will be handled specially during SELECT execution
		// For now, return an error if accessed outside window context
		if env.windowRows == nil {
			return nil, fmt.Errorf("window function %s used outside window context", ex.Name)
		}
		return evalWindowFunction(env, ex, row)
	}

	builtinFunctions := getAllFunctions()
	if handler, ok := builtinFunctions[ex.Name]; ok {
		return handler(env, ex, row)
	}
	if handler, ok := builtinFunctions[strings.ToUpper(ex.Name)]; ok {
		return handler(env, ex, row)
	}
	return nil, fmt.Errorf("unknown function: %s", ex.Name)
}

// Wrapper functions to match funcHandler signature
func evalCoalesceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCoalesce(env, ex.Args, row)
}
func evalNullifFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalNullif(env, ex.Args, row)
}
func evalIsNullFuncWrapper(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalIsNullFunc(env, ex.Args, row)
}
func evalJSONGetFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalJSONGet(env, ex.Args, row)
}
func evalJSONExtendedFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalJSONExtended(env, ex, row)
}
func evalNowFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return time.Now(), nil
}

// evalRowToTextFunc implements ROW_TO_TEXT() — concatenates every column
// value of the current row into one space-separated string, for ad-hoc
// whole-row substring search without setting up FTS, e.g.:
//
//	SELECT * FROM orders WHERE ROW_TO_TEXT() LIKE '%acme corp%' AND status = 'open'
//
// This reads the ambient Row directly rather than evaluating ex.Args, so it
// must never run through the raw fast path (which pre-evaluates args and
// substitutes an empty Row) — see the *FuncCall case in isSimpleRawExpr.
func evalRowToTextFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) > 0 {
		return nil, fmt.Errorf("ROW_TO_TEXT expects no arguments")
	}
	// Row maps store each column under both its qualified ("t.col") and
	// unqualified ("col") key pointing at the same value; skip qualified
	// keys so each value is included exactly once.
	keys := make([]string, 0, len(row))
	for k := range row {
		if !strings.Contains(k, ".") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		v := row[k]
		if v == nil {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%v", v)
	}
	return sb.String(), nil
}
func evalCurrentDateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
}
func evalTodayFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
}
func evalFromTimestampFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("FROM_TIMESTAMP expects 1 argument")
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	floatAny, err := coerceToFloat(val)
	if err != nil {
		return nil, fmt.Errorf("FROM_TIMESTAMP expects numeric or string input: %w", err)
	}
	seconds, ok := floatAny.(float64)
	if !ok {
		return nil, fmt.Errorf("FROM_TIMESTAMP expected float result, got %T", floatAny)
	}
	sec, frac := math.Modf(seconds)
	nsec := int64(math.Round(frac * 1e9))
	return time.Unix(int64(sec), nsec), nil
}
func evalTimestampFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP expects 1 argument")
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if floatAny, err := coerceToFloat(val); err == nil {
		if seconds, ok := floatAny.(float64); ok {
			return int64(seconds), nil
		}
	}
	t, err := parseTimeValue(val)
	if err != nil {
		return nil, fmt.Errorf("TIMESTAMP: %v", err)
	}
	return t.Unix(), nil
}
func evalLTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLTrim(env, ex.Args, row)
}
func evalRTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRTrim(env, ex.Args, row)
}
func evalTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTrim(env, ex.Args, row)
}
func evalUpperFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUpper(env, ex.Args, row)
}
func evalLowerFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLower(env, ex.Args, row)
}
func evalConcatFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalConcat(env, ex.Args, row)
}
func evalLengthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLength(env, ex.Args, row)
}
func evalSubstringFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSubstring(env, ex.Args, row)
}
func evalMD5Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMD5(env, ex.Args, row)
}
func evalSHA1Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA1(env, ex.Args, row)
}
func evalSHA256Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA256(env, ex.Args, row)
}
func evalSHA512Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA512(env, ex.Args, row)
}
func evalBase64Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalBase64(env, ex.Args, row)
}
func evalBase64DecodeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalBase64Decode(env, ex.Args, row)
}
func evalLeftFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLeft(env, ex.Args, row)
}
func evalRightFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRight(env, ex.Args, row)
}
func evalCastFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCast(env, ex.Args, row)
}
func evalReplaceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalReplace(env, ex.Args, row)
}
func evalInstrFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalInstr(env, ex.Args, row)
}
func evalAbsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAbs(env, ex.Args, row)
}
func evalRoundFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRound(env, ex.Args, row)
}
func evalFloorFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalFloor(env, ex.Args, row)
}
func evalCeilFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCeil(env, ex.Args, row)
}
func evalReverseFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalReverse(env, ex.Args, row)
}
func evalRepeatFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRepeat(env, ex.Args, row)
}
func evalPrintfFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPrintf(env, ex.Args, row)
}
func evalLpadFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLpad(env, ex.Args, row)
}
func evalRpadFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRpad(env, ex.Args, row)
}
func evalGreatestFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalGreatest(env, ex.Args, row)
}
func evalLeastFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLeast(env, ex.Args, row)
}
func evalIfFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalIf(env, ex.Args, row)
}
func evalStrftimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalStrftime(env, ex.Args, row)
}
func evalDateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDate(env, ex.Args, row)
}
func evalTimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTime(env, ex.Args, row)
}
func evalYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalYear(env, ex.Args, row)
}
func evalMonthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMonth(env, ex.Args, row)
}
func evalDayFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDay(env, ex.Args, row)
}
func evalHourFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalHour(env, ex.Args, row)
}
func evalMinuteFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMinute(env, ex.Args, row)
}
func evalSecondFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSecond(env, ex.Args, row)
}
func evalRandomFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRandom(env, ex.Args, row)
}
func evalModFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMod(env, ex.Args, row)
}
func evalPowerFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPower(env, ex.Args, row)
}
func evalSqrtFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSqrt(env, ex.Args, row)
}
func evalLogFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog(env, ex.Args, row)
}
func evalLnFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLn(env, ex.Args, row)
}
func evalLog10Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog10(env, ex.Args, row)
}
func evalLog2Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog2(env, ex.Args, row)
}
func evalExpFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalExp(env, ex.Args, row)
}
func evalSignFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSign(env, ex.Args, row)
}
func evalTruncateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTruncate(env, ex.Args, row)
}
func evalPiFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return math.Pi, nil
}
func evalSinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSin(env, ex.Args, row)
}
func evalCosFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCos(env, ex.Args, row)
}
func evalTanFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTan(env, ex.Args, row)
}
func evalAsinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAsin(env, ex.Args, row)
}
func evalAcosFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAcos(env, ex.Args, row)
}
func evalAtanFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAtan(env, ex.Args, row)
}
func evalAtan2Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAtan2(env, ex.Args, row)
}
func evalDegreesFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDegrees(env, ex.Args, row)
}
func evalRadiansFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRadians(env, ex.Args, row)
}
func evalSpaceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSpace(env, ex.Args, row)
}
func evalAsciiFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAscii(env, ex.Args, row)
}
func evalCharFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalChar(env, ex.Args, row)
}
func evalInitcapFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalInitcap(env, ex.Args, row)
}
func evalSplitPartFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSplitPart(env, ex.Args, row)
}
func evalSoundexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSoundex(env, ex.Args, row)
}
func evalQuoteFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalQuote(env, ex.Args, row)
}
func evalHexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalHex(env, ex.Args, row)
}
func evalUnhexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUnhex(env, ex.Args, row)
}
func evalUuidFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUuid(env, ex.Args, row)
}
func evalTypeofFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTypeof(env, ex.Args, row)
}
func evalVersionFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return "tinySQL 1.0", nil
}
func evalConcatWsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalConcatWs(env, ex.Args, row)
}
func evalPositionFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPosition(env, ex.Args, row)
}
func evalDayOfWeekFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDayOfWeek(env, ex.Args, row)
}
func evalDayOfYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDayOfYear(env, ex.Args, row)
}
func evalWeekOfYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalWeekOfYear(env, ex.Args, row)
}
func evalQuarterFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalQuarter(env, ex.Args, row)
}
func evalDateAddFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateAdd(env, ex.Args, row)
}
func evalDateSubFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateSub(env, ex.Args, row)
}

func evalCoalesce(env ExecEnv, args []Expr, row Row) (any, error) {
	for _, a := range args {
		v, err := evalExpr(env, a, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}

func evalNullif(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF expects 2 args")
	}
	lv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return nil, nil
	}
	if rv == nil {
		return lv, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	if cmp == 0 {
		return nil, nil
	}
	return lv, nil
}

func evalJSONGet(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_GET expects (json, path)")
	}
	jv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	pv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	ps, _ := pv.(string)
	return jsonGet(jv, ps), nil
}

func evalJSONExtended(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch ex.Name {
	case "JSON_SET":
		if len(ex.Args) != 3 {
			return nil, fmt.Errorf("JSON_SET expects (json, path, value)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		val, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonSet(jv, ps, val), nil

	case "JSON_EXTRACT":
		// Alias for JSON_GET
		if len(ex.Args) != 2 {
			return nil, fmt.Errorf("JSON_EXTRACT expects (json, path)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonGet(jv, ps), nil
	}
	return nil, fmt.Errorf("unknown JSON function: %s", ex.Name)
}

func evalCountSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Star {
		return 1, nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return 1, nil
}

func evalAggregateSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func evalDateDiff(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 3 {
		return nil, fmt.Errorf("DATEDIFF expects 3 arguments: (unit, start_date, end_date)")
	}

	// Get the unit (HOURS, DAYS, MINUTES, etc.)
	unitVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	unit, ok := unitVal.(string)
	if !ok {
		return nil, fmt.Errorf("DATEDIFF unit must be a string")
	}

	// Get start date
	startVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}

	// Get end date
	endVal, err := evalExpr(env, ex.Args[2], row)
	if err != nil {
		return nil, err
	}

	// Convert values to time.Time
	startTime, err := parseTimeValue(startVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF start_date: %v", err)
	}

	endTime, err := parseTimeValue(endVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF end_date: %v", err)
	}

	// Calculate difference
	diff := endTime.Sub(startTime)

	// Return based on unit
	switch strings.ToUpper(unit) {
	case "HOURS":
		return int(diff.Hours()), nil
	case "MINUTES":
		return int(diff.Minutes()), nil
	case "SECONDS":
		return int(diff.Seconds()), nil
	case "DAYS":
		return int(diff.Hours() / 24), nil
	case "WEEKS":
		return int(diff.Hours() / (24 * 7)), nil
	case "MONTHS":
		// Approximate: 30 days per month
		return int(diff.Hours() / (24 * 30)), nil
	case "YEARS":
		// Approximate: 365 days per year
		return int(diff.Hours() / (24 * 365)), nil
	default:
		return nil, fmt.Errorf("unsupported DATEDIFF unit: %s (supported: HOURS, MINUTES, SECONDS, DAYS, WEEKS, MONTHS, YEARS)", unit)
	}
}

// parseTimeFixedDigits parses the fixed-width layouts "2006-01-02 15:04:05",
// "2006-01-02T15:04:05" and "2006-01-02" directly, without going through
// time.Parse's layout interpreter. Timestamp columns in analytical/RAG
// queries (RECENCY_SCORE, RAG_HYBRID_SCORE, date functions) are parsed once
// per row, and time.Parse's generality made it a top-3 CPU cost in such
// scans; direct digit slicing is ~15x cheaper. Returns ok=false for
// anything that does not match exactly — including out-of-range components,
// which time.Date would silently normalize (e.g. month 13 → January) where
// time.Parse reports an error — so callers fall back to the general path
// and error behavior is unchanged.
func parseTimeFixedDigits(s string) (time.Time, bool) {
	digit2 := func(i int) (int, bool) {
		c0, c1 := s[i]-'0', s[i+1]-'0'
		if c0 > 9 || c1 > 9 {
			return 0, false
		}
		return int(c0)*10 + int(c1), true
	}
	if len(s) != 10 && len(s) != 19 {
		return time.Time{}, false
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	yHi, ok1 := digit2(0)
	yLo, ok2 := digit2(2)
	m, ok3 := digit2(5)
	d, ok4 := digit2(8)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return time.Time{}, false
	}
	y := yHi*100 + yLo
	var hh, mm, ss int
	if len(s) == 19 {
		if (s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':' {
			return time.Time{}, false
		}
		var ok5, ok6, ok7 bool
		hh, ok5 = digit2(11)
		mm, ok6 = digit2(14)
		ss, ok7 = digit2(17)
		if !ok5 || !ok6 || !ok7 || hh > 23 || mm > 59 || ss > 59 {
			return time.Time{}, false
		}
	}
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return time.Time{}, false
	}
	t := time.Date(y, time.Month(m), d, hh, mm, ss, 0, time.UTC)
	// Reject dates time.Date normalized (e.g. Feb 31 → Mar 3) to keep
	// time.Parse's out-of-range error semantics via the fallback path.
	if t.Day() != d {
		return time.Time{}, false
	}
	return t, true
}

func parseTimeValue(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as time")
	}

	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		if t, ok := parseTimeFixedDigits(v); ok {
			return t, nil
		}
		// Select candidate formats by string length to avoid trying all formats on every call.
		var formats []string
		switch len(v) {
		case 5: // "15:04"
			formats = []string{"15:04"}
		case 8: // "15:04:05"
			formats = []string{"15:04:05"}
		case 10: // "2006-01-02"
			formats = []string{"2006-01-02"}
		case 16: // "2006-01-02 15:04"
			formats = []string{"2006-01-02 15:04"}
		case 19: // "2006-01-02 15:04:05" or "2006-01-02T15:04:05"
			formats = []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"}
		default: // RFC3339 with timezone and other variants
			formats = []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02", "15:04:05", "15:04"}
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse '%s' as time", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", val)
	}
}

func triToValue(t int) any {
	if t == tvTrue {
		return true
	}
	if t == tvFalse {
		return false
	}
	return nil
}

// String manipulation functions
// trimSide selects which side(s) a trim function removes characters from.
type trimSide uint8

const (
	trimLeft trimSide = 1 << iota
	trimRight
	trimBoth = trimLeft | trimRight
)

// evalTrimCommon implements TRIM/LTRIM/RTRIM(str [, cutset]).
// NULL input yields NULL; non-string inputs are coerced to their text form
// (SQLite/MySQL behaviour, e.g. LTRIM(123) = '123'); the default cutset is
// Unicode whitespace (unicode.IsSpace), consistent across all three functions.
func evalTrimCommon(env ExecEnv, name string, side trimSide, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("%s expects 1 or 2 arguments", name)
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	cutset := ""
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("%s cutset must be a string", name)
			}
		}
	}

	if cutset == "" {
		// Default: Unicode-aware whitespace trimming.
		switch side {
		case trimLeft:
			return strings.TrimLeftFunc(str, unicode.IsSpace), nil
		case trimRight:
			return strings.TrimRightFunc(str, unicode.IsSpace), nil
		default:
			return strings.TrimSpace(str), nil
		}
	}
	switch side {
	case trimLeft:
		return strings.TrimLeft(str, cutset), nil
	case trimRight:
		return strings.TrimRight(str, cutset), nil
	default:
		return strings.Trim(str, cutset), nil
	}
}

func evalLTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "LTRIM", trimLeft, args, row)
}

func evalRTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "RTRIM", trimRight, args, row)
}

func evalTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "TRIM", trimBoth, args, row)
}

// ISNULL function - returns TRUE if argument is NULL, FALSE otherwise
func evalIsNullFunc(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ISNULL expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	return val == nil, nil
}

// String manipulation functions - UPPER, LOWER, CONCAT, LENGTH, SUBSTRING

func evalUpper(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return strings.ToUpper(str), nil
}

func evalLower(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return strings.ToLower(str), nil
}

func evalConcat(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return "", nil
	}

	var sb strings.Builder
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}

		if val != nil {
			str, ok := val.(string)
			if !ok {
				str = fmt.Sprintf("%v", val)
			}
			sb.WriteString(str)
		}
	}

	return sb.String(), nil
}

func evalLength(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return len(str), nil
}

//nolint:gocyclo // SUBSTRING handling covers varying arity, coercion, and bounds checks.
func evalSubstring(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING expects 2 or 3 arguments")
	}

	// Get string value
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// Get start position (1-indexed in SQL)
	startVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	startAny, err := coerceToInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING start position must be numeric")
	}
	start, ok := startAny.(int)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING start position must be an integer")
	}
	start = start - 1 // Convert to 0-indexed
	if start < 0 {
		start = 0
	}
	if start >= len(str) {
		return "", nil
	}

	// Get length if provided
	if len(args) == 3 {
		lengthVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		lengthAny, err := coerceToInt(lengthVal)
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING length must be numeric")
		}
		length, ok := lengthAny.(int)
		if !ok {
			return nil, fmt.Errorf("SUBSTRING length must be an integer")
		}

		end := start + length
		if end > len(str) {
			end = len(str)
		}
		return str[start:end], nil
	}

	return str[start:], nil
}

func evalLeft(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LEFT expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LEFT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("LEFT length must be an integer")
	}

	if length < 0 {
		return "", nil
	}
	if length > len(str) {
		return str, nil
	}
	return str[:length], nil
}

func evalRight(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("RIGHT expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RIGHT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("RIGHT length must be an integer")
	}

	if length < 0 {
		return "", nil
	}
	if length > len(str) {
		return str, nil
	}
	return str[len(str)-length:], nil
}

// New string functions

func evalReplace(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("REPLACE expects 3 arguments: (string, from, to)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	fromVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	from := fmt.Sprintf("%v", fromVal)

	toVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	to := fmt.Sprintf("%v", toVal)

	return strings.ReplaceAll(str, from, to), nil
}

func evalInstr(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("INSTR expects 2 arguments: (string, search)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return 0, nil
	}
	str := fmt.Sprintf("%v", val)

	searchVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	search := fmt.Sprintf("%v", searchVal)

	idx := strings.Index(str, search)
	if idx == -1 {
		return 0, nil
	}
	return idx + 1, nil // 1-based index
}

func evalReverse(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("REVERSE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func evalRepeat(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("REPEAT expects 2 arguments: (string, count)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	countVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	countAny, err := coerceToInt(countVal)
	if err != nil {
		return nil, fmt.Errorf("REPEAT count must be numeric")
	}
	count, ok := countAny.(int)
	if !ok || count < 0 {
		return "", nil
	}
	return strings.Repeat(str, count), nil
}

func evalPrintf(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("PRINTF expects at least 1 argument")
	}
	formatVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	format := fmt.Sprintf("%v", formatVal)

	fmtArgs := make([]any, len(args)-1)
	for i := 1; i < len(args); i++ {
		v, err := evalExpr(env, args[i], row)
		if err != nil {
			return nil, err
		}
		fmtArgs[i-1] = v
	}
	return fmt.Sprintf(format, fmtArgs...), nil
}

func evalLpad(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("LPAD expects 2-3 arguments: (string, length[, pad])")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return str, nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		pad = fmt.Sprintf("%v", padVal)
		if pad == "" {
			pad = " "
		}
	}

	if len(str) >= length {
		return str[:length], nil
	}
	needed := length - len(str)
	padding := strings.Repeat(pad, (needed/len(pad))+1)[:needed]
	return padding + str, nil
}

func evalRpad(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("RPAD expects 2-3 arguments: (string, length[, pad])")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return str, nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		pad = fmt.Sprintf("%v", padVal)
		if pad == "" {
			pad = " "
		}
	}

	if len(str) >= length {
		return str[:length], nil
	}
	needed := length - len(str)
	padding := strings.Repeat(pad, (needed/len(pad))+1)[:needed]
	return str + padding, nil
}

// Math functions

func evalAbs(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ABS requires numeric argument")
	}
	if f < 0 {
		return -f, nil
	}
	return f, nil
}

func evalRound(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND expects 1-2 arguments")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ROUND requires numeric argument")
	}

	decimals := 0
	if len(args) == 2 {
		decVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if decVal == nil {
			return nil, nil
		}
		decAny, err := coerceToInt(decVal)
		if err != nil {
			return nil, fmt.Errorf("ROUND decimals must be numeric")
		}
		var ok bool
		decimals, ok = decAny.(int)
		if !ok {
			return nil, fmt.Errorf("ROUND decimals internal error: expected int, got %T", decAny)
		}
	}

	mult := math.Pow(10, float64(decimals))
	return math.Round(f*mult) / mult, nil
}

func evalFloor(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("FLOOR requires numeric argument")
	}
	return math.Floor(f), nil
}

func evalCeil(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("CEIL requires numeric argument")
	}
	return math.Ceil(f), nil
}

func evalGreatest(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("GREATEST expects at least 1 argument")
	}
	var result any
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp, err := compare(val, result)
		if err != nil {
			return nil, err
		}
		if cmp > 0 {
			result = val
		}
	}
	return result, nil
}

func evalLeast(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LEAST expects at least 1 argument")
	}
	var result any
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp, err := compare(val, result)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			result = val
		}
	}
	return result, nil
}

func evalIf(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("IF expects 3 arguments: (condition, true_value, false_value)")
	}
	cond, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	// Check if condition is truthy
	isTrue := false
	switch v := cond.(type) {
	case bool:
		isTrue = v
	case int:
		isTrue = v != 0
	case float64:
		isTrue = v != 0
	case string:
		isTrue = v != "" && v != "0" && strings.ToLower(v) != "false"
	default:
		isTrue = cond != nil
	}
	if isTrue {
		return evalExpr(env, args[1], row)
	}
	return evalExpr(env, args[2], row)
}

func evalRandom(env ExecEnv, args []Expr, row Row) (any, error) {
	return rand.Float64(), nil
}

// Date/time functions

func evalStrftime(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("STRFTIME expects 1-2 arguments: (format[, datetime])")
	}
	formatVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	format := fmt.Sprintf("%v", formatVal)

	var t time.Time
	if len(args) == 2 {
		dtVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(dtVal)
		if err != nil {
			return nil, err
		}
	} else {
		t = time.Now()
	}

	return strftimeFormat(t, format), nil
}

func evalDate(env ExecEnv, args []Expr, row Row) (any, error) {
	var t time.Time
	if len(args) == 0 {
		t = time.Now()
	} else {
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(val)
		if err != nil {
			return nil, err
		}
	}
	return t.Format("2006-01-02"), nil
}

func evalTime(env ExecEnv, args []Expr, row Row) (any, error) {
	var t time.Time
	if len(args) == 0 {
		t = time.Now()
	} else {
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(val)
		if err != nil {
			return nil, err
		}
	}
	return t.Format("15:04:05"), nil
}

func evalYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("YEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Year(), nil
}

func evalMonth(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MONTH expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return int(t.Month()), nil
}

func evalDay(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAY expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Day(), nil
}

func evalHour(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("HOUR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Hour(), nil
}

func evalMinute(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MINUTE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Minute(), nil
}

func evalSecond(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SECOND expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Second(), nil
}

// parseDateTime tries to parse a value as a time.Time
func parseDateTime(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as datetime")
	}
	if t, ok := val.(time.Time); ok {
		return t, nil
	}
	str := fmt.Sprintf("%v", val)
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, str); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse '%s' as datetime", str)
}

// strftimeFormat converts SQLite-style strftime format to Go time
func strftimeFormat(t time.Time, format string) string {
	// Map SQLite strftime codes to Go format
	replacements := map[string]string{
		"%Y": "2006",
		"%m": "01",
		"%d": "02",
		"%H": "15",
		"%M": "04",
		"%S": "05",
		"%j": fmt.Sprintf("%03d", t.YearDay()),
		"%W": fmt.Sprintf("%02d", (t.YearDay()-int(t.Weekday())+7)/7),
		"%w": fmt.Sprintf("%d", t.Weekday()),
		"%s": fmt.Sprintf("%d", t.Unix()),
		"%%": "%",
	}
	result := format
	for code, goFmt := range replacements {
		result = strings.ReplaceAll(result, code, goFmt)
	}
	return t.Format(result)
}

// -------------------- Additional Math Functions --------------------

func evalMod(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD expects 2 arguments")
	}
	av, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	bv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	a, ok := numeric(av)
	if !ok {
		return nil, fmt.Errorf("MOD: first argument must be numeric")
	}
	b, ok := numeric(bv)
	if !ok {
		return nil, fmt.Errorf("MOD: second argument must be numeric")
	}
	if b == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}
	return math.Mod(a, b), nil
}

func evalPower(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("POWER expects 2 arguments")
	}
	baseVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	expVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	base, ok := numeric(baseVal)
	if !ok {
		return nil, fmt.Errorf("POWER: base must be numeric")
	}
	exp, ok := numeric(expVal)
	if !ok {
		return nil, fmt.Errorf("POWER: exponent must be numeric")
	}
	return math.Pow(base, exp), nil
}

func evalSqrt(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SQRT expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SQRT: argument must be numeric")
	}
	if n < 0 {
		return nil, fmt.Errorf("SQRT: argument must be non-negative")
	}
	return math.Sqrt(n), nil
}

func evalLog(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("LOG expects 1 or 2 arguments")
	}
	if len(args) == 1 {
		// LOG(x) = natural log
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		n, ok := numeric(val)
		if !ok {
			return nil, fmt.Errorf("LOG: argument must be numeric")
		}
		if n <= 0 {
			return nil, fmt.Errorf("LOG: argument must be positive")
		}
		return math.Log(n), nil
	}
	// LOG(base, x)
	baseVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	val, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	base, ok := numeric(baseVal)
	if !ok {
		return nil, fmt.Errorf("LOG: base must be numeric")
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG: argument must be numeric")
	}
	if base <= 0 || base == 1 || n <= 0 {
		return nil, fmt.Errorf("LOG: invalid arguments")
	}
	return math.Log(n) / math.Log(base), nil
}

func evalLn(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LN: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LN: argument must be positive")
	}
	return math.Log(n), nil
}

func evalLog10(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOG10 expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG10: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LOG10: argument must be positive")
	}
	return math.Log10(n), nil
}

func evalLog2(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOG2 expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG2: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LOG2: argument must be positive")
	}
	return math.Log2(n), nil
}

func evalExp(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("EXP expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("EXP: argument must be numeric")
	}
	return math.Exp(n), nil
}

func evalSign(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIGN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SIGN: argument must be numeric")
	}
	if n > 0 {
		return 1, nil
	} else if n < 0 {
		return -1, nil
	}
	return 0, nil
}

func evalTruncate(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TRUNCATE expects 1-2 arguments")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("TRUNCATE: argument must be numeric")
	}
	decimals := 0
	if len(args) == 2 {
		dv, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		d, ok := numeric(dv)
		if !ok {
			return nil, fmt.Errorf("TRUNCATE: decimals must be numeric")
		}
		decimals = int(d)
	}
	multiplier := math.Pow(10, float64(decimals))
	return math.Trunc(n*multiplier) / multiplier, nil
}

// Trigonometric functions
func evalSin(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SIN: argument must be numeric")
	}
	return math.Sin(n), nil
}

func evalCos(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("COS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("COS: argument must be numeric")
	}
	return math.Cos(n), nil
}

func evalTan(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TAN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("TAN: argument must be numeric")
	}
	return math.Tan(n), nil
}

func evalAsin(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASIN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ASIN: argument must be numeric")
	}
	if n < -1 || n > 1 {
		return nil, fmt.Errorf("ASIN: argument must be between -1 and 1")
	}
	return math.Asin(n), nil
}

func evalAcos(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ACOS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ACOS: argument must be numeric")
	}
	if n < -1 || n > 1 {
		return nil, fmt.Errorf("ACOS: argument must be between -1 and 1")
	}
	return math.Acos(n), nil
}

func evalAtan(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ATAN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ATAN: argument must be numeric")
	}
	return math.Atan(n), nil
}

func evalAtan2(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ATAN2 expects 2 arguments")
	}
	yVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	xVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	y, ok := numeric(yVal)
	if !ok {
		return nil, fmt.Errorf("ATAN2: y must be numeric")
	}
	x, ok := numeric(xVal)
	if !ok {
		return nil, fmt.Errorf("ATAN2: x must be numeric")
	}
	return math.Atan2(y, x), nil
}

func evalDegrees(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DEGREES expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("DEGREES: argument must be numeric")
	}
	return n * 180 / math.Pi, nil
}

func evalRadians(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("RADIANS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("RADIANS: argument must be numeric")
	}
	return n * math.Pi / 180, nil
}

// -------------------- Additional String Functions --------------------

func evalSpace(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SPACE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SPACE: argument must be numeric")
	}
	if n < 0 {
		return "", nil
	}
	return strings.Repeat(" ", int(n)), nil
}

func evalAscii(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASCII expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	if len(s) == 0 {
		return 0, nil
	}
	return int(s[0]), nil
}

func evalChar(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("CHAR: argument must be numeric")
	}
	if n < 0 || n > 127 {
		return "", nil
	}
	return string(rune(int(n))), nil
}

func evalInitcap(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("INITCAP expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	words := strings.Fields(s)
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(string(word[0])) + strings.ToLower(word[1:])
		}
	}
	return strings.Join(words, " "), nil
}

func evalSplitPart(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("SPLIT_PART expects 3 arguments: (string, delimiter, part)")
	}
	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	delimVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	partVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", strVal)
	delim := fmt.Sprintf("%v", delimVal)
	part, ok := numeric(partVal)
	if !ok {
		return nil, fmt.Errorf("SPLIT_PART: part must be numeric")
	}
	parts := strings.Split(s, delim)
	idx := int(part) - 1 // 1-indexed
	if idx < 0 || idx >= len(parts) {
		return "", nil
	}
	return parts[idx], nil
}

func evalSoundex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SOUNDEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := strings.ToUpper(fmt.Sprintf("%v", val))
	if len(s) == 0 {
		return "", nil
	}
	// Soundex algorithm
	result := []byte{s[0]}
	mapping := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	lastCode := mapping[s[0]]
	for i := 1; i < len(s) && len(result) < 4; i++ {
		c := s[i]
		if code, ok := mapping[c]; ok && code != lastCode {
			result = append(result, code)
			lastCode = code
		} else if c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U' || c == 'H' || c == 'W' || c == 'Y' {
			lastCode = 0
		}
	}
	for len(result) < 4 {
		result = append(result, '0')
	}
	return string(result), nil
}

func evalQuote(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("QUOTE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return "NULL", nil
	}
	s := fmt.Sprintf("%v", val)
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'", nil
}

func evalHex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("HEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	return fmt.Sprintf("%X", []byte(s)), nil
}

func evalUnhex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNHEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	if len(s)%2 != 0 {
		s = "0" + s
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		var b byte
		_, err := fmt.Sscanf(s[i:i+2], "%02X", &b)
		if err != nil {
			return nil, fmt.Errorf("UNHEX: invalid hex string")
		}
		result[i/2] = b
	}
	return string(result), nil
}

func evalConcatWs(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("CONCAT_WS expects at least 2 arguments")
	}
	sepVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	sep := fmt.Sprintf("%v", sepVal)
	var parts []string
	for _, arg := range args[1:] {
		v, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(parts, sep), nil
}

func evalPosition(env ExecEnv, args []Expr, row Row) (any, error) {
	// POSITION(substring IN string) - 1-indexed, returns 0 if not found
	if len(args) != 2 {
		return nil, fmt.Errorf("POSITION expects 2 arguments: (substring, string)")
	}
	subVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	strVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	sub := fmt.Sprintf("%v", subVal)
	str := fmt.Sprintf("%v", strVal)
	idx := strings.Index(str, sub)
	if idx < 0 {
		return 0, nil
	}
	return idx + 1, nil
}

// -------------------- Additional Functions --------------------

func evalUuid(env ExecEnv, args []Expr, row Row) (any, error) {
	// Generate a simple UUID v4
	b := make([]byte, 16)
	crand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func evalTypeof(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TYPEOF expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return "null", nil
	}
	switch val.(type) {
	case int, int8, int16, int32, int64:
		return "integer", nil
	case uint, uint8, uint16, uint32, uint64:
		return "integer", nil
	case float32, float64:
		return "real", nil
	case bool:
		return "boolean", nil
	case string:
		return "text", nil
	case time.Time:
		return "datetime", nil
	case []any, map[string]any:
		return "json", nil
	default:
		return fmt.Sprintf("%T", val), nil
	}
}

func evalDayOfWeek(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAYOFWEEK expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return int(t.Weekday()) + 1, nil // 1=Sunday, 7=Saturday
}

func evalDayOfYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAYOFYEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.YearDay(), nil
}

func evalWeekOfYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WEEKOFYEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	_, week := t.ISOWeek()
	return week, nil
}

func evalQuarter(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("QUARTER expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return (int(t.Month())-1)/3 + 1, nil
}

func evalDateAdd(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD expects 3 arguments: (date, interval, unit)")
	}
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}

	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_ADD: interval must be numeric")
	}
	unit := strings.ToUpper(fmt.Sprintf("%v", unitVal))

	switch unit {
	case "YEAR", "YEARS":
		t = t.AddDate(int(interval), 0, 0)
	case "MONTH", "MONTHS":
		t = t.AddDate(0, int(interval), 0)
	case "DAY", "DAYS":
		t = t.AddDate(0, 0, int(interval))
	case "HOUR", "HOURS":
		t = t.Add(time.Duration(interval) * time.Hour)
	case "MINUTE", "MINUTES":
		t = t.Add(time.Duration(interval) * time.Minute)
	case "SECOND", "SECONDS":
		t = t.Add(time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("DATE_ADD: unknown unit '%s'", unit)
	}
	return t, nil
}

func evalDateSub(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_SUB expects 3 arguments: (date, interval, unit)")
	}
	// DATE_SUB is just DATE_ADD with negated interval
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}

	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_SUB: interval must be numeric")
	}
	interval = -interval // Negate for subtraction
	unit := strings.ToUpper(fmt.Sprintf("%v", unitVal))

	switch unit {
	case "YEAR", "YEARS":
		t = t.AddDate(int(interval), 0, 0)
	case "MONTH", "MONTHS":
		t = t.AddDate(0, int(interval), 0)
	case "DAY", "DAYS":
		t = t.AddDate(0, 0, int(interval))
	case "HOUR", "HOURS":
		t = t.Add(time.Duration(interval) * time.Hour)
	case "MINUTE", "MINUTES":
		t = t.Add(time.Duration(interval) * time.Minute)
	case "SECOND", "SECONDS":
		t = t.Add(time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("DATE_SUB: unknown unit '%s'", unit)
	}
	return t, nil
}

func evalCast(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("CAST expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	typeExpr, ok := args[1].(*VarRef)
	if !ok {
		lit, ok := args[1].(*Literal)
		if !ok {
			return nil, fmt.Errorf("CAST type must be a type name")
		}
		typeStr, ok := lit.Val.(string)
		if !ok {
			return nil, fmt.Errorf("CAST type must be a string")
		}
		return castValue(val, strings.ToUpper(typeStr))
	}

	return castValue(val, strings.ToUpper(typeExpr.Name))
}

func castValue(val any, targetType string) (any, error) {
	if val == nil {
		return nil, nil
	}

	switch targetType {
	case "TEXT", "STRING", "VARCHAR", "CHAR":
		return fmt.Sprintf("%v", val), nil
	case "INT", "INTEGER":
		return coerceToInt(val)
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC":
		if f, ok := numeric(val); ok {
			return f, nil
		}
		str := fmt.Sprintf("%v", val)
		return strconv.ParseFloat(str, 64)
	case "BOOL", "BOOLEAN":
		switch v := val.(type) {
		case bool:
			return v, nil
		case int:
			return v != 0, nil
		case float64:
			return v != 0, nil
		case string:
			return strings.ToLower(v) == "true" || v == "1", nil
		}
		return false, nil
	default:
		return nil, fmt.Errorf("unsupported cast type: %s", targetType)
	}
}

// Hashing functions - MD5, SHA1, SHA256, SHA512

func evalMD5(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MD5 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// MD5 hash
	hasher := md5.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA1(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA1 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// SHA1 hash
	hasher := sha1.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA256(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA256 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// SHA256 hash
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA512(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA512 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// SHA512 hash
	hasher := sha512.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalBase64(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BASE64 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// Base64 encode
	encoded := base64.StdEncoding.EncodeToString([]byte(str))
	return encoded, nil
}

func evalBase64Decode(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BASE64_DECODE expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// Base64 decode
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("BASE64_DECODE: %v", err)
	}

	return string(decoded), nil
}

func isAggregate(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN",
			"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX":
			return true
		}
	case *Unary:
		return isAggregate(ex.Expr)
	case *Binary:
		return isAggregate(ex.Left) || isAggregate(ex.Right)
	case *IsNull:
		return isAggregate(ex.Expr)
	case *CaseExpr:
		if ex.Operand != nil && isAggregate(ex.Operand) {
			return true
		}
		for _, w := range ex.Whens {
			if isAggregate(w.When) || isAggregate(w.Then) {
				return true
			}
		}
		if ex.Else != nil && isAggregate(ex.Else) {
			return true
		}
	}
	return false
}

func evalAggregate(env ExecEnv, e Expr, rows []Row) (any, error) {
	switch ex := e.(type) {
	case *FuncCall:
		return evalAggregateFuncCall(env, ex, rows)
	case *Unary:
		return evalAggregateUnary(env, ex, rows)
	case *Binary:
		return evalAggregateBinary(env, ex, rows)
	case *IsNull:
		return evalAggregateIsNull(env, ex, rows)
	case *CaseExpr:
		return evalAggregateCase(env, ex, rows)
	default:
		if len(rows) == 0 {
			return nil, nil
		}
		return evalExpr(env, e, rows[0])
	}
}

func evalAggregateFuncCall(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	switch ex.Name {
	case "COUNT":
		return evalAggregateCount(env, ex, rows)
	case "SUM", "AVG":
		return evalAggregateSumAvg(env, ex, rows)
	case "MIN", "MAX":
		return evalAggregateMinMax(env, ex, rows)
	case "MEDIAN":
		return evalAggregateMedian(env, ex, rows)
	case "MIN_BY", "ARG_MIN":
		return evalAggregateMinBy(env, ex, rows)
	case "MAX_BY", "ARG_MAX":
		return evalAggregateMaxBy(env, ex, rows)
	case "VEC_AVG":
		return evalAggregateVecAvg(env, ex, rows)
	default:
		// For non-aggregate functions like DATEDIFF, LEFT, etc., evaluate their arguments
		// in the aggregate context first, then call the function
		if len(rows) == 0 {
			return nil, nil
		}

		// Create a new FuncCall with evaluated arguments
		evaledArgs := make([]Expr, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := evalAggregate(env, arg, rows)
			if err != nil {
				return nil, err
			}
			evaledArgs[i] = &Literal{Val: val}
		}

		evaledFunc := &FuncCall{
			Name:     ex.Name,
			Args:     evaledArgs,
			Star:     ex.Star,
			Distinct: ex.Distinct,
		}

		// Now evaluate the function normally with a single row
		return evalFuncCall(env, evaledFunc, rows[0])
	}
}

func evalAggregateCount(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if ex.Star {
		return len(rows), nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}

	// Handle COUNT(DISTINCT col)
	if ex.Distinct {
		seen := make(map[string]bool)
		for _, r := range rows {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
			v, err := evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			if v != nil {
				// Convert to string for deduplication
				key := fmt.Sprintf("%v", v)
				seen[key] = true
			}
		}
		return len(seen), nil
	}

	// Regular COUNT(col)
	cnt := 0
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v != nil {
			cnt++
		}
	}
	return cnt, nil
}

func evalAggregateSumAvg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	var (
		sumFloat float64
		sumRat   = new(big.Rat)
		useRat   bool
		n        int
	)
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if f, ok := numeric(v); ok {
			if useRat {
				sumRat.Add(sumRat, new(big.Rat).SetFloat64(f))
			} else {
				sumFloat += f
			}
			n++
			continue
		}
		if rv, ok := storage.DecimalFromAny(v); ok {
			if !useRat {
				// migrate any accumulated float sum into rational
				if n > 0 {
					sumRat.SetFloat64(sumFloat)
				}
				useRat = true
			}
			sumRat.Add(sumRat, new(big.Rat).Set(rv))
			n++
			continue
		}
	}
	if ex.Name == "SUM" {
		if useRat {
			return sumRat, nil
		}
		return sumFloat, nil
	}
	if n == 0 {
		return nil, nil
	}
	if useRat {
		avg := new(big.Rat).Quo(sumRat, big.NewRat(int64(n), 1))
		return avg, nil
	}
	return sumFloat / float64(n), nil
}

func evalAggregateMinMax(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	var have bool
	var best any
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if !have {
			best = v
			have = true
		} else {
			cmp, err := compare(v, best)
			if err == nil {
				if ex.Name == "MIN" && cmp < 0 {
					best = v
				}
				if ex.Name == "MAX" && cmp > 0 {
					best = v
				}
			}
		}
	}
	if !have {
		return nil, nil
	}
	return best, nil
}

func evalAggregateMedian(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("MEDIAN expects 1 arg")
	}
	var values []float64
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if f, ok := numeric(v); ok {
			values = append(values, f)
		}
	}
	if len(values) == 0 {
		return nil, nil
	}

	// Sort values
	sort.Float64s(values)

	// Calculate median
	n := len(values)
	if n%2 == 1 {
		// Odd number of values - return middle value
		return values[n/2], nil
	}
	// Even number of values - return average of two middle values
	return (values[n/2-1] + values[n/2]) / 2.0, nil
}

// evalAggregateMinBy returns the value from first argument where second argument is minimum
// Usage: MIN_BY(value_column, order_column)
func evalAggregateMinBy(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("MIN_BY expects 2 arguments: (value_column, order_column)")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var minOrderVal any
	var resultVal any
	first := true

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}

		// Evaluate the ordering column (second argument)
		orderVal, err := evalExpr(env, ex.Args[1], r)
		if err != nil {
			return nil, err
		}

		// Skip NULL values in comparison
		if orderVal == nil {
			continue
		}

		// First non-NULL value or found a smaller value
		if first {
			minOrderVal = orderVal
			resultVal, err = evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			first = false
		} else {
			cmp, err := compare(orderVal, minOrderVal)
			if err == nil && cmp < 0 {
				minOrderVal = orderVal
				resultVal, err = evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return resultVal, nil
}

// evalAggregateMaxBy returns the value from first argument where second argument is maximum
// Usage: MAX_BY(value_column, order_column)
func evalAggregateMaxBy(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("MAX_BY expects 2 arguments: (value_column, order_column)")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var maxOrderVal any
	var resultVal any
	first := true

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}

		// Evaluate the ordering column (second argument)
		orderVal, err := evalExpr(env, ex.Args[1], r)
		if err != nil {
			return nil, err
		}

		// Skip NULL values in comparison
		if orderVal == nil {
			continue
		}

		// First non-NULL value or found a larger value
		if first {
			maxOrderVal = orderVal
			resultVal, err = evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			first = false
		} else {
			cmp, err := compare(orderVal, maxOrderVal)
			if err == nil && cmp > 0 {
				maxOrderVal = orderVal
				resultVal, err = evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return resultVal, nil
}

// evalAggregateVecAvg computes the element-wise average of a set of vectors.
func evalAggregateVecAvg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("VEC_AVG expects 1 argument")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var sum []float64
	count := 0

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		val, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		var vec []float64
		switch v := val.(type) {
		case []float64:
			vec = v
		default:
			continue
		}
		if sum == nil {
			sum = make([]float64, len(vec))
		}
		if len(vec) != len(sum) {
			return nil, fmt.Errorf("VEC_AVG: dimension mismatch (%d vs %d)", len(vec), len(sum))
		}
		for i, f := range vec {
			sum[i] += f
		}
		count++
	}

	if count == 0 {
		return nil, nil
	}
	avg := make([]float64, len(sum))
	for i := range sum {
		avg[i] = sum[i] / float64(count)
	}
	return avg, nil
}

func evalAggregateUnary(env ExecEnv, ex *Unary, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			return new(big.Rat).Set(r), nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			neg := new(big.Rat).Set(r)
			neg.Mul(neg, big.NewRat(-1, 1))
			return neg, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalAggregateBinary(env ExecEnv, ex *Binary, rows []Row) (any, error) {
	lv, err := evalAggregate(env, ex.Left, rows)
	if err != nil {
		return nil, err
	}
	rv, err := evalAggregate(env, ex.Right, rows)
	if err != nil {
		return nil, err
	}
	return evalExpr(env, &Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
}

func evalAggregateIsNull(env ExecEnv, ex *IsNull, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	if ex.Negate {
		return !isNull(v), nil
	}
	return isNull(v), nil
}

func evalAggregateCase(env ExecEnv, ex *CaseExpr, rows []Row) (any, error) {
	if ex.Operand != nil {
		target, err := evalAggregate(env, ex.Operand, rows)
		if err != nil {
			return nil, err
		}
		for _, w := range ex.Whens {
			whenVal, err := evalAggregate(env, w.When, rows)
			if err != nil {
				return nil, err
			}
			if cmp, err := compare(target, whenVal); err == nil && cmp == 0 {
				return evalAggregate(env, w.Then, rows)
			}
		}
	} else {
		for _, w := range ex.Whens {
			cond, err := evalAggregate(env, w.When, rows)
			if err != nil {
				return nil, err
			}
			if toTri(cond) == tvTrue {
				return evalAggregate(env, w.Then, rows)
			}
		}
	}
	if ex.Else != nil {
		return evalAggregate(env, ex.Else, rows)
	}
	return nil, nil
}

func rowsFromTable(t *storage.Table, alias string) ([]Row, []string) {
	numCols := len(t.Cols)
	// Pre-compute lowercase qualified and unqualified column keys.
	qualKeys := make([]string, numCols)
	unqualKeys := make([]string, numCols)
	for i, c := range t.Cols {
		qualKeys[i] = strings.ToLower(alias + "." + c.Name)
		unqualKeys[i] = strings.ToLower(c.Name)
	}

	cols := make([]string, numCols)
	copy(cols, qualKeys)

	// Pre-compute which unqualified names are unique (no duplicates), and
	// whether any duplicate exists at all. This is the only thing that can
	// make inserting a column's unqualified key unsafe to do unconditionally
	// (a qualified key like "t.name" can't collide with an unqualified key
	// for a different, uniquely-named column). Real schemas never have
	// duplicate column names, so computing this once per query — instead of
	// re-checking "does this key already exist" on every single row below —
	// turns the common case into one map assignment per column instead of a
	// map lookup plus a conditional assignment.
	unqualSeen := make(map[string]bool, numCols)
	firstOccurrence := make([]bool, numCols)
	hasDup := false
	for i, k := range unqualKeys {
		if unqualSeen[k] {
			hasDup = true
			continue
		}
		unqualSeen[k] = true
		firstOccurrence[i] = true
	}
	// Total keys per row: qualified + unique unqualified.
	keysPerRow := numCols + len(unqualSeen)

	out := make([]Row, len(t.Rows))
	if !hasDup {
		for ri, r := range t.Rows {
			row := make(Row, keysPerRow)
			for i := range t.Cols {
				v := r[i]
				row[qualKeys[i]] = v
				row[unqualKeys[i]] = v
			}
			out[ri] = row
		}
		return out, cols
	}

	// Slow path: at least one duplicate unqualified column name exists, so
	// the first occurrence (in column order) must win — mirrors the
	// pre-optimization behavior exactly.
	for ri, r := range t.Rows {
		row := make(Row, keysPerRow)
		for i := range t.Cols {
			row[qualKeys[i]] = r[i]
		}
		for i := range t.Cols {
			if firstOccurrence[i] {
				row[unqualKeys[i]] = r[i]
			}
		}
		out[ri] = row
	}
	return out, cols
}

func keysOfRow(r Row) []string {
	var ks []string
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func aliasOr(f FromItem) string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Table
}

func mergeRows(l, r Row) Row {
	m := make(Row, len(l)+len(r))
	for k, v := range l {
		m[k] = v
	}
	for k, v := range r {
		m[k] = v
	}
	return m
}
func cloneRow(r Row) Row {
	m := make(Row, len(r))
	for k, v := range r {
		m[k] = v
	}
	return m
}
func addRightNulls(m Row, alias string, t *storage.Table) {
	for _, c := range t.Cols {
		putVal(m, alias+"."+c.Name, nil)
		if _, ex := m[strings.ToLower(c.Name)]; !ex {
			putVal(m, c.Name, nil)
		}
	}
}

func fmtKeyPart(v any) string {
	var b strings.Builder
	writeFmtKeyPart(&b, v)
	return b.String()
}

func comparableKeyPart(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case int:
		return x
	case int64:
		return x
	case float64:
		return x
	case bool:
		return x
	case string:
		return x
	default:
		return fmtKeyPart(v)
	}
}

// writeFmtKeyPart writes a typed key part directly to a builder, avoiding
// intermediate string allocations used by the old fmtKeyPart approach.
func writeFmtKeyPart(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("N:")
	case int:
		b.WriteString("I:")
		b.WriteString(strconv.Itoa(x))
	case float64:
		b.WriteString("F:")
		b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
	case bool:
		if x {
			b.WriteString("B:1")
		} else {
			b.WriteString("B:0")
		}
	case string:
		b.WriteString("S:")
		b.WriteString(x)
	default:
		byt, _ := storage.JSONMarshal(x)
		b.WriteString("J:")
		b.Write(byt)
	}
}

func distinctRows(rows []Row, cols []string) []Row {
	seen := make(map[string]bool, len(rows)/2)
	out := make([]Row, 0, len(rows))
	// Pre-lowercase column names once.
	lcCols := make([]string, len(cols))
	for i, c := range cols {
		lcCols[i] = strings.ToLower(c)
	}
	var buf strings.Builder
	for _, r := range rows {
		buf.Reset()
		for i, c := range lcCols {
			if i > 0 {
				buf.WriteByte('|')
			}
			writeFmtKeyPart(&buf, r[c])
		}
		key := buf.String()
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

func inferType(v any) storage.ColType {
	switch v.(type) {
	case int, int64:
		return storage.IntType
	case float64:
		return storage.FloatType
	case bool:
		return storage.BoolType
	case string:
		return storage.TextType
	case []float64:
		return storage.VectorType
	case []byte:
		return storage.BlobType
	default:
		return storage.JsonType
	}
}
func coerceToTypeAllowNull(v any, t storage.ColType) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case storage.IntType:
		return coerceToInt(v)
	case storage.FloatType:
		return coerceToFloat(v)
	case storage.TextType:
		return fmt.Sprintf("%v", v), nil
	case storage.BoolType:
		return coerceToBool(v)
	case storage.JsonType:
		return coerceToJson(v)
	case storage.VectorType:
		return coerceToVector(v)
	case storage.BlobType:
		return coerceToBlob(v)
	default:
		return v, nil
	}
}

// coerceColumnValue applies SQLite's documented affinity rules only to
// SQLite-style declarations. Native tinySQL columns retain their existing
// strict conversion behaviour. SQLite affinity conversion is deliberately
// lossless: a value which cannot be represented without changing meaning is
// retained with its original storage class rather than rejected or truncated.
func coerceColumnValue(v any, col storage.Column) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch col.Affinity {
	case storage.AffinityInteger:
		return coerceSQLiteInteger(v)
	case storage.AffinityReal:
		return coerceSQLiteReal(v)
	case storage.AffinityText:
		// SQLite does not coerce BLOB values when applying TEXT affinity.
		if _, ok := v.([]byte); ok {
			return v, nil
		}
		return fmt.Sprintf("%v", v), nil
	case storage.AffinityNumeric:
		return coerceSQLiteNumeric(v)
	case storage.AffinityBlob:
		return v, nil
	default:
		return coerceToTypeAllowNull(v, col.Type)
	}
}

func coerceSQLiteInteger(v any) (any, error) {
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			f, floatErr := strconv.ParseFloat(s, 64)
			if floatErr != nil || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
				return v, nil
			}
			return int(f), nil
		}
		return int(i), nil
	case float64:
		if math.Trunc(x) == x && x >= math.MinInt64 && x <= math.MaxInt64 {
			return int(x), nil
		}
	}
	return v, nil
}

func coerceSQLiteReal(v any) (any, error) {
	switch x := v.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return v, nil
		}
		return f, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	}
	return v, nil
}

func coerceSQLiteNumeric(v any) (any, error) {
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return int(i), nil
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if math.Trunc(f) == f && f >= math.MinInt64 && f <= math.MaxInt64 {
				return int(f), nil
			}
			return f, nil
		}
	}
	return v, nil
}

// MaxBlobBytes bounds a single BLOB accepted by the SQL executor. It avoids
// integer/codec overflows and accidental unbounded allocations while staying
// comfortably above normal compressed MVT payloads.
const MaxBlobBytes = 64 << 20

func coerceToBlob(v any) (any, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to BLOB", v)
	}
	if len(b) > MaxBlobBytes {
		return nil, fmt.Errorf("BLOB is %d bytes; maximum is %d", len(b), MaxBlobBytes)
	}
	// Driver callers commonly reuse their parameter buffer. The database owns
	// its row bytes, so retain neither the input nor a caller-visible alias.
	return append([]byte(nil), b...), nil
}

func coerceToInt(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to INT", x)
		}
		return n, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to INT", v)
	}
}

func coerceToBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int, int64:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		return s == "true" || s == "1" || s == "t" || s == "yes", nil
	default:
		return nil, fmt.Errorf("cannot convert %T to BOOL", v)
	}
}

func coerceToJson(v any) (any, error) {
	switch x := v.(type) {
	case string:
		var anyv any
		if json.Unmarshal([]byte(x), &anyv) == nil {
			return anyv, nil
		}
		return x, nil
	default:
		return x, nil
	}
}

// coerceToVector converts a value to []float64 for VECTOR columns.
// Accepts: []float64 (passthrough), JSON string "[1.0, 2.0, 3.0]",
// []any (from JSON parse), or []int.
func coerceToVector(v any) (any, error) {
	switch x := v.(type) {
	case []float64:
		return x, nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return nil, fmt.Errorf("cannot convert empty string to VECTOR")
		}
		var arr []float64
		if err := json.Unmarshal([]byte(s), &arr); err == nil {
			return arr, nil
		}
		// Try parsing as []any from JSON
		var anyArr []any
		if err := json.Unmarshal([]byte(s), &anyArr); err == nil {
			return anySliceToFloat64(anyArr)
		}
		return nil, fmt.Errorf("cannot convert %q to VECTOR", s)
	case []any:
		return anySliceToFloat64(x)
	case []int:
		out := make([]float64, len(x))
		for i, v := range x {
			out[i] = float64(v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to VECTOR", v)
	}
}

// anySliceToFloat64 converts a []any of numeric values to []float64.
func anySliceToFloat64(arr []any) ([]float64, error) {
	out := make([]float64, len(arr))
	for i, v := range arr {
		switch n := v.(type) {
		case float64:
			out[i] = n
		case int:
			out[i] = float64(n)
		case int64:
			out[i] = float64(n)
		case json.Number:
			f, err := n.Float64()
			if err != nil {
				return nil, fmt.Errorf("vector element %d: %w", i, err)
			}
			out[i] = f
		default:
			return nil, fmt.Errorf("vector element %d: cannot convert %T to float64", i, v)
		}
	}
	return out, nil
}

// JSON helpers

type pathPart struct {
	key string
	idx int
}

func parseJSONPath(s string) []pathPart {
	var out []pathPart
	cur := ""
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
		case '[':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
			j := i + 1
			for j < len(s) && s[j] != ']' {
				j++
			}
			if j <= len(s)-1 {
				n, _ := strconv.Atoi(s[i+1 : j])
				out = append(out, pathPart{idx: n})
				i = j
			}
		default:
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, pathPart{key: cur, idx: -1})
	}
	return out
}
func jsonGet(v any, path string) any {
	if v == nil || path == "" {
		return nil
	}
	parts := parseJSONPath(path)
	cur := v
	for _, p := range parts {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[p.key]
		case []any:
			if p.idx >= 0 && p.idx < len(c) {
				cur = c[p.idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return cur
}

//nolint:gocyclo // JSON setter walks paths with mixed map/array handling.
func jsonSet(v any, path string, value any) any {
	if path == "" {
		return value
	}

	parts := parseJSONPath(path)
	if len(parts) == 0 {
		return value
	}

	// If v is nil, create a new structure
	if v == nil {
		if parts[0].idx >= 0 {
			v = make([]any, parts[0].idx+1)
		} else {
			v = make(map[string]any)
		}
	}

	// Navigate to the parent of the target
	cur := v
	for i := 0; i < len(parts)-1; i++ {
		p := parts[i]
		switch c := cur.(type) {
		case map[string]any:
			if p.idx >= 0 {
				// This should be an array access, but we have a map
				return v
			}
			if c[p.key] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.key] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.key] = make(map[string]any)
				}
			}
			cur = c[p.key]
		case []any:
			if p.idx < 0 || p.idx >= len(c) {
				return v
			}
			if c[p.idx] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.idx] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.idx] = make(map[string]any)
				}
			}
			cur = c[p.idx]
		default:
			return v
		}
	}

	// Set the final value
	lastPart := parts[len(parts)-1]
	switch c := cur.(type) {
	case map[string]any:
		if lastPart.idx >= 0 {
			return v // Invalid: trying to use array index on map
		}
		c[lastPart.key] = value
	case []any:
		if lastPart.idx < 0 {
			return v // Invalid: trying to use string key on array
		}
		// Extend array if needed
		for len(c) <= lastPart.idx {
			c = append(c, nil)
		}
		c[lastPart.idx] = value
		// Update the reference in the parent
		if len(parts) > 1 {
			parentParts := parts[:len(parts)-1]
			parent := v
			for _, p := range parentParts[:len(parentParts)-1] {
				switch pc := parent.(type) {
				case map[string]any:
					parent = pc[p.key]
				case []any:
					parent = pc[p.idx]
				}
			}
			lastParentPart := parentParts[len(parentParts)-1]
			switch pc := parent.(type) {
			case map[string]any:
				pc[lastParentPart.key] = c
			case []any:
				pc[lastParentPart.idx] = c
			}
		} else {
			v = c
		}
	default:
		return v
	}

	return v
}

// -------------------- misc helpers --------------------

func columnsFromRows(rows []Row) []string {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	cols := make([]string, 0, len(rows[0]))
	for _, row := range rows {
		for k := range row {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// Helper functions for projName and anyAggInSelect

// projName generates the column name for a select item
func projName(it SelectItem, idx int) string {
	if it.Alias != "" {
		return it.Alias
	}
	if ref, ok := it.Expr.(*VarRef); ok {
		return ref.Name
	}
	return fmt.Sprintf("col_%d", idx)
}

// anyAggInSelect checks if any select item contains an aggregate function
func anyAggInSelect(items []SelectItem) bool {
	for _, it := range items {
		if isAggregate(it.Expr) {
			return true
		}
	}
	return false
}

// anyWindowInSelect checks if any window functions are used in SELECT projections
func anyWindowInSelect(items []SelectItem) bool {
	for _, it := range items {
		if hasWindowFunction(it.Expr) {
			return true
		}
	}
	return false
}

// hasWindowFunction checks if an expression contains a window function
func hasWindowFunction(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		if ex.Over != nil {
			return true
		}
		// Check arguments recursively
		for _, arg := range ex.Args {
			if hasWindowFunction(arg) {
				return true
			}
		}
	case *Unary:
		return hasWindowFunction(ex.Expr)
	case *Binary:
		return hasWindowFunction(ex.Left) || hasWindowFunction(ex.Right)
	case *IsNull:
		return hasWindowFunction(ex.Expr)
	case *CaseExpr:
		if ex.Operand != nil && hasWindowFunction(ex.Operand) {
			return true
		}
		for _, w := range ex.Whens {
			if hasWindowFunction(w.When) || hasWindowFunction(w.Then) {
				return true
			}
		}
		if ex.Else != nil && hasWindowFunction(ex.Else) {
			return true
		}
	}
	return false
}

// ==================== Window Function Support ====================

// extractWindowOffset extracts and validates offset from window function arguments
func extractWindowOffset(env ExecEnv, args []Expr, row Row, defaultOffset int) (int, error) {
	if len(args) <= 1 {
		return defaultOffset, nil
	}
	offsetVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return 0, err
	}
	if offsetInt, ok := offsetVal.(int); ok {
		return offsetInt, nil
	}
	if offsetFloat, ok := offsetVal.(float64); ok {
		return int(offsetFloat), nil
	}
	return defaultOffset, nil
}

// evalLagFunction evaluates the LAG window function
func evalLagFunction(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	offset, err := extractWindowOffset(env, ex.Args, row, 1)
	if err != nil {
		return nil, err
	}
	lagIdx := currentIdx - offset
	if lagIdx < 0 {
		// Return default value if provided
		if len(ex.Args) > 2 {
			return evalExpr(env, ex.Args[2], row)
		}
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[lagIdx])
}

// evalLeadFunction evaluates the LEAD window function
func evalLeadFunction(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	offset, err := extractWindowOffset(env, ex.Args, row, 1)
	if err != nil {
		return nil, err
	}
	leadIdx := currentIdx + offset
	if leadIdx >= len(partitionRows) {
		// Return default value if provided
		if len(ex.Args) > 2 {
			return evalExpr(env, ex.Args[2], row)
		}
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[leadIdx])
}

// evalFirstValue evaluates the FIRST_VALUE window function
func evalFirstValue(env ExecEnv, ex *FuncCall, partitionRows []Row) (any, error) {
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("FIRST_VALUE requires an argument")
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[0])
}

// evalLastValue evaluates the LAST_VALUE window function
func evalLastValue(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int) (any, error) {
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("LAST_VALUE requires an argument")
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}
	// Use frame end if specified
	endIdx := len(partitionRows) - 1
	if ex.Over.Frame != nil {
		endIdx = calculateFrameEnd(currentIdx, len(partitionRows), ex.Over.Frame)
	}
	return evalExpr(env, ex.Args[0], partitionRows[endIdx])
}

// evalMovingAggregate evaluates MOVING_SUM and MOVING_AVG window functions
func evalMovingAggregate(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	// Get window size from first argument
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("%s requires window size argument", ex.Name)
	}
	sizeVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	var windowSize int
	if sizeInt, ok := sizeVal.(int); ok {
		windowSize = sizeInt
	} else if sizeFloat, ok := sizeVal.(float64); ok {
		windowSize = int(sizeFloat)
	}

	// Calculate start of window
	startIdx := currentIdx - windowSize + 1
	if startIdx < 0 {
		startIdx = 0
	}

	// Get value expression (second argument if provided, otherwise assume column)
	var valueExpr Expr
	if len(ex.Args) > 1 {
		valueExpr = ex.Args[1]
	} else {
		// Use ORDER BY column as default
		if len(ex.Over.OrderBy) > 0 {
			valueExpr = newVarRef(ex.Over.OrderBy[0].Col)
		} else {
			return nil, fmt.Errorf("%s requires value expression", ex.Name)
		}
	}

	// Calculate sum over window
	var sum float64
	count := 0
	for i := startIdx; i <= currentIdx && i < len(partitionRows); i++ {
		val, err := evalExpr(env, valueExpr, partitionRows[i])
		if err != nil {
			return nil, err
		}
		if val != nil {
			if valFloat, ok := val.(float64); ok {
				sum += valFloat
			} else if valInt, ok := val.(int); ok {
				sum += float64(valInt)
			}
			count++
		}
	}

	if ex.Name == "MOVING_SUM" {
		return sum, nil
	}
	// MOVING_AVG
	if count == 0 {
		return nil, nil
	}
	return sum / float64(count), nil
}

// evalWindowFunction evaluates a window function with OVER clause
func evalWindowFunction(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Over == nil {
		return nil, fmt.Errorf("window function %s requires OVER clause", ex.Name)
	}

	// Get all rows for this window
	allRows := env.windowRows
	if allRows == nil {
		return nil, fmt.Errorf("window function context not available")
	}

	// Apply PARTITION BY to get relevant partition
	partitionRows := allRows
	if len(ex.Over.PartitionBy) > 0 {
		partitionRows = filterPartition(env, allRows, ex.Over.PartitionBy, row)
	}

	// Apply ORDER BY to partition
	if len(ex.Over.OrderBy) > 0 {
		partitionRows = sortRows(partitionRows, ex.Over.OrderBy)
	}

	// Find current row position in partition
	currentIdx := findRowIndex(partitionRows, row, env.windowIndex)

	// Evaluate the specific window function
	switch ex.Name {
	case "ROW_NUMBER":
		return currentIdx + 1, nil
	case "RANK":
		return evalRankFunction(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "DENSE_RANK":
		return evalDenseRankFunction(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "PERCENT_RANK":
		return evalPercentRank(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "CUME_DIST":
		return evalCumeDist(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "NTILE":
		return evalNtile(env, ex, partitionRows, currentIdx, row)
	case "LAG":
		return evalLagFunction(env, ex, partitionRows, currentIdx, row)
	case "LEAD":
		return evalLeadFunction(env, ex, partitionRows, currentIdx, row)
	case "FIRST_VALUE":
		return evalFirstValue(env, ex, partitionRows)
	case "LAST_VALUE":
		return evalLastValue(env, ex, partitionRows, currentIdx)
	case "MOVING_SUM", "MOVING_AVG":
		return evalMovingAggregate(env, ex, partitionRows, currentIdx, row)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", ex.Name)
	}
}

// rowsOrderTie reports whether a and b have identical values for every
// ORDER BY column — the sort direction is irrelevant for tie detection,
// only equality matters.
func rowsOrderTie(a, b Row, orderBy []OrderItem) bool {
	for _, oi := range orderBy {
		col := strings.ToLower(oi.Col)
		av, _ := getValLower(a, col)
		bv, _ := getValLower(b, col)
		if compareForOrder(av, bv, false) != 0 {
			return false
		}
	}
	return true
}

// evalRankFunction computes SQL RANK(): tied rows (identical ORDER BY key)
// share the same rank, and the rank after a tie group skips ahead by the
// group's size (e.g. 1, 1, 3, 4, 4, 6). partitionRows must already be
// sorted by orderBy (evalWindowFunction guarantees this).
func evalRankFunction(partitionRows []Row, currentIdx int, orderBy []OrderItem) int {
	if len(orderBy) == 0 {
		return currentIdx + 1
	}
	i := currentIdx
	for i > 0 && rowsOrderTie(partitionRows[i-1], partitionRows[currentIdx], orderBy) {
		i--
	}
	return i + 1
}

// evalDenseRankFunction computes SQL DENSE_RANK(): like RANK but without
// gaps after ties (e.g. 1, 1, 2, 3, 3, 4).
func evalDenseRankFunction(partitionRows []Row, currentIdx int, orderBy []OrderItem) int {
	if len(orderBy) == 0 {
		return currentIdx + 1
	}
	rank := 1
	for i := 1; i <= currentIdx; i++ {
		if !rowsOrderTie(partitionRows[i-1], partitionRows[i], orderBy) {
			rank++
		}
	}
	return rank
}

// evalPercentRank computes SQL PERCENT_RANK(): (RANK - 1) / (partition size - 1),
// or 0 when the partition has only one row.
func evalPercentRank(partitionRows []Row, currentIdx int, orderBy []OrderItem) float64 {
	total := len(partitionRows)
	if total <= 1 {
		return 0
	}
	rank := evalRankFunction(partitionRows, currentIdx, orderBy)
	return float64(rank-1) / float64(total-1)
}

// evalCumeDist computes SQL CUME_DIST(): the fraction of partition rows
// whose ORDER BY key is less than or equal to the current row's key. All
// rows in the same tie group get the same value (the group's last position).
func evalCumeDist(partitionRows []Row, currentIdx int, orderBy []OrderItem) float64 {
	total := len(partitionRows)
	if total == 0 {
		return 0
	}
	i := currentIdx
	for i+1 < total && rowsOrderTie(partitionRows[i+1], partitionRows[currentIdx], orderBy) {
		i++
	}
	return float64(i+1) / float64(total)
}

// evalNtile computes SQL NTILE(n): divides the partition into n
// (approximately) equal-sized buckets and returns the 1-based bucket number
// for the current row. When the partition size doesn't divide evenly, the
// first (size % n) buckets get one extra row each — matching PostgreSQL's
// NTILE bucketing.
func evalNtile(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("NTILE expects 1 argument")
	}
	nVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	n, err := toInt(nVal)
	if err != nil {
		return nil, fmt.Errorf("NTILE: %w", err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("NTILE argument must be positive, got %d", n)
	}
	total := len(partitionRows)
	base := total / n
	remainder := total % n
	boundary := remainder * (base + 1)
	if currentIdx < boundary {
		return currentIdx/(base+1) + 1, nil
	}
	if base == 0 {
		// n exceeds the partition size; every remaining row is its own
		// bucket, continuing on from the (remainder) buckets already used.
		return remainder + (currentIdx - boundary) + 1, nil
	}
	return remainder + (currentIdx-boundary)/base + 1, nil
}

// filterPartition returns rows that match the partition of the current row
func filterPartition(env ExecEnv, allRows []Row, partitionBy []Expr, currentRow Row) []Row {
	// Evaluate partition expressions for current row
	currentPartition := make([]any, len(partitionBy))
	for i, expr := range partitionBy {
		val, err := evalExpr(env, expr, currentRow)
		if err != nil {
			continue
		}
		currentPartition[i] = val
	}

	// Filter rows with same partition values
	var result []Row
	for _, row := range allRows {
		match := true
		for i, expr := range partitionBy {
			val, err := evalExpr(env, expr, row)
			cmp, cmpErr := compare(val, currentPartition[i])
			if err != nil || cmpErr != nil || cmp != 0 {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}
	return result
}

// sortRows sorts rows according to ORDER BY items
// sortRows returns a sorted copy of rows (used for window-function partition
// ordering, which must not mutate the caller's slice). Like applySortOrder,
// sort keys are extracted once per row instead of re-looked-up from the row
// map on every comparator call.
func sortRows(rows []Row, orderBy []OrderItem) []Row {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)
	if len(orderBy) == 0 || len(sorted) <= 1 {
		return sorted
	}
	lcOrdCols := make([]string, len(orderBy))
	for i, oi := range orderBy {
		lcOrdCols[i] = strings.ToLower(oi.Col)
	}
	items := make([]orderedValueRow, len(sorted))
	for i, row := range sorted {
		items[i] = buildOrderByValues(row, lcOrdCols)
	}
	sort.SliceStable(items, func(i, j int) bool {
		return compareOrderedValueRows(orderBy, items[i], items[j]) < 0
	})
	for i, item := range items {
		sorted[i] = item.row
	}
	return sorted
}

// findRowIndex finds the index of the current row in the partition
func findRowIndex(partitionRows []Row, currentRow Row, hint int) int {
	// Try hint first (optimization)
	if hint >= 0 && hint < len(partitionRows) {
		if rowsEqual(partitionRows[hint], currentRow) {
			return hint
		}
	}

	// Linear search
	for i, row := range partitionRows {
		if rowsEqual(row, currentRow) {
			return i
		}
	}
	return 0
}

// rowsEqual checks if two rows are equal (same values for all columns)
func rowsEqual(a, b Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		cmp, err := compare(v, b[k])
		if err != nil || cmp != 0 {
			return false
		}
	}
	return true
}

// calculateFrameEnd calculates the end index for frame-based window functions
func calculateFrameEnd(currentIdx, totalRows int, frame *WindowFrame) int {
	if frame == nil {
		return currentIdx // Default: CURRENT ROW
	}

	switch frame.EndType {
	case "CURRENT":
		return currentIdx
	case "UNBOUNDED_FOLLOWING":
		return totalRows - 1
	case "OFFSET_FOLLOWING":
		endIdx := currentIdx + frame.EndValue
		if endIdx >= totalRows {
			return totalRows - 1
		}
		return endIdx
	case "OFFSET_PRECEDING":
		endIdx := currentIdx - frame.EndValue
		if endIdx < 0 {
			return 0
		}
		return endIdx
	default:
		return currentIdx
	}
}
