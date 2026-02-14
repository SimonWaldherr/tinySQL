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
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// allFunctions is a lazily-initialised, read-only function registry that
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
}

// Execute runs a parsed SQL statement against the given storage DB and tenant.
// It dispatches to handlers per statement kind and returns a ResultSet for
// SELECT (nil for DDL/DML). The context is checked at safe points to support
// cancellation.
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultSet, error) {
	env := ExecEnv{ctx: ctx, tenant: tenant, db: db}
	switch s := stmt.(type) {
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
	case *AlterTable:
		return executeAlterTable(env, s)
	case *Insert:
		return executeInsert(env, s)
	case *Update:
		return executeUpdate(env, s)
	case *Delete:
		return executeDelete(env, s)
	case *Select:
		return executeSelect(env, s)
	case *CreateJob:
		return executeCreateJob(env, s)
	case *AlterJob:
		return executeAlterJob(env, s)
	case *DropJob:
		return executeDropJob(env, s)
	}
	return nil, fmt.Errorf("unknown statement")
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

	if s.AsSelect == nil {
		t := storage.NewTable(s.Name, s.Cols, s.IsTemp)
		err := env.db.Put(env.tenant, t)
		return nil, err
	}
	rs, err := Execute(env.ctx, env.db, env.tenant, s.AsSelect)
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
	if s.IfExists {
		_, err := env.db.Get(env.tenant, s.Name)
		if err != nil {
			// Table doesn't exist, silently succeed
			return nil, nil
		}
	}
	return nil, env.db.Drop(env.tenant, s.Name)
}

func executeCreateIndex(env ExecEnv, s *CreateIndex) (*ResultSet, error) {
	// For now, indexes are a no-op since tinySQL doesn't persist them
	// In a real implementation, this would store index metadata
	if s.IfNotExists {
		// Could check if index already exists
	}
	// Log or store index information if needed
	return nil, nil
}

func executeDropIndex(env ExecEnv, s *DropIndex) (*ResultSet, error) {
	// For now, indexes are a no-op since tinySQL doesn't persist them
	if s.IfExists {
		// Could check if index exists
	}
	return nil, nil
}

func executeCreateView(env ExecEnv, s *CreateView) (*ResultSet, error) {
	// Views are not currently stored in tinySQL
	// In a full implementation, we would store the SELECT statement
	// and execute it when the view is queried
	if s.IfNotExists && !s.OrReplace {
		// Could check if view already exists
	}
	// For now, return success
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
	if err := env.db.Catalog().RegisterJob(job); err != nil {
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
	if err := env.db.Catalog().RegisterJob(job); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropJob(env ExecEnv, s *DropJob) (*ResultSet, error) {
	if err := env.db.Catalog().DeleteJob(s.Name); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropView(env ExecEnv, s *DropView) (*ResultSet, error) {
	// Views are not currently stored in tinySQL
	if s.IfExists {
		// Could check if view exists
	}
	return nil, nil
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
		env.db.Put(env.tenant, t)
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
			cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
			}
			row[i] = cv
		}
		t.Rows = append(t.Rows, row)
	}
	t.Version++
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
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
			cv, err := coerceToTypeAllowNull(v, t.Cols[idx].Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
			}
			row[idx] = cv
		}
		t.Rows = append(t.Rows, row)
	}
	t.Version++
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
	return nil, nil
}

func executeUpdate(env ExecEnv, s *Update) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
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
	for ri, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, c.Name, r[i])
			putVal(row, s.Table+"."+c.Name, r[i])
		}
		ok := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			ok = (toTri(v) == tvTrue)
		}
		if ok {
			for i, ex := range setIdx {
				v, err := evalExpr(env, ex, row)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
				if err != nil {
					return nil, err
				}
				t.Rows[ri][i] = cv
			}
			n++
		}
	}
	t.Version++
	if n > 0 {
		t.MarkDirtyFrom(-1) // UPDATE is non-append; force full-table WAL
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
}

func executeDelete(env ExecEnv, s *Delete) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	var kept [][]any
	del := 0
	for _, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, c.Name, r[i])
			putVal(row, s.Table+"."+c.Name, r[i])
		}
		keep := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			if toTri(v) == tvTrue {
				keep = false
			}
		}
		if keep {
			kept = append(kept, r)
		} else {
			del++
		}
	}
	t.Rows = kept
	t.Version++
	if del > 0 {
		t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
	}
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
}

//nolint:gocyclo // SELECT execution handles joins, CTEs, filtering, projection, and aggregation.
func executeSelect(env ExecEnv, s *Select) (*ResultSet, error) {
	// Process CTEs first
	cteEnv, err := processCTEs(env, s)
	if err != nil {
		return nil, err
	}

	// FROM (Tabelle, CTE oder Subselect) - now optional
	var leftRows []Row

	// Check if FROM clause exists
	if s.From.Table == "" && s.From.Subquery == nil && s.From.TableFunc == nil {
		// No FROM clause - create a single dummy row for expression evaluation
		// This allows SELECT NOW(), SELECT 1+1, etc.
		leftRows = []Row{make(Row)}
	} else if s.From.Subquery != nil {
		// FROM (SELECT ...) AS alias
		subResult, err := executeSelect(env, s.From.Subquery)
		if err != nil {
			return nil, err
		}
		leftRows = make([]Row, len(subResult.Rows))
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
	} else if s.From.TableFunc != nil {
		// FROM table-valued function
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
		leftRows = make([]Row, len(rs.Rows))
		for i, row := range rs.Rows {
			leftRows[i] = make(Row)
			for k, v := range row {
				leftRows[i][strings.ToLower(k)] = v
				if s.From.Alias != "" {
					leftRows[i][strings.ToLower(s.From.Alias+"."+k)] = v
				}
			}
		}
	} else {
		// No subquery and no table function: this can be a CTE name, a virtual
		// catalog table (catalog.*), or a regular table. Prefer CTE binding first.
		if cteEnv.ctes != nil {
			if cteResult, exists := cteEnv.ctes[s.From.Table]; exists {
				leftRows = make([]Row, len(cteResult.Rows))
				for i, row := range cteResult.Rows {
					leftRows[i] = make(Row)
					for k, v := range row {
						leftRows[i][k] = v
						leftRows[i][s.From.Table+"."+k] = v
					}
				}
			}
		}

		// Handle virtual catalog.* tables regardless of CTE presence
		if leftRows == nil && strings.HasPrefix(strings.ToLower(s.From.Table), "catalog.") {
			parts := strings.SplitN(s.From.Table, ".", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid catalog reference: %s", s.From.Table)
			}
			name := strings.ToLower(parts[1])
			switch name {
			case "tables":
				// Auto-populate from real tables, then merge catalog-only entries.
				leftRows = sysTablesRows(env)
				catTabs := env.db.Catalog().GetTables()
				catMap := make(map[string]*storage.CatalogTable, len(catTabs))
				for _, ct := range catTabs {
					catMap[strings.ToLower(ct.Name)] = ct
				}
				// Track which real tables we've seen.
				seen := make(map[string]bool, len(leftRows))
				for _, r := range leftRows {
					tName, _ := r["name"].(string)
					seen[strings.ToLower(tName)] = true
					if ct, ok := catMap[strings.ToLower(tName)]; ok {
						putVal(r, "schema", ct.Schema)
						putVal(r, "type", ct.Type)
						putVal(r, "row_count", ct.RowCount)
						putVal(r, "created_at", ct.CreatedAt)
						putVal(r, "updated_at", ct.UpdatedAt)
					} else {
						putVal(r, "schema", "main")
						putVal(r, "type", "TABLE")
					}
				}
				// Add catalog-only entries that aren't real tables yet.
				for _, ct := range catTabs {
					if seen[strings.ToLower(ct.Name)] {
						continue
					}
					r := make(Row)
					putVal(r, "schema", ct.Schema)
					putVal(r, "name", ct.Name)
					putVal(r, "type", ct.Type)
					putVal(r, "row_count", ct.RowCount)
					putVal(r, "created_at", ct.CreatedAt)
					putVal(r, "updated_at", ct.UpdatedAt)
					if s.From.Alias != "" {
						putVal(r, s.From.Alias+".schema", ct.Schema)
						putVal(r, s.From.Alias+".name", ct.Name)
					}
					leftRows = append(leftRows, r)
				}
			case "columns":
				cols := env.db.Catalog().GetAllColumns()
				leftRows = make([]Row, len(cols))
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
			case "functions":
				// Auto-populate from real function registry, then overlay catalog entries.
				leftRows = sysFunctionsRows()
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
			case "jobs":
				jobs := env.db.Catalog().ListJobs()
				leftRows = make([]Row, len(jobs))
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
					putVal(leftRows[i], "last_run_at", j.LastRunAt)
					putVal(leftRows[i], "next_run_at", j.NextRunAt)
				}
			case "views":
				views := env.db.Catalog().GetViews()
				leftRows = make([]Row, len(views))
				for i, v := range views {
					leftRows[i] = make(Row)
					putVal(leftRows[i], "schema", v.Schema)
					putVal(leftRows[i], "name", v.Name)
					putVal(leftRows[i], "sql_text", v.SQLText)
					putVal(leftRows[i], "created_at", v.CreatedAt)
				}
			default:
				return nil, fmt.Errorf("unknown catalog table: %s", name)
			}
		}

		// Handle virtual sys.* tables
		if leftRows == nil && strings.HasPrefix(strings.ToLower(s.From.Table), "sys.") {
			parts := strings.SplitN(s.From.Table, ".", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid sys reference: %s", s.From.Table)
			}
			name := strings.ToLower(parts[1])
			sysRows, err := resolveSysTable(env, name)
			if err != nil {
				return nil, err
			}
			leftRows = sysRows
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
		}

		// If still not resolved, treat as a regular table (prefer cteEnv.db when available)
		if leftRows == nil {
			var leftT *storage.Table
			var err error
			if cteEnv.ctes != nil {
				leftT, err = cteEnv.db.Get(cteEnv.tenant, s.From.Table)
			} else {
				leftT, err = env.db.Get(env.tenant, s.From.Table)
			}
			if err != nil {
				return nil, err
			}
			leftRows, _ = rowsFromTable(leftT, aliasOr(s.From))
		}
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
			// If ORDER BY present, sort now so the first row per key is the right one
			if len(s.OrderBy) > 0 {
				lcOrdCols := make([]string, len(s.OrderBy))
				for idx, oi := range s.OrderBy {
					lcOrdCols[idx] = strings.ToLower(oi.Col)
				}
				sort.SliceStable(outRows, func(i, j int) bool {
					a := outRows[i]
					b := outRows[j]
					for k, oi := range s.OrderBy {
						av := a[lcOrdCols[k]]
						bv := b[lcOrdCols[k]]
						cmp := compareForOrder(av, bv, oi.Desc)
						if cmp == 0 {
							continue
						}
						if oi.Desc {
							return cmp > 0
						}
						return cmp < 0
					}
					return false
				})
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
			outRows = res
		} else {
			outRows = distinctRows(outRows, outCols)
		}
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		lcOrdCols := make([]string, len(s.OrderBy))
		for idx, oi := range s.OrderBy {
			lcOrdCols[idx] = strings.ToLower(oi.Col)
		}
		sort.SliceStable(outRows, func(i, j int) bool {
			a := outRows[i]
			b := outRows[j]
			for k, oi := range s.OrderBy {
				av := a[lcOrdCols[k]]
				bv := b[lcOrdCols[k]]
				cmp := compareForOrder(av, bv, oi.Desc)
				if cmp == 0 {
					continue
				}
				if oi.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
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
		}
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func processInnerJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	// Use hash join optimization for large datasets
	if len(leftRows) > 50 || len(rightRows) > 50 {
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
	if len(leftRows) > 50 || len(rightRows) > 50 {
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
	needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)

	if needAgg {
		return processAggregateQuery(env, s, filtered)
	}
	return processNonAggregateQuery(env, s, filtered)
}

//nolint:gocyclo // Aggregation flow must cover grouping, HAVING, and projection variants.
func processAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	groups := make(map[string][]Row, len(filtered)/2) // Estimate group count
	orderKeys := make([]string, 0, len(filtered)/2)
	outRows := make([]Row, 0, len(filtered)/2)
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))

	for _, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}
		var parts []string
		for _, g := range s.GroupBy {
			v, err := evalExpr(env, g, r)
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, fmtKeyPart(v))
		}
		ks := strings.Join(parts, "\x1f")
		if _, ok := groups[ks]; !ok {
			orderKeys = append(orderKeys, ks)
		}
		groups[ks] = append(groups[ks], r)
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
			} else {
				val, err = evalExpr(env, it.Expr, rows[0])
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

		newAdded := 0
		targetCols := nextRs.Cols
		if accRs != nil && accRs.Cols != nil {
			targetCols = accRs.Cols
		}

		var alignedRows []Row
		if accRs != nil && accRs.Cols != nil && nextRs != nil && nextRs.Cols != nil && len(nextRs.Cols) == len(accRs.Cols) {
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
					nr[cte.Name+"."+tgt] = val
				}
				alignedRows = append(alignedRows, nr)
			}
		} else {
			alignedRows = nextRs.Rows
		}

		for _, r := range alignedRows {
			sig := rowSignature(r, targetCols)
			if !seen[sig] {
				seen[sig] = true
				accRows = append(accRows, r)
				newAdded++
			}
		}
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
func putVal(row Row, key string, val any)     { row[strings.ToLower(key)] = val }
func isNull(v any) bool                       { return v == nil }
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

func compareInt(ax int, b any) (int, error) {
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
		return evalVarRef(ex, row)
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
	case *CaseExpr:
		return evalCaseExpr(env, ex, row)
	case *SubqueryExpr:
		return evalSubqueryExpr(env, ex)
	}
	return nil, fmt.Errorf("unknown expression")
}

func evalVarRef(ex *VarRef, row Row) (any, error) {
	if v, ok := getVal(row, ex.Name); ok {
		return v, nil
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

	// Convert to strings
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	pattern, ok := patternVal.(string)
	if !ok {
		pattern = fmt.Sprintf("%v", patternVal)
	}

	// Get escape character if specified
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

	// Match pattern
	matched := matchLikePattern(str, pattern, escapeChar)

	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// matchLikePattern matches a string against a SQL LIKE pattern
// % matches zero or more characters
// _ matches exactly one character
func matchLikePattern(str, pattern string, escape rune) bool {
	sIdx, pIdx := 0, 0
	sLen, pLen := len(str), len(pattern)
	star := -1
	match := 0

	for sIdx < sLen {
		if pIdx < pLen {
			pChar := rune(pattern[pIdx])

			// Check for escape character
			if pChar == escape && pIdx+1 < pLen {
				// Next character is literal
				pIdx++
				if sIdx < sLen && str[sIdx] == pattern[pIdx] {
					sIdx++
					pIdx++
					continue
				}
				return false
			}

			// Handle wildcard characters
			if pChar == '%' {
				star = pIdx
				match = sIdx
				pIdx++
				continue
			}
			if pChar == '_' || str[sIdx] == pattern[pIdx] {
				sIdx++
				pIdx++
				continue
			}
		}

		// No match, backtrack to last %
		if star != -1 {
			pIdx = star + 1
			match++
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
		if v, ok := row[strings.ToLower(rs.Cols[0])]; ok {
			return v, nil
		}
	}
	if len(row) == 1 {
		for _, v := range row {
			return v, nil
		}
	}
	for _, col := range rs.Cols {
		if v, ok := row[strings.ToLower(col)]; ok {
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

func parseTimeValue(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as time")
	}

	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try various time formats
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04",
			"2006-01-02",
			"15:04:05",
			"15:04",
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
func evalLTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("LTRIM expects 1 or 2 arguments")
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
		return nil, fmt.Errorf("LTRIM expects a string argument")
	}

	// Default: trim whitespace
	cutset := " \t\n\r"

	// If second argument provided, use as cutset
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("LTRIM cutset must be a string")
			}
		}
	}

	return strings.TrimLeft(str, cutset), nil
}

func evalRTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("RTRIM expects 1 or 2 arguments")
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
		return nil, fmt.Errorf("RTRIM expects a string argument")
	}

	// Default: trim whitespace
	cutset := " \t\n\r"

	// If second argument provided, use as cutset
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("RTRIM cutset must be a string")
			}
		}
	}

	return strings.TrimRight(str, cutset), nil
}

func evalTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TRIM expects 1 or 2 arguments")
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
		return nil, fmt.Errorf("TRIM expects a string argument")
	}

	// Default: trim whitespace (use TrimSpace for compatibility)
	if len(args) == 1 {
		return strings.TrimSpace(str), nil
	}

	// If second argument provided, use as cutset for both sides
	cutsetVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if cutsetVal == nil {
		return strings.TrimSpace(str), nil
	}

	cutsetStr, ok := cutsetVal.(string)
	if !ok {
		return nil, fmt.Errorf("TRIM cutset must be a string")
	}

	return strings.Trim(str, cutsetStr), nil
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
	sum := 0.0
	n := 0
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if f, ok := numeric(v); ok {
			sum += f
			n++
		}
	}
	if ex.Name == "SUM" {
		return sum, nil
	}
	if n == 0 {
		return nil, nil
	}
	return sum / float64(n), nil
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

	// Pre-compute which unqualified names are unique (no duplicates).
	unqualSeen := make(map[string]bool, numCols)
	for _, k := range unqualKeys {
		unqualSeen[k] = true
	}
	// Total keys per row: qualified + unique unqualified.
	keysPerRow := numCols + len(unqualSeen)

	out := make([]Row, len(t.Rows))
	for ri, r := range t.Rows {
		row := make(Row, keysPerRow)
		for i := range t.Cols {
			v := r[i]
			row[qualKeys[i]] = v
		}
		for i := range t.Cols {
			uq := unqualKeys[i]
			if _, exists := row[uq]; !exists {
				row[uq] = r[i]
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
		byt, _ := json.Marshal(x)
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
	default:
		return v, nil
	}
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
	seen := make(map[string]bool, len(rows[0]))
	cols := make([]string, 0, len(rows[0]))
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
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

	case "LAG":
		offset := 1
		if len(ex.Args) > 1 {
			offsetVal, err := evalExpr(env, ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			if offsetInt, ok := offsetVal.(int); ok {
				offset = offsetInt
			} else if offsetFloat, ok := offsetVal.(float64); ok {
				offset = int(offsetFloat)
			}
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

	case "LEAD":
		offset := 1
		if len(ex.Args) > 1 {
			offsetVal, err := evalExpr(env, ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			if offsetInt, ok := offsetVal.(int); ok {
				offset = offsetInt
			} else if offsetFloat, ok := offsetVal.(float64); ok {
				offset = int(offsetFloat)
			}
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

	case "FIRST_VALUE":
		if len(ex.Args) == 0 {
			return nil, fmt.Errorf("FIRST_VALUE requires an argument")
		}
		if len(partitionRows) == 0 {
			return nil, nil
		}
		return evalExpr(env, ex.Args[0], partitionRows[0])

	case "LAST_VALUE":
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

	case "MOVING_SUM", "MOVING_AVG":
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
				valueExpr = &VarRef{Name: ex.Over.OrderBy[0].Col}
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

	default:
		return nil, fmt.Errorf("unsupported window function: %s", ex.Name)
	}
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
func sortRows(rows []Row, orderBy []OrderItem) []Row {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)

	sort.SliceStable(sorted, func(i, j int) bool {
		a := sorted[i]
		b := sorted[j]
		for _, oi := range orderBy {
			av, _ := getVal(a, oi.Col)
			bv, _ := getVal(b, oi.Col)
			cmp := compareForOrder(av, bv, oi.Desc)
			if cmp == 0 {
				continue
			}
			if oi.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

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
