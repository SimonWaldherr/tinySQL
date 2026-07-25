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
	"sync"
	"time"

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
	// triggerDepth is incremented for nested trigger bodies. It is deliberately
	// part of the value-style execution environment so child statements inherit
	// the current depth without any process-global state.
	triggerDepth int
	// statementWAL is shared by nested DML (for example trigger bodies) so
	// AdvancedWAL emits a single commit only after the outer statement has
	// completed successfully.
	statementWAL *statementWAL
	// foreignKeyActions tracks the active referential-action path. Nested
	// ON UPDATE/ON DELETE cascades share it so a cyclic reference fails
	// cleanly instead of recursing until stack exhaustion.
	foreignKeyActions *foreignKeyActionState
	// now is the evaluation timestamp for this statement, set once when the
	// statement begins executing. RECENCY_SCORE/RAG_HYBRID_SCORE/RAG_RANK_SCORE
	// default to it (via envNow) instead of calling time.Now() per row, so
	// every row of one query — and every RAG scorer call within it — sees the
	// same "now" and ranks consistently. The zero value falls back to
	// time.Now() (see envNow), which is what ExecEnv{} in tests gets.
	now time.Time
}

// envNow returns the statement's evaluation timestamp, falling back to
// time.Now() when env.now is unset (e.g. ExecEnv{} in unit tests, or any
// evaluation path outside executeStatement's per-statement clock).
func envNow(env ExecEnv) time.Time {
	if env.now.IsZero() {
		return time.Now()
	}
	return env.now
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
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultSet, error) {
	return executeStatement(ctx, db, tenant, stmt)
}
