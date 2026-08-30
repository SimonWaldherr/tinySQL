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
	// allFuncTable holds the same handlers as allFunctions, addressed by the
	// 1-based index allFuncIndex maps each name to. Parsed FuncCall nodes
	// store that index so evaluation needs no map lookup — see
	// FuncCall.handlerIdx for why an index rather than the func value.
	allFuncTable []funcHandler
	allFuncIndex map[string]int32
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
		for k, v := range getGeoSimplifyFunctions() {
			m[k] = v
		}
		for k, v := range getGeoEditingFunctions() {
			m[k] = v
		}
		for k, v := range getGeoRelateFunctions() {
			m[k] = v
		}
		for k, v := range getGeoClipFunctions() {
			m[k] = v
		}
		for k, v := range getGeoWKTFunctions() {
			m[k] = v
		}
		for k, v := range getGeoWKBFunctions() {
			m[k] = v
		}
		for k, v := range getGeoHashFunctions() {
			m[k] = v
		}
		for k, v := range getGeoTransformFunctions() {
			m[k] = v
		}
		for k, v := range getGeoExtraRelateFunctions() {
			m[k] = v
		}
		for k, v := range getRouteFunctions() {
			m[k] = v
		}
		for k, v := range getMBTilesFunctions() {
			m[k] = v
		}
		for k, v := range getTileFunctions() {
			m[k] = v
		}
		for k, v := range getCRSFunctions() {
			m[k] = v
		}
		for k, v := range getGeoPackageFunctions() {
			m[k] = v
		}
		allFunctions = m
		allFuncTable = make([]funcHandler, 0, len(m))
		allFuncIndex = make(map[string]int32, len(m))
		for name, h := range m {
			allFuncTable = append(allFuncTable, h)
			allFuncIndex[name] = int32(len(allFuncTable))
		}
	})
	return allFunctions
}

// funcHandlerTable and funcHandlerIndex expose the flattened registry, both
// initialized by the same sync.Once as allFunctions.
func funcHandlerTable() []funcHandler {
	getAllFunctions()
	return allFuncTable
}

func funcHandlerIndex() map[string]int32 {
	getAllFunctions()
	return allFuncIndex
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
	// cteCacheable marks ResultSets materialized by a non-recursive CTE.
	// They are immutable for the lifetime of one statement, so the converted
	// row maps used by FROM/JOIN can safely be shared by repeated references.
	cteCacheable bool
}

// cteRowCache keeps the row-map representation of a materialized CTE scoped
// to one statement execution. The cache must not outlive that execution: its
// values deliberately contain the CTE's current rows, rather than a copy that
// could be reused after the underlying tables have changed.
type cteRowCache struct {
	mu      sync.RWMutex
	entries map[cteRowCacheKey][]Row
}

type cteRowCacheKey struct {
	result    *ResultSet
	cteName   string
	qualifier string
}

func newCTERowCache() *cteRowCache {
	return &cteRowCache{}
}

func (c *cteRowCache) load(key cteRowCacheKey) ([]Row, bool) {
	c.mu.RLock()
	rows, ok := c.entries[key]
	c.mu.RUnlock()
	return rows, ok
}

func (c *cteRowCache) store(key cteRowCacheKey, rows []Row) []Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[key]; ok {
		return existing
	}
	if c.entries == nil {
		c.entries = make(map[cteRowCacheKey][]Row)
	}
	c.entries[key] = rows
	return rows
}

type ExecEnv struct {
	ctx         context.Context
	tenant      string
	db          *storage.DB
	ctes        map[string]*ResultSet // For CTE support
	cteRowCache *cteRowCache          // materialized CTE source rows, per statement
	windowRows  []Row                 // All rows for window function context
	windowIndex int                   // Current row index in window context
	// windowPartitions memoizes, per structural PARTITION BY/ORDER BY shape
	// and PARTITION BY key, the partitioned+ordered row set built from
	// windowRows -- see windowPartitionCache in eval_window.go for why.
	// Initialized alongside windowRows (exec_group.go) whenever a query uses
	// window functions; nil otherwise.
	windowPartitions *windowPartitionCache
	// subqueryCache memoizes the result of executing a WHERE/SELECT-list
	// EXISTS/scalar/IN subquery's SELECT, keyed by the owning AST node, so a
	// provably uncorrelated subquery inside e.g. a WHERE clause executes once
	// per statement instead of once per outer row -- see evalCachedSubquery
	// (eval_subquery_cache.go) and isSelectCorrelated for the safety check
	// gating which nodes are eligible. Set once per top-level Execute() call
	// (exec_statement.go), mirroring windowPartitions: never reused across
	// separate executions, even of the same cached/compiled statement.
	subqueryCache *subqueryResultCache
	viewDepth     int
	// triggerRow binds new.<col>/old.<col>/bare-col pseudo-columns while
	// executing a trigger body statement (see executeTrigger and
	// triggerRowBinding in triggers.go), so NEW.col/OLD.col resolve even
	// though the body statement's own row context (e.g. an INSERT's VALUES
	// row) has no such columns. It resolves directly against the newRow/
	// oldRow maps the firing DML statement already built (for its own
	// WHERE/RETURNING evaluation), rather than a third map merging copies of
	// both under renamed keys.
	triggerRow *triggerRowBinding
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
	// dml carries the planning work executeStatement already performed to pick
	// this statement's rollback snapshot, so the DML handler that runs
	// immediately afterwards does not repeat it. See dmlPlan; nil means
	// "nothing precomputed", which every consumer handles.
	dml *dmlPlan
	// procedureOverride is the storedProcedure executeStatement already
	// resolved for the exact top-level *CallProcedure this env was built for,
	// so execStmt/executeCallProcedure reuse that one lookup instead of each
	// independently re-reading the live, hot-swappable procedure registry --
	// which could otherwise observe a concurrent RegisterStoredProcedureWithOptions
	// mid-statement and dispatch under a lock/rollback decision that no longer
	// matches the handler actually invoked. Always nil before it reaches a
	// nested statement (ProcedureContext.Execute strips it): it is only valid
	// for the exact statement it was resolved for, never for whatever a
	// procedure handler executes next.
	procedureOverride *storedProcedure
	// rollbackArmed is true once an ancestor statement in this call chain
	// already holds an active rollback snapshot (see ProcedureContext.Execute).
	// db.SnapshotForStatement's catalog half installs at most one rollback
	// point at a time -- see armCatalogRollback -- so a nested statement must
	// never arm a second one while an outer one is still active: doing so
	// would silently steal catalog-mutation capture away from the outer
	// snapshot instead of adding coverage.
	rollbackArmed bool
}

// planFor returns the precomputed plan for stmt, or nil when this environment
// has none. The identity check is the safety mechanism: nested DML (a trigger
// body, a foreign-key cascade) is dispatched through execStmt carrying the
// *outer* statement's environment, so a plan is only usable by the exact
// statement it was built for.
func (env ExecEnv) planFor(stmt Statement) *dmlPlan {
	if env.dml == nil || env.dml.stmt != stmt {
		return nil
	}
	return env.dml
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
