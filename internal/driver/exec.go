// Executing statements on a connection, and the parsed-statement cache that
// avoids re-parsing repeated query text. Also the SQL-level transaction control
// (BEGIN/COMMIT/ROLLBACK issued as statements rather than through BeginTx).
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// database/sql routes db.Exec/tx.Exec straight here whenever the driver
	// implements driver.ExecerContext (conn does) -- an explicit db.Prepare
	// is never involved, so stmt.go's prepared/pooled AST reuse never
	// engages for this extremely common calling pattern (an application
	// re-issuing the same parameterized INSERT/UPDATE/DELETE text with new
	// bound values on every call, e.g. a batch-insert loop). execPreparedFor
	// gives that same shape the same benefit: skip bindPlaceholders' text
	// render (which hex-encodes every []byte argument into a SQL literal)
	// and the full lex/parse pass that would otherwise follow, per call.
	if pq, _ := execPreparedFor(query); pq != nil && len(args) == len(pq.markers) {
		if exec, err := pq.acquire(); err == nil {
			defer pq.release(exec)
			for i, arg := range args {
				exec.params[i].Val = driverValueLiteral(arg.Value)
			}
			return c.execStatement(ctx, exec.statement)
		}
	}
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.execSQL(ctx, sqlStr)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Mirrors ExecContext above for db.Query/tx.Query, but only takes the
	// cached-AST path for a plain SELECT: querySQL's text path additionally
	// knows how to run a DML statement with a RETURNING clause under the
	// write/persistence path instead of a bare reader lock (see
	// isWriteStatement/executeWriteStatement in query.go), and duplicating
	// that branching here is not worth the risk for what would be an unusual
	// way to call RETURNING. A cached INSERT/UPDATE/DELETE AST (built by an
	// ExecContext call reusing the same text) is simply not reused here;
	// queryPreparedAdHoc reports ok=false and this falls back to the
	// existing text path.
	if pq, _ := execPreparedFor(query); pq != nil && len(args) == len(pq.markers) {
		if rows, ok, err := c.queryPreparedAdHoc(ctx, pq, args); ok {
			return rows, err
		}
	}
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.querySQL(ctx, sqlStr)
}

// queryPreparedAdHoc runs a cached prepared-AST SELECT for QueryContext's
// ad-hoc (no explicit db.Prepare) callers. ok is false -- with no error worth
// reporting -- when pq's cached AST is not a plain SELECT, or acquiring an
// execution failed; the caller falls back to the text-binding path.
func (c *conn) queryPreparedAdHoc(ctx context.Context, pq *preparedQuery, args []driver.NamedValue) (driver.Rows, bool, error) {
	exec, err := pq.acquire()
	if err != nil {
		return nil, false, nil
	}
	if _, ok := exec.statement.(*engine.Select); !ok {
		pq.release(exec)
		return nil, false, nil
	}
	defer pq.release(exec)
	for i, arg := range args {
		exec.params[i].Val = driverValueLiteral(arg.Value)
	}
	rows, err := c.queryStatement(ctx, exec.statement)
	return rows, true, err
}

// execPreparedCache is a bounded, process-wide cache of preparedQuery
// templates for ad-hoc DML/SELECT issued via ExecContext/QueryContext
// (i.e. without an explicit db.Prepare call). Keyed by the raw, unbound SQL
// text -- the point is to look this up *before* paying bindPlaceholders'
// text-render cost, unlike parsedStmtCache below which is keyed by the
// already-substituted text.
//
// A *preparedQuery's AST references tables/columns only by name (see
// engine.Insert/Update/Delete/Select) and resolves them fresh against
// whichever *storage.DB and tenant the caller's engine.Execute/queryStatement/
// execStatement call supplies -- it carries no assumption about which
// database it runs against, the same way parsedStmtCache's shared SELECT ASTs
// already don't. Sharing one process-wide, so the same INSERT text reused
// against a different connection (or a different tinySQL database entirely)
// still gets a cache hit; acquire()/release()'s existing pool protocol is
// what already makes concurrent reuse of one preparedQuery race-free for
// db.Prepare-based callers, and that protocol does not care who called it.
var (
	execPreparedMu    sync.RWMutex
	execPreparedCache = make(map[string]*preparedQuery)
)

const (
	execPreparedCacheMaxEntries = 256
	execPreparedCacheMaxSQLLen  = 8 << 10
)

// execPreparedFor returns a cached preparedQuery for sqlStr, building and
// caching one on a cold miss. It returns a nil *preparedQuery -- never an
// error worth surfacing -- when sqlStr is too long to cache, uses numbered
// ($1/:1) rather than positional (?) placeholders, has no placeholders at
// all, or is not a SELECT/INSERT/UPDATE/DELETE: callers fall back to the
// text-binding path unchanged in every such case, which remains the sole
// source of truth for a genuine syntax error (buildPreparedQuery's own
// error, e.g. "prepared fast path supports ... only", is deliberately
// swallowed here rather than surfaced, so a shape this cache does not serve
// still gets the same error text it always did).
func execPreparedFor(sqlStr string) (*preparedQuery, error) {
	if len(sqlStr) > execPreparedCacheMaxSQLLen {
		return nil, nil
	}
	execPreparedMu.RLock()
	pq := execPreparedCache[sqlStr]
	execPreparedMu.RUnlock()
	if pq != nil {
		return pq, nil
	}
	built, err := buildPreparedQuery(sqlStr)
	if err != nil || built == nil {
		return nil, nil
	}
	execPreparedMu.Lock()
	if existing := execPreparedCache[sqlStr]; existing != nil {
		built = existing
	} else {
		if len(execPreparedCache) >= execPreparedCacheMaxEntries {
			// Random eviction via map iteration order, matching
			// parsedStmtCache: a bad eviction just costs one rebuild.
			for k := range execPreparedCache {
				if len(execPreparedCache) < execPreparedCacheMaxEntries {
					break
				}
				delete(execPreparedCache, k)
			}
		}
		execPreparedCache[sqlStr] = built
	}
	execPreparedMu.Unlock()
	return built, nil
}

// Non-context fallbacks
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.ExecContext(context.Background(), query, n)
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.QueryContext(context.Background(), query, n)
}

func (c *conn) currentDB() *storage.DB {
	if c.inTx && c.shadow != nil {
		return c.shadow
	}
	return c.srv.db
}

// parsedStmtCache is a bounded, process-wide cache of parsed statements
// keyed by the final (post-placeholder-binding) SQL text. Applications going
// through database/sql re-issue the same statement text on every call —
// dashboards, health checks, catalog queries, RAG query templates — and
// previously paid a full lex+parse each time. Only SELECT/EXPLAIN results
// are cached: they are the shapes that repeat, and read statements are
// naturally re-executed from a shared AST elsewhere (the public
// ParseSQL-once/Execute-many pattern), so cross-connection reuse is safe.
// Oversized statements (bulk INSERTs, inlined vector literals — unique per
// call) are parsed directly and never stored, so they cannot churn the
// cache.
var (
	parsedStmtMu       sync.RWMutex
	parsedStmtCache    = make(map[string]engine.Statement)
	parsedStmtInFlight = make(map[string]*parsedStmtCall)
)

// parsedStmtCall is a small, channel-based singleflight for cold parse-cache
// misses. It deliberately lives beside the cache instead of adding another
// dependency: the leader parses once, concurrent readers wait without holding
// a mutex, and every waiter receives the same immutable SELECT/EXPLAIN AST.
// DML is never shared this way because it can carry connection-local state.
type parsedStmtCall struct {
	done   chan struct{}
	stmt   engine.Statement
	err    error
	shared bool
}

const (
	parsedStmtCacheMaxEntries = 256
	parsedStmtCacheMaxSQLLen  = 8 << 10
)

func parseSQLCached(sqlStr string) (engine.Statement, error) {
	cacheable := len(sqlStr) <= parsedStmtCacheMaxSQLLen && parseCacheCandidate(sqlStr)
	if !cacheable {
		return engine.NewParser(sqlStr).ParseStatement()
	}

	parsedStmtMu.RLock()
	st, ok := parsedStmtCache[sqlStr]
	parsedStmtMu.RUnlock()
	if ok {
		return st, nil
	}

	// Register a leader while holding the short cache mutex. Waiters release
	// it before blocking, so a cold burst neither serializes readers nor causes
	// N identical lex/parse passes.
	parsedStmtMu.Lock()
	if st, ok := parsedStmtCache[sqlStr]; ok {
		parsedStmtMu.Unlock()
		return st, nil
	}
	if call := parsedStmtInFlight[sqlStr]; call != nil {
		parsedStmtMu.Unlock()
		<-call.done
		if call.err != nil {
			return nil, call.err
		}
		if call.shared {
			return call.stmt, nil
		}
		// A malformed read-shaped statement is not shared. This branch is
		// defensive: cache candidates are SELECT/EXPLAIN only.
		return engine.NewParser(sqlStr).ParseStatement()
	}
	call := &parsedStmtCall{done: make(chan struct{})}
	parsedStmtInFlight[sqlStr] = call
	parsedStmtMu.Unlock()

	p := engine.NewParser(sqlStr)
	var err error
	st, err = p.ParseStatement()
	if err == nil {
		switch st.(type) {
		case *engine.Select, *engine.Explain:
			call.shared = true
		}
	}

	parsedStmtMu.Lock()
	if err == nil && call.shared {
		if len(parsedStmtCache) >= parsedStmtCacheMaxEntries {
			// Random eviction via map iteration order; a bad eviction just
			// costs one re-parse.
			for k := range parsedStmtCache {
				if len(parsedStmtCache) < parsedStmtCacheMaxEntries {
					break
				}
				delete(parsedStmtCache, k)
			}
		}
		parsedStmtCache[sqlStr] = st
	}
	call.stmt = st
	call.err = err
	delete(parsedStmtInFlight, sqlStr)
	close(call.done)
	parsedStmtMu.Unlock()
	return st, err
}

// parseCacheCandidate keeps DML out of the cold-miss coordinator. A write
// cannot be shared safely between connections, and coordinating it would only
// serialize a burst of independent mutations. The parser remains the final
// authority; this inexpensive check merely limits the immutable read cache.
func parseCacheCandidate(sqlStr string) bool {
	sqlStr = strings.TrimLeft(sqlStr, " \t\r\n")
	end := 0
	for end < len(sqlStr) {
		ch := sqlStr[end]
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') {
			break
		}
		end++
	}
	if end == 0 {
		return false
	}
	switch strings.ToUpper(sqlStr[:end]) {
	case "SELECT", "EXPLAIN":
		return true
	default:
		return false
	}
}

//nolint:gocyclo // execSQL coordinates parsing, locking, WAL, and transaction paths.
func (c *conn) execSQL(ctx context.Context, sqlStr string) (driver.Result, error) {
	// Transaction-control statements are always short and keyword-only, so
	// bail out before normalizeTransactionSQL's ToUpper/Fields/Join pass
	// unless the text could plausibly be one: that pass used to run over the
	// full substituted SQL text of every statement, including a bulk INSERT
	// with kilobytes of inlined literals.
	if looksLikeTransactionControl(sqlStr) {
		if res, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
			return res, err
		}
	}
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}
	return c.execStatement(ctx, st)
}

// looksLikeTransactionControl reports whether sqlStr's first keyword could
// start a transaction-control statement, without allocating or scanning past
// it. It is intentionally over-inclusive (BEGIN/START/COMMIT/ROLLBACK cover
// every case execTransactionControl actually recognizes) -- the full,
// authoritative check still happens there; this only skips that check's
// whole-string ToUpper/Fields/Join cost for the common case of a statement
// that plainly starts with SELECT/INSERT/UPDATE/DELETE/etc.
func looksLikeTransactionControl(sqlStr string) bool {
	s := strings.TrimLeft(sqlStr, " \t\r\n")
	end := 0
	for end < len(s) && end < 16 {
		ch := s[end]
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') {
			break
		}
		end++
	}
	switch strings.ToUpper(s[:end]) {
	case "BEGIN", "START", "COMMIT", "ROLLBACK":
		return true
	default:
		return false
	}
}

func (c *conn) execTransactionControl(ctx context.Context, sqlStr string) (driver.Result, bool, error) {
	switch normalizeTransactionSQL(sqlStr) {
	case "BEGIN", "BEGIN TRANSACTION", "START TRANSACTION":
		if c.inTx {
			return nil, true, fmt.Errorf("tinysql: transaction already active")
		}
		if _, err := c.BeginTx(ctx, driver.TxOptions{}); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "BEGIN READ ONLY", "BEGIN TRANSACTION READ ONLY", "START TRANSACTION READ ONLY":
		if c.inTx {
			return nil, true, fmt.Errorf("tinysql: transaction already active")
		}
		if _, err := c.BeginTx(ctx, driver.TxOptions{ReadOnly: true}); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "COMMIT", "COMMIT TRANSACTION":
		if err := c.commitTx(); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "ROLLBACK", "ROLLBACK TRANSACTION":
		if err := c.rollbackTx(); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	default:
		return nil, false, nil
	}
}

func normalizeTransactionSQL(sqlStr string) string {
	s := strings.TrimSpace(sqlStr)
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return strings.Join(strings.Fields(strings.ToUpper(s)), " ")
}

// affectedRows extracts the affected-row count from an UPDATE/DELETE result.
// The engine returns a single {countCell: n} row for the plain form; a
// RETURNING clause instead projects one row per affected row.
func affectedRows(rs *engine.ResultSet, countCell string) int64 {
	if rs == nil {
		return 0
	}
	if len(rs.Rows) == 1 && len(rs.Cols) == 1 && rs.Cols[0] == countCell {
		switch n := rs.Rows[0][countCell].(type) {
		case int:
			return int64(n)
		case int64:
			return n
		case float64:
			return int64(n)
		}
	}
	return int64(len(rs.Rows))
}

// isWriteStatement identifies statements that need the writer slot and the
// durability path. SELECT, EXPLAIN, and PRAGMA are the only guaranteed reads;
// conservatively scheduling all other statements as writes keeps DDL, catalog
// actions, jobs, and RBAC from bypassing the writer gate.
func isWriteStatement(st engine.Statement) bool {
	switch st.(type) {
	case *engine.Select, *engine.Explain, *engine.Pragma:
		return false
	}
	return true
}

// executeWriteStatement executes a write using the one canonical locking,
// transaction, WAL, and persistence path. QueryContext uses it for DML
// RETURNING so those rows do not accidentally run under a reader lock or skip
// the durability acknowledgement that ExecContext provides.
func (c *conn) executeWriteStatement(ctx context.Context, st engine.Statement) (*engine.ResultSet, error) {
	// This connection has now attempted a write; Close() must persist,
	// mirroring the persist() call that follows a successful write below
	// (directly here for autocommit, via commitTx for a transaction).
	c.wrote = true
	if c.srv.db.IsReadOnly() || (c.inTx && c.txReadOnly) {
		return nil, fmt.Errorf("tinysql: write attempted in read-only transaction")
	}
	if c.inTx {
		// ModeAdvancedWAL logs row operations as they happen. Open one WAL
		// transaction for the whole block (lazily, on its first write) so
		// recovery replays it only once this connection commits. Without
		// that grouping every statement was its own committed WAL transaction,
		// and a ROLLBACK left it on disk to be replayed.
		if _, err := c.shadow.BeginAmbientWALTx(); err != nil {
			return nil, err
		}
		rs, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
		if err != nil {
			return nil, err
		}
		c.txDirty = true
		return rs, nil
	}

	if err := c.srv.acquireWriter(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseWriter()
	// writeMu serializes this write against every other writer for its whole
	// span, mutation and persist alike -- see its doc comment on server.
	// mu.Lock() below is narrower: held only while the statement actually
	// mutates c.srv.db, so a concurrent SELECT (mu.RLock()) is blocked only
	// for that in-memory step, not for the persist() call's disk I/O after
	// it. persist() is still fully serialized against other writers by
	// writeMu, which is what makes releasing mu before calling it safe.
	c.srv.writeMu.Lock()
	defer c.srv.writeMu.Unlock()
	rs, err := c.executeAndLockContent(ctx, st)
	if err != nil {
		return nil, err
	}
	// Fail the statement if it could not be made durable, rather than
	// acknowledging a write that a restart will not find.
	if err := c.srv.persist(); err != nil {
		return nil, err
	}
	return rs, nil
}

// executeAndLockContent runs st against the live database under mu.Lock(),
// releasing it as soon as engine.Execute returns rather than leaving it held
// for whatever the caller does afterward (persist()'s disk I/O, notably).
// Run the statement against the live database in every storage mode,
// ModeWAL included: the engine appends it to the WAL itself and rolls the
// statement back if that append fails, so the log still leads the change.
func (c *conn) executeAndLockContent(ctx context.Context, st engine.Statement) (*engine.ResultSet, error) {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	return engine.Execute(ctx, c.srv.db, c.tenant, st)
}

func (c *conn) execStatement(ctx context.Context, st engine.Statement) (driver.Result, error) {
	if isWriteStatement(st) {
		rs, err := c.executeWriteStatement(ctx, st)
		if err != nil {
			return nil, err
		}
		// Report affected rows for UPDATE/DELETE. The engine returns a single
		// {updated|deleted: n} cell for the plain form; a RETURNING clause
		// projects one row per affected row. INSERT's AST retains its VALUES
		// rows, which is the database/sql-visible inserted count.
		switch s := st.(type) {
		case *engine.Insert:
			return driver.RowsAffected(int64(len(s.Rows))), nil
		case *engine.Update:
			return driver.RowsAffected(affectedRows(rs, "updated")), nil
		case *engine.Delete:
			return driver.RowsAffected(affectedRows(rs, "deleted")), nil
		}
		return driver.RowsAffected(0), nil
	}

	// READS: unter RLock auf aktueller DB
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	defer c.srv.mu.RUnlock()
	_, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
	if err != nil {
		return nil, err
	}
	// no rows affected for pure reads
	return driver.RowsAffected(0), nil
}
