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
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.execSQL(ctx, sqlStr)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.querySQL(ctx, sqlStr)
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
	if res, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
		return res, err
	}
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}
	return c.execStatement(ctx, st)
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
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	// Run the statement against the live database in every storage mode,
	// ModeWAL included. The engine appends it to the WAL itself and rolls the
	// statement back if that append fails, so the log still leads the change.
	rs, err := engine.Execute(ctx, c.srv.db, c.tenant, st)
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
