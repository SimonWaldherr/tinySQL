// Queries: turning a statement into driver.Rows.
package driver

import (
	"context"
	"database/sql/driver"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func (c *conn) querySQL(ctx context.Context, sqlStr string) (driver.Rows, error) {
	if looksLikeTransactionControl(sqlStr) {
		if _, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
			if err != nil {
				return nil, err
			}
			return emptyRows{}, nil
		}
	}
	// Queries return a driver.Rows. Some write statements are row-producing
	// too (DML ... RETURNING), so keep their normal writer/persistence path
	// while forwarding the engine's ResultSet instead of discarding it.
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}

	if statementReturnsRows(st) {
		if isWriteStatement(st) {
			rs, err := c.executeWriteStatement(ctx, st)
			if err != nil {
				return nil, err
			}
			return &rows{rs: rs}, nil
		}
		return c.queryStatement(ctx, st)
	}

	// For statements with no result rows, execute via the pre-parsed statement
	// (no re-parse) and return an empty set to satisfy driver.Rows.
	if _, err = c.execStatement(ctx, st); err != nil {
		return nil, err
	}
	return emptyRows{}, nil
}

// statementReturnsRows identifies the database/sql Query shapes for which
// engine.Execute produces result rows. PRAGMA is intentionally included for
// read pragmas such as table_info; CALL lets database/sql clients consume a
// procedure's ResultSet through Query/QueryRow.
func statementReturnsRows(st engine.Statement) bool {
	switch s := st.(type) {
	case *engine.Select, *engine.Explain, *engine.Pragma, *engine.CallProcedure:
		return true
	case *engine.Insert:
		return len(s.Returning) > 0
	case *engine.Update:
		return len(s.Returning) > 0
	case *engine.Delete:
		return len(s.Returning) > 0
	default:
		return false
	}
}

// queryStatement executes an already parsed read-shaped statement. It is
// deliberately shared by normal and prepared queries so locking, snapshots,
// and database/sql row ownership remain identical.
func (c *conn) queryStatement(ctx context.Context, st engine.Statement) (driver.Rows, error) {
	return c.queryStatementWithCleanup(ctx, st, nil)
}

// queryStatementWithCleanup executes a read-shaped statement. Directly
// streamable scans transfer ownership of the server reader reservation and
// cleanup to the returned rows. Shapes which the engine must materialize
// anyway execute synchronously and release both before returning, avoiding a
// redundant producer goroutine and row channel.
//
// The cleanup is primarily used by prepared statements: a direct stream keeps
// evaluating the borrowed AST after QueryContext returns, so releasing it
// before Rows reaches EOF or Close would let another execution overwrite
// bound literals while the producer is still scanning. A materialized result
// no longer refers to that AST and can release it immediately.
//
// onClose is always consumed by this function: immediately on a start error,
// or later by rows.Close after the stream producer has stopped.
func (c *conn) queryStatementWithCleanup(ctx context.Context, st engine.Statement, onClose func()) (driver.Rows, error) {
	if err := c.srv.acquireReader(ctx); err != nil {
		if onClose != nil {
			onClose()
		}
		return nil, err
	}
	c.srv.mu.RLock()
	if !engine.MayStreamIncrementally(st) {
		rs, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
		c.srv.mu.RUnlock()
		c.srv.releaseReader()
		if onClose != nil {
			onClose()
		}
		if err != nil {
			return nil, err
		}
		if rs == nil {
			rs = &engine.ResultSet{}
		}
		return &rows{rs: rs}, nil
	}
	stream, err := engine.ExecuteStream(ctx, c.currentDB(), c.tenant, st)
	// ExecuteStream does not return until its producer has either pinned an
	// immutable table snapshot, opened a read-only paged cursor, retained the
	// storage content read lock, or finished materializing. The server lock is
	// therefore no longer needed after the header arrives. Releasing it here
	// lets a writer proceed concurrently with a slow client; storage's table
	// copy-on-write keeps the stream's result snapshot stable.
	c.srv.mu.RUnlock()
	if err != nil {
		c.srv.releaseReader()
		if onClose != nil {
			onClose()
		}
		return nil, err
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			c.srv.releaseReader()
			if onClose != nil {
				onClose()
			}
		})
	}
	// Buffered rows no longer refer to the database or the prepared statement.
	// Release their ownership as soon as production completes; Close retains
	// the same release path for a producer blocked by backpressure.
	go func() {
		<-stream.Done()
		release()
	}()
	return &rows{
		stream:     stream,
		streamCols: stream.Columns(),
		onClose:    release,
	}, nil
}

// NamedValueChecker
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Normalize common Go types into database/sql primitive types.
	switch v := nv.Value.(type) {
	case time.Time:
		nv.Value = v.UTC().Format(time.RFC3339Nano)
	case []byte:
		// Keep BLOB parameters as bytes. bindPlaceholders emits a SQL X'...'
		// literal and the parser recreates []byte without a text/base64 round
		// trip.
		nv.Value = append([]byte(nil), v...)
	case int:
		nv.Value = int64(v)
	}
	return nil
}
