// Queries: turning a statement into driver.Rows.
package driver

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func (c *conn) querySQL(ctx context.Context, sqlStr string) (driver.Rows, error) {
	if _, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
		if err != nil {
			return nil, err
		}
		return emptyRows{}, nil
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
// engine.Execute produces result rows. PRAGMA is intentionally included: its
// handler returns a ResultSet for read pragmas such as table_info.
func statementReturnsRows(st engine.Statement) bool {
	switch s := st.(type) {
	case *engine.Select, *engine.Explain, *engine.Pragma:
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
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	defer c.srv.mu.RUnlock()
	rs, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
	if err != nil {
		return nil, err
	}
	return &rows{rs: rs}, nil
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
