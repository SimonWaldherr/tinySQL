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
	// Queries return a driver.Rows. For non-SELECT statements, execute them
	// and return an empty result set to satisfy the interface.
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}

	// For non-result statements, execute via pre-parsed statement (no re-parse).
	_, isSelect := st.(*engine.Select)
	_, isExplain := st.(*engine.Explain)
	if !isSelect && !isExplain {
		if _, err = c.execStatement(ctx, st); err != nil {
			return nil, err
		}
		return emptyRows{}, nil
	}

	return c.queryStatement(ctx, st)
}

// queryStatement executes an already parsed SELECT/EXPLAIN statement. It is
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
