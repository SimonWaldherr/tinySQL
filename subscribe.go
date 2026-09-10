package tinysql

import (
	"context"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

type QueryChange = engine.QueryChange
type QuerySubscription = engine.QuerySubscription

// SubscribeSQL delivers an initial result and coalesced result deltas for a
// SELECT, including views, joins and GROUP BY. Complex queries are re-executed
// on database changes; result deltas are unordered. Close when done.
func SubscribeSQL(ctx context.Context, db *DB, tenant, query string) (*QuerySubscription, error) {
	return engine.SubscribeSQL(ctx, db, tenant, query)
}
