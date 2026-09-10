package tinysql

import (
	"context"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

type QueryChange = engine.QueryChange
type QuerySubscription = engine.QuerySubscription
type SubscriptionStats = engine.SubscriptionStats
type SubscriptionOptions = engine.SubscriptionOptions

// SubscribeSQL delivers an initial result and coalesced result deltas for a
// SELECT, including views, joins and GROUP BY. Complex queries are re-executed
// on database changes; result deltas are unordered. Close when done.
func SubscribeSQL(ctx context.Context, db *DB, tenant, query string) (*QuerySubscription, error) {
	return engine.SubscribeSQL(ctx, db, tenant, query)
}

// SubscribeSQLWithOptions applies optional result size limits.
func SubscribeSQLWithOptions(ctx context.Context, db *DB, tenant, query string, opts SubscriptionOptions) (*QuerySubscription, error) {
	return engine.SubscribeSQLWithOptions(ctx, db, tenant, query, opts)
}
