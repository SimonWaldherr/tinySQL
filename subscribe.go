package tinysql

import (
	"context"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

type QueryChange = engine.QueryChange
type QuerySubscription = engine.QuerySubscription

// SubscribeSQL delivers an initial result and coalesced result deltas for a
// deterministic, single-table SELECT. Close the subscription when done.
func SubscribeSQL(ctx context.Context, db *DB, tenant, query string) (*QuerySubscription, error) {
	return engine.SubscribeSQL(ctx, db, tenant, query)
}
