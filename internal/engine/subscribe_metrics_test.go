package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"testing"
)

func TestSubscriptionSingleMetricUnchangedAndChanged(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_metrics (value INT)`)
	execSQL(t, db, `INSERT INTO live_metrics VALUES (1), (2)`)
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT COUNT(*) AS n FROM live_metrics`).(*Select)}
	first, err := state.refresh(t.Context())
	if err != nil || first == nil || !first.Initial || first.Added[0]["n"] != 2 {
		t.Fatalf("initial: %v %v", first, err)
	}
	execSQL(t, db, `UPDATE live_metrics SET value = 3 WHERE value = 1`)
	unchanged, err := state.refresh(t.Context())
	if err != nil || unchanged != nil {
		t.Fatalf("unchanged: %v %v", unchanged, err)
	}
	execSQL(t, db, `INSERT INTO live_metrics VALUES (4)`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || len(change.Added) != 1 || len(change.Removed) != 1 || change.Added[0]["n"] != 3 || change.Removed[0]["n"] != 2 {
		t.Fatalf("changed: %v %v", change, err)
	}
}
