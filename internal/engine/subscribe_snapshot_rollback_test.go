package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Changes that precede a failed statement may still be awaiting delivery.
// A rollback must not discard them or introduce any of the failed writes.
func TestSubscriptionAggregateCoalescesRollbackWithCommittedChanges(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, q := range []string{
		`CREATE TABLE live_snapshot (id INT, category TEXT, amount INT, code INT)`,
		`CREATE UNIQUE INDEX live_snapshot_code ON live_snapshot(code)`,
		`INSERT INTO live_snapshot VALUES (1, 'a', 2, 10), (2, 'b', 5, 20), (3, 'a', 8, 30)`,
	} {
		execSQL(t, db, q)
	}
	query := `SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM live_snapshot WHERE amount >= 3 GROUP BY category`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	execSQL(t, db, `INSERT INTO live_snapshot VALUES (4, 'a', 7, 40)`)
	if _, err := Execute(t.Context(), db, "default", mustParse(`UPDATE live_snapshot SET amount = 99, code = 100 WHERE id >= 1`)); err == nil {
		t.Fatal("expected unique index failure")
	}
	execSQL(t, db, `UPDATE live_snapshot SET category = 'b', amount = 9 WHERE id = 3`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil {
		t.Fatalf("refresh: %v %v", change, err)
	}
	want := execSQL(t, db, query)
	if !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
	execSQL(t, db, `DELETE FROM live_snapshot WHERE id = 2`)
	if _, err := state.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	want = execSQL(t, db, query)
	if !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("after delete got %v want %v", state.snapshot, want.Rows)
	}
}

// An update outside the query's dependencies can keep the aggregate state, but
// later updates and deletions must still find the replacement row by identity.
func TestSubscriptionAggregateUnrelatedUpdateMaintainsIdentity(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_identity (id INT, g INT, v INT, keep INT, note INT)`)
	execSQL(t, db, `INSERT INTO live_identity VALUES (1,1,10,1,0), (2,1,20,1,0), (3,2,30,0,0)`)
	query := `SELECT g, COUNT(*) AS n, SUM(v) AS total FROM live_identity WHERE keep = 1 GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	a := state.aggregate
	old := a.rows[0]
	oldID := &old.source[0]
	oldGroup := a.groups[old.group].state
	execSQL(t, db, `UPDATE live_identity SET note = 7 WHERE id = 1`)
	if delta, err := state.refresh(t.Context()); err != nil || delta != nil {
		t.Fatalf("irrelevant update: %v %v", delta, err)
	}
	if a.rows[0] != old || a.groups[old.group].state != oldGroup {
		t.Fatal("aggregate state was rebuilt")
	}
	if a.identities[oldID] != nil || a.identities[&old.source[0]] != old {
		t.Fatal("row identity was not replaced")
	}
	if _, ok := a.groups[old.group].members[old]; !ok {
		t.Fatal("group lost the retained member")
	}
	for _, mutation := range []string{`UPDATE live_identity SET v = 11 WHERE id = 1`, `DELETE FROM live_identity WHERE id = 1`, `UPDATE live_identity SET keep = 1 WHERE id = 3`} {
		execSQL(t, db, mutation)
		if _, err := state.refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		want := execSQL(t, db, query)
		if !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("%s: got %v want %v", mutation, state.snapshot, want.Rows)
		}
	}
}
