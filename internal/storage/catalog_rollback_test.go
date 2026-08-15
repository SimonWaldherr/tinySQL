package storage

import "testing"

// The catalog half of a StatementSnapshot is copy-on-write: nothing is copied
// until the statement actually mutates the catalog, and nothing is restored if
// it never did. These tests pin both directions, because before them the
// catalog-rollback behavior of a failed DML statement had no coverage at all —
// the only test that touched RestoreStatementSnapshot's catalog path
// (TestStatementSnapshotPreservesRBAC) would pass just as happily if that path
// became a no-op.

// TestStatementSnapshotSkipsCatalogWhenUntouched is the common case: an
// ordinary INSERT/UPDATE/DELETE that fails a constraint check never reached
// the catalog, so rolling it back must not rebuild one. Keeping the same
// *CatalogManager matters beyond cost — a long-lived holder of the pointer
// (Scheduler captures one at construction) is orphaned by a replacement.
func TestStatementSnapshotSkipsCatalogWhenUntouched(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = [][]any{{1}}
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}
	before := db.Catalog()
	beforeRev := before.Revision()

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForTableStatement("default", "t")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	tbl.Rows = append(tbl.Rows, []any{2}) // the statement's half-applied work
	db.RestoreStatementSnapshot(snapshot)
	db.ReleaseStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if got := db.Catalog(); got != before {
		t.Fatal("rollback replaced the CatalogManager even though the statement never mutated it")
	}
	if got := db.Catalog().Revision(); got != beforeRev {
		t.Fatalf("catalog revision moved from %d to %d across a rollback that changed nothing", beforeRev, got)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("table rows after rollback = %d, want 1", len(tbl.Rows))
	}
}

// TestStatementSnapshotRestoresCatalogWhenMutated is the case the copy exists
// for. The capture is taken on the first catalog write, so what it holds must
// be the state from before that write, not after it.
func TestStatementSnapshotRestoresCatalogWhenMutated(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	catalog := db.Catalog()
	if err := catalog.RegisterView("main", "before_view", "SELECT 1"); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot := db.SnapshotForStatement()
	// The statement creates a view and then fails.
	if err := db.Catalog().RegisterView("main", "during_view", "SELECT 1"); err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	db.RestoreStatementSnapshot(snapshot)
	db.ReleaseStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if _, ok := db.Catalog().GetView("main", "during_view"); ok {
		t.Fatal("rollback kept a view the failed statement created")
	}
	if _, ok := db.Catalog().GetView("main", "before_view"); !ok {
		t.Fatal("rollback lost a view that existed before the failed statement")
	}
}

// TestReleasedSnapshotDoesNotCaptureLaterStatements guards the arming
// lifecycle. A snapshot left armed after its statement finished would capture
// the *next* statement's catalog pre-image, and a rollback of a third
// statement would then reinstate it — silently undoing committed work.
func TestReleasedSnapshotDoesNotCaptureLaterStatements(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	db.LockContentForWrite()
	first := db.SnapshotForStatement()
	db.ReleaseStatementSnapshot(first)
	db.UnlockContentForWrite()

	// A later, successful statement mutates the catalog.
	if err := db.Catalog().RegisterView("main", "later_view", "SELECT 1"); err != nil {
		t.Fatal(err)
	}

	if first.catalog.captured() {
		t.Fatal("a released snapshot captured a later statement's catalog")
	}

	// Restoring the released snapshot must therefore be a catalog no-op.
	db.LockContentForWrite()
	db.RestoreStatementSnapshot(first)
	db.UnlockContentForWrite()
	if _, ok := db.Catalog().GetView("main", "later_view"); !ok {
		t.Fatal("restoring a released snapshot undid a later statement's committed catalog change")
	}
}

// TestMarkMaterializedViewsStaleByDependencySkipsWriteLockWhenNothingDepends
// pins the precheck that keeps ordinary DML off the catalog's write lock. The
// revision counter is the observable: it only moves under lockWrite, and the
// SQL driver's COMMIT path runs a full catalog copy and deep-compare whenever
// it has moved.
func TestMarkMaterializedViewsStaleByDependencySkipsWriteLockWhenNothingDepends(t *testing.T) {
	c := NewCatalogManager()
	before := c.Revision()

	if affected := c.MarkMaterializedViewsStaleByDependency("main", "orders"); len(affected) != 0 {
		t.Fatalf("affected views = %#v for a catalog with no materialized views, want none", affected)
	}
	if got := c.Revision(); got != before {
		t.Fatalf("revision moved from %d to %d without marking anything stale", before, got)
	}
}

// TestMarkMaterializedViewsStaleByDependencyStillMarks confirms the precheck
// did not disable the actual behavior.
func TestMarkMaterializedViewsStaleByDependencyStillMarks(t *testing.T) {
	c := NewCatalogManager()
	if err := c.RegisterMaterializedView(&CatalogMaterializedView{
		Schema: "main", Name: "mv", InvalidateOnChange: true,
	}); err != nil {
		t.Fatal(err)
	}
	c.SetDependencies("main", "mv", "materialized_view", []CatalogDependency{{
		Schema: "main", ObjectName: "mv", DependsOnSchema: "main", DependsOnName: "orders",
	}})

	affected := c.MarkMaterializedViewsStaleByDependency("main", "orders")
	if len(affected) != 1 || affected[0] != "mv" {
		t.Fatalf("affected views = %#v, want [mv]", affected)
	}
	mv, ok := c.GetMaterializedView("main", "mv")
	if !ok || !mv.IsStale {
		t.Fatalf("materialized view stale flag = %v (found=%v), want true", ok && mv.IsStale, ok)
	}
}
