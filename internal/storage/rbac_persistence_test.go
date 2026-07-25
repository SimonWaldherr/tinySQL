package storage

import (
	"path/filepath"
	"testing"
)

// A failed statement restores the catalog from its snapshot. RBAC used to be
// absent from that snapshot form, so any rollback wiped every user, role and
// grant — and because enforcement is opt-in via HasUsers, losing the last user
// switched authorization OFF rather than denying access.
func TestStatementSnapshotPreservesRBAC(t *testing.T) {
	db := NewDB()
	cat := db.Catalog()
	if err := cat.CreateUser("alice", "pw", []string{"admin"}); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateRole("admin"); err != nil {
		t.Fatal(err)
	}
	if err := cat.GrantPermission("admin", PermSelect, "main", "t"); err != nil {
		t.Fatal(err)
	}
	if !db.Catalog().IsRBACEnabled() {
		t.Fatal("expected RBAC to be enabled after CreateUser")
	}

	// Simulate one failed statement: take a snapshot, then restore it.
	db.LockContentForWrite()
	snap := db.SnapshotForStatement()
	db.RestoreStatementSnapshot(snap)
	db.UnlockContentForWrite()

	if !db.Catalog().IsRBACEnabled() {
		t.Error("regression: RBAC disabled after a statement rollback (users/roles/grants wiped) -> all permission checks now pass")
	}
	if _, ok := db.Catalog().GetUser("alice"); !ok {
		t.Error("regression: user alice lost after a statement rollback")
	}
	if !db.Catalog().HasPermission("alice", PermSelect, "main", "t") {
		t.Error("regression: grant lost after a statement rollback")
	}
}

// The same omission meant RBAC never reached a checkpoint file, so a restart
// came back with authorization disabled.
func TestGOBCheckpointPersistsRBAC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.gob")

	db := NewDB()
	cat := db.Catalog()
	if err := cat.CreateRole("admin"); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateUser("alice", "pw", []string{"admin"}); err != nil {
		t.Fatal(err)
	}
	if err := cat.GrantPermission("admin", PermSelect, "main", "t"); err != nil {
		t.Fatal(err)
	}
	if err := cat.RegisterView("main", "v", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := SaveToFile(db, path); err != nil {
		t.Fatal(err)
	}

	reopened := NewDB()
	if _, err := loadGOBInto(reopened, path); err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.Catalog().GetView("main", "v"); !ok {
		t.Error("view not restored from checkpoint")
	}
	if !reopened.Catalog().IsRBACEnabled() {
		t.Error("regression: RBAC not persisted by the GOB checkpoint -> after a restart every permission check passes")
	}
	if _, ok := reopened.Catalog().GetUser("alice"); !ok {
		t.Error("regression: user alice not persisted by the GOB checkpoint")
	}
	if !reopened.Catalog().Authenticate("alice", "pw") {
		t.Error("regression: alice's credentials not persisted by the GOB checkpoint")
	}
}
