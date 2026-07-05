// Tests for RBAC enforcement (rbac.go, internal/storage/rbac.go). Covers:
// backward compatibility when no users are defined (the overwhelmingly
// common case — RBAC must be fully transparent then), and enforcement once
// it's opted into via CreateUser.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRBACTransparentWithoutUsers(t *testing.T) {
	db := storage.NewDB()
	// No CreateUser call at all — every existing embedder/test relies on
	// this working exactly as it always has, with no user in context.
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	rs := execSQL(t, db, `SELECT * FROM t`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row without RBAC enabled, got %d", len(rs.Rows))
	}
}

func TestRBACDeniesWithoutUserInContext(t *testing.T) {
	db := storage.NewDB()
	if err := db.Catalog().CreateUser("alice", "secret123", nil); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	// Now that a user exists, RBAC is active — a request with no user in
	// context must be rejected, not silently treated as trusted.
	stmt := mustParse(`CREATE TABLE t (id INT)`)
	_, err := Execute(context.Background(), db, "default", stmt)
	if err == nil {
		t.Fatal("expected access denied with no user in context, got nil error")
	}
}

func TestRBACDeniesWithoutPermission(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateUser("alice", "secret123", nil); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	// alice has no roles/grants at all yet.
	ctx := WithUser(context.Background(), "alice")
	stmt := mustParse(`CREATE TABLE t (id INT)`)
	_, err := Execute(ctx, db, "default", stmt)
	if err == nil {
		t.Fatal("expected access denied for a grant-less user, got nil error")
	}
}

func TestRBACAllowsWithGrant(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateRole("app_writer"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := cat.GrantPermission("app_writer", storage.PermDDL, "*", "*"); err != nil {
		t.Fatalf("GrantPermission DDL: %v", err)
	}
	if err := cat.GrantPermission("app_writer", storage.PermInsert, "*", "*"); err != nil {
		t.Fatalf("GrantPermission INSERT: %v", err)
	}
	if err := cat.GrantPermission("app_writer", storage.PermSelect, "*", "*"); err != nil {
		t.Fatalf("GrantPermission SELECT: %v", err)
	}
	if err := cat.CreateUser("bob", "hunter2", []string{"app_writer"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	ctx := WithUser(context.Background(), "bob")
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("CREATE TABLE should be allowed: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1)`)); err != nil {
		t.Fatalf("INSERT should be allowed: %v", err)
	}
	rs, err := Execute(ctx, db, "default", mustParse(`SELECT * FROM t`))
	if err != nil {
		t.Fatalf("SELECT should be allowed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}

	// bob has no DELETE grant.
	if _, err := Execute(ctx, db, "default", mustParse(`DELETE FROM t WHERE id = 1`)); err == nil {
		t.Fatal("expected DELETE to be denied (no DELETE grant), got nil error")
	}
}

func TestRBACPerTableGrantScoping(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	// Bootstrap the two tables while RBAC is still off (no users yet), then
	// create users afterward — mirrors a realistic provisioning sequence.
	execSQL(t, db, `CREATE TABLE public_data (id INT)`)
	execSQL(t, db, `CREATE TABLE secret_data (id INT)`)
	execSQL(t, db, `INSERT INTO public_data VALUES (1)`)
	execSQL(t, db, `INSERT INTO secret_data VALUES (1)`)

	if err := cat.CreateRole("reader"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := cat.GrantPermission("reader", storage.PermSelect, "main", "public_data"); err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}
	if err := cat.CreateUser("carol", "pw12345", []string{"reader"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	ctx := WithUser(context.Background(), "carol")
	if _, err := Execute(ctx, db, "default", mustParse(`SELECT * FROM public_data`)); err != nil {
		t.Fatalf("expected SELECT on public_data to be allowed: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`SELECT * FROM secret_data`)); err == nil {
		t.Fatal("expected SELECT on secret_data to be denied (grant is scoped to public_data only)")
	}
}

func TestRBACDisabledUserDenied(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateRole("admin"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := cat.GrantPermission("admin", storage.PermAll, "*", "*"); err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}
	if err := cat.CreateUser("dave", "pw", []string{"admin"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	ctx := WithUser(context.Background(), "dave")
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("expected admin to succeed before disabling: %v", err)
	}
	if err := cat.SetUserDisabled("dave", true); err != nil {
		t.Fatalf("SetUserDisabled: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`SELECT * FROM t`)); err == nil {
		t.Fatal("expected a disabled user to be denied, got nil error")
	}
}

func TestRBACAuthenticate(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateUser("erin", "correct-password", nil); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if !cat.Authenticate("erin", "correct-password") {
		t.Error("expected correct password to authenticate")
	}
	if cat.Authenticate("erin", "wrong-password") {
		t.Error("expected wrong password to fail authentication")
	}
	if cat.Authenticate("nonexistent-user", "anything") {
		t.Error("expected unknown user to fail authentication")
	}
}

func TestRBACRoleGrantRevoke(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateRole("temp_role"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := cat.CreateUser("frank", "pw", nil); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := cat.GrantRoleToUser("frank", "temp_role"); err != nil {
		t.Fatalf("GrantRoleToUser: %v", err)
	}
	u, ok := cat.GetUser("frank")
	if !ok || len(u.Roles) != 1 || u.Roles[0] != "temp_role" {
		t.Fatalf("expected frank to have temp_role, got %+v", u)
	}
	if err := cat.RevokeRoleFromUser("frank", "temp_role"); err != nil {
		t.Fatalf("RevokeRoleFromUser: %v", err)
	}
	u, ok = cat.GetUser("frank")
	if !ok || len(u.Roles) != 0 {
		t.Fatalf("expected frank to have no roles after revoke, got %+v", u)
	}
}

func TestGenerateRandomPassword(t *testing.T) {
	p1, err := storage.GenerateRandomPassword(16)
	if err != nil {
		t.Fatalf("GenerateRandomPassword: %v", err)
	}
	p2, err := storage.GenerateRandomPassword(16)
	if err != nil {
		t.Fatalf("GenerateRandomPassword: %v", err)
	}
	if p1 == p2 {
		t.Error("expected two calls to produce different random passwords")
	}
	if len(p1) != 32 { // 16 bytes hex-encoded = 32 chars
		t.Errorf("expected 32-char hex string, got %d chars: %q", len(p1), p1)
	}
}

func TestRBACExplicitDisableOverridesHasUsers(t *testing.T) {
	db := storage.NewDB()
	cat := db.Catalog()
	if err := cat.CreateRole("admin_role"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := cat.GrantPermission("admin_role", storage.PermAll, "*", "*"); err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}
	if err := cat.CreateUser("admin", "pw", []string{"admin_role"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if !db.IsRBACEnabled() {
		t.Fatal("expected RBAC to be enabled once a user exists")
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE t (id INT)`)); err == nil {
		t.Fatal("expected an unauthenticated request to be denied while RBAC is active")
	}

	db.SetRBACEnabled(false)
	if db.IsRBACEnabled() {
		t.Fatal("expected IsRBACEnabled to report false after SetRBACEnabled(false)")
	}
	// Same unauthenticated context that was denied a moment ago must now
	// succeed — users/roles still exist, but enforcement is off.
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("expected the request to succeed once RBAC is explicitly disabled: %v", err)
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO t VALUES (1)`)); err != nil {
		t.Fatalf("expected INSERT to succeed while RBAC is disabled: %v", err)
	}

	db.SetRBACEnabled(true)
	if !db.IsRBACEnabled() {
		t.Fatal("expected IsRBACEnabled to report true again after SetRBACEnabled(true)")
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO t VALUES (2)`)); err == nil {
		t.Fatal("expected enforcement to resume after SetRBACEnabled(true), denying an unauthenticated request again")
	}
	// The existing admin account and its grants must still work — disabling
	// and re-enabling must not have lost any RBAC state.
	adminCtx := WithUser(context.Background(), "admin")
	if _, err := Execute(adminCtx, db, "default", mustParse(`INSERT INTO t VALUES (2)`)); err != nil {
		t.Fatalf("expected admin's grants to still work after the disable/re-enable cycle: %v", err)
	}
}

func TestRBACDisableBeforeAnyUserIsANoOp(t *testing.T) {
	// Calling SetRBACEnabled on a database with no users at all shouldn't
	// break anything either way — IsRBACEnabled must stay false regardless.
	db := storage.NewDB()
	db.SetRBACEnabled(false)
	if db.IsRBACEnabled() {
		t.Fatal("expected IsRBACEnabled to be false with no users, disabled")
	}
	db.SetRBACEnabled(true)
	if db.IsRBACEnabled() {
		t.Fatal("expected IsRBACEnabled to still be false with no users, even after SetRBACEnabled(true) (no users exist to enforce against)")
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("expected CREATE TABLE to succeed with no users regardless of the toggle: %v", err)
	}
}
