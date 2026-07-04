// Tests for RBAC SQL syntax (rbac_parser.go): CREATE/DROP/ALTER USER,
// CREATE/DROP ROLE, GRANT/REVOKE. Exercises the same bootstrap sequence a
// real deployment would run, entirely through SQL text rather than the Go
// API tested in rbac_test.go.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRBACSQLBootstrapSequence(t *testing.T) {
	db := storage.NewDB()
	base := context.Background()

	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'sup3rsecret' ROLE admin_role`)

	adminCtx := WithUser(base, "admin")
	if _, err := Execute(adminCtx, db, "default", mustParse(`CREATE ROLE readonly`)); err != nil {
		t.Fatalf("admin CREATE ROLE should succeed: %v", err)
	}
	if _, err := Execute(adminCtx, db, "default", mustParse(`GRANT SELECT ON * TO ROLE readonly`)); err != nil {
		t.Fatalf("admin GRANT should succeed: %v", err)
	}
	if _, err := Execute(adminCtx, db, "default", mustParse(`CREATE USER viewer WITH PASSWORD 'viewpass' ROLE readonly`)); err != nil {
		t.Fatalf("admin CREATE USER should succeed: %v", err)
	}
	if _, err := Execute(adminCtx, db, "default", mustParse(`CREATE TABLE secrets (id INT, val TEXT)`)); err != nil {
		t.Fatalf("admin CREATE TABLE should succeed: %v", err)
	}
	if _, err := Execute(adminCtx, db, "default", mustParse(`INSERT INTO secrets VALUES (1, 'classified')`)); err != nil {
		t.Fatalf("admin INSERT should succeed: %v", err)
	}

	viewerCtx := WithUser(base, "viewer")
	rs, err := Execute(viewerCtx, db, "default", mustParse(`SELECT * FROM secrets`))
	if err != nil {
		t.Fatalf("viewer SELECT should succeed (has SELECT grant): %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if _, err := Execute(viewerCtx, db, "default", mustParse(`DELETE FROM secrets WHERE id = 1`)); err == nil {
		t.Fatal("viewer DELETE should be denied (no DELETE grant)")
	}
	if _, err := Execute(viewerCtx, db, "default", mustParse(`INSERT INTO secrets VALUES (2, 'x')`)); err == nil {
		t.Fatal("viewer INSERT should be denied (no INSERT grant)")
	}

	if _, err := Execute(adminCtx, db, "default", mustParse(`ALTER USER viewer DISABLE`)); err != nil {
		t.Fatalf("admin ALTER USER DISABLE should succeed: %v", err)
	}
	if _, err := Execute(viewerCtx, db, "default", mustParse(`SELECT * FROM secrets`)); err == nil {
		t.Fatal("disabled viewer SELECT should be denied")
	}
	if _, err := Execute(adminCtx, db, "default", mustParse(`ALTER USER viewer ENABLE`)); err != nil {
		t.Fatalf("admin ALTER USER ENABLE should succeed: %v", err)
	}
	if _, err := Execute(viewerCtx, db, "default", mustParse(`SELECT * FROM secrets`)); err != nil {
		t.Fatalf("re-enabled viewer SELECT should succeed: %v", err)
	}

	if !db.Catalog().Authenticate("admin", "sup3rsecret") {
		t.Error("admin should authenticate with the correct password")
	}
	if db.Catalog().Authenticate("admin", "wrong") {
		t.Error("admin should not authenticate with the wrong password")
	}
}

func TestRBACSQLAlterUserPassword(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'old-password' ROLE admin_role`)

	adminCtx := WithUser(context.Background(), "admin")
	if _, err := Execute(adminCtx, db, "default", mustParse(`ALTER USER admin WITH PASSWORD 'new-password'`)); err != nil {
		t.Fatalf("ALTER USER WITH PASSWORD should succeed: %v", err)
	}
	if db.Catalog().Authenticate("admin", "old-password") {
		t.Error("old password should no longer authenticate")
	}
	if !db.Catalog().Authenticate("admin", "new-password") {
		t.Error("new password should authenticate")
	}
	// Role membership must survive the password change (executeAlterUser
	// recreates the account internally via DropUser+CreateUser).
	u, ok := db.Catalog().GetUser("admin")
	if !ok || len(u.Roles) != 1 || u.Roles[0] != "admin_role" {
		t.Errorf("expected admin_role membership to survive password change, got %+v", u)
	}
}

func TestRBACSQLGrantOnSpecificTable(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)
	adminCtx := WithUser(context.Background(), "admin")

	Execute(adminCtx, db, "default", mustParse(`CREATE TABLE t1 (id INT)`))
	Execute(adminCtx, db, "default", mustParse(`CREATE TABLE t2 (id INT)`))
	Execute(adminCtx, db, "default", mustParse(`INSERT INTO t1 VALUES (1)`))
	Execute(adminCtx, db, "default", mustParse(`INSERT INTO t2 VALUES (1)`))

	execSQLAs(t, db, adminCtx, `CREATE ROLE t1_reader`)
	execSQLAs(t, db, adminCtx, `GRANT SELECT ON t1 TO ROLE t1_reader`)
	execSQLAs(t, db, adminCtx, `CREATE USER t1user WITH PASSWORD 'pw' ROLE t1_reader`)

	userCtx := WithUser(context.Background(), "t1user")
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM t1`)); err != nil {
		t.Fatalf("expected SELECT on t1 to be allowed: %v", err)
	}
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM t2`)); err == nil {
		t.Fatal("expected SELECT on t2 to be denied (grant is scoped to t1 only)")
	}
}

func TestRBACSQLGrantWildcardTable(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)
	adminCtx := WithUser(context.Background(), "admin")

	execSQLAs(t, db, adminCtx, `CREATE TABLE a (id INT)`)
	execSQLAs(t, db, adminCtx, `CREATE TABLE b (id INT)`)
	execSQLAs(t, db, adminCtx, `CREATE ROLE any_reader`)
	execSQLAs(t, db, adminCtx, `GRANT SELECT ON * TO ROLE any_reader`)
	execSQLAs(t, db, adminCtx, `CREATE USER anyuser WITH PASSWORD 'pw' ROLE any_reader`)

	userCtx := WithUser(context.Background(), "anyuser")
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM a`)); err != nil {
		t.Fatalf("expected SELECT on a to be allowed via wildcard grant: %v", err)
	}
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM b`)); err != nil {
		t.Fatalf("expected SELECT on b to be allowed via wildcard grant: %v", err)
	}
}

func TestRBACSQLRevokePrivilege(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)
	adminCtx := WithUser(context.Background(), "admin")

	execSQLAs(t, db, adminCtx, `CREATE TABLE t (id INT)`)
	execSQLAs(t, db, adminCtx, `CREATE ROLE r`)
	execSQLAs(t, db, adminCtx, `GRANT SELECT ON t TO ROLE r`)
	execSQLAs(t, db, adminCtx, `CREATE USER u WITH PASSWORD 'pw' ROLE r`)

	userCtx := WithUser(context.Background(), "u")
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM t`)); err != nil {
		t.Fatalf("expected SELECT to be allowed before revoke: %v", err)
	}

	execSQLAs(t, db, adminCtx, `REVOKE SELECT ON t FROM ROLE r`)
	if _, err := Execute(userCtx, db, "default", mustParse(`SELECT * FROM t`)); err == nil {
		t.Fatal("expected SELECT to be denied after revoke")
	}
}

func TestRBACSQLMultiplePermissionsInOneGrant(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)
	adminCtx := WithUser(context.Background(), "admin")

	execSQLAs(t, db, adminCtx, `CREATE TABLE t (id INT)`)
	execSQLAs(t, db, adminCtx, `CREATE ROLE editor`)
	execSQLAs(t, db, adminCtx, `GRANT SELECT, INSERT, UPDATE ON t TO ROLE editor`)
	execSQLAs(t, db, adminCtx, `CREATE USER ed WITH PASSWORD 'pw' ROLE editor`)

	edCtx := WithUser(context.Background(), "ed")
	if _, err := Execute(edCtx, db, "default", mustParse(`INSERT INTO t VALUES (1)`)); err != nil {
		t.Fatalf("expected INSERT to be allowed: %v", err)
	}
	if _, err := Execute(edCtx, db, "default", mustParse(`UPDATE t SET id = 2 WHERE id = 1`)); err != nil {
		t.Fatalf("expected UPDATE to be allowed: %v", err)
	}
	if _, err := Execute(edCtx, db, "default", mustParse(`DELETE FROM t WHERE id = 2`)); err == nil {
		t.Fatal("expected DELETE to be denied (not in the multi-permission grant)")
	}
}

// execSQLAs runs sql with ctx (rather than execSQL's hardcoded
// context.Background(), which would fail once RBAC is active) and fails
// the test on error.
func execSQLAs(t *testing.T, db *storage.DB, ctx context.Context, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(ctx, db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
	return rs
}
