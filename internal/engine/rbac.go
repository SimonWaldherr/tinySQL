// RBAC enforcement for Execute (see internal/storage/rbac.go for the
// underlying users/roles/grants model).
//
// The acting user travels via context.Context (WithUser/UserFromContext),
// not as an Execute parameter — adding a parameter would break every
// existing caller's signature (internal/driver, cmd/server, every test in
// this module). A context key is the idiomatic Go way to thread an
// optional, cross-cutting value like this without an API break, and it
// composes with context.WithTimeout/WithCancel that callers already use.
//
// Enforcement itself is opt-in at the database level: checkPermission is a
// no-op unless db.IsRBACEnabled() is true (which itself is false until the
// first CreateUser call), so every existing embedder and test that never
// creates a user sees zero behavior change. Call db.SetRBACEnabled(false)
// to force enforcement off even after users/roles have been created — for
// a setup that provisions accounts ahead of time but isn't ready to
// enforce yet, or a dev/test environment that wants audit-log attribution
// via WithUser without access checks getting in the way.
package engine

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type rbacUserContextKey struct{}

// WithUser returns a context carrying the acting username for RBAC checks.
// Pass it to Execute via the context.Context parameter, e.g.
// Execute(engine.WithUser(ctx, "alice"), db, tenant, stmt).
func WithUser(ctx context.Context, username string) context.Context {
	return context.WithValue(ctx, rbacUserContextKey{}, username)
}

// UserFromContext returns the username set by WithUser, if any.
func UserFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	u, ok := ctx.Value(rbacUserContextKey{}).(string)
	return u, ok && u != ""
}

// checkPermission enforces RBAC for stmt, if enabled. Returns nil
// immediately if RBAC isn't active — see the package doc comment above and
// storage.DB.IsRBACEnabled/SetRBACEnabled.
func checkPermission(ctx context.Context, db *storage.DB, stmt Statement) error {
	if !db.IsRBACEnabled() {
		return nil
	}
	user, ok := UserFromContext(ctx)
	if !ok {
		return fmt.Errorf("access denied: this database requires an authenticated user (see engine.WithUser), but none was provided")
	}
	perm, schema, table, needsCheck := requiredPermission(stmt)
	if !needsCheck {
		return nil
	}
	if !db.Catalog().HasPermission(user, perm, schema, table) {
		return fmt.Errorf("access denied: user %q lacks %s permission on %s.%s", user, perm, schema, table)
	}
	return nil
}

// requiredPermission maps a statement to the permission/table it needs.
// needsCheck is false for statements that are always safe (EXPLAIN, PRAGMA)
// or that have no single physical table to scope a check to (e.g. a SELECT
// whose FROM is a subquery or table function, or CTE-only statements) —
// those fall through unchecked, a deliberate, documented scope boundary
// rather than an attempt at full per-referenced-table checking across
// joins/subqueries/CTEs.
func requiredPermission(stmt Statement) (perm storage.Permission, schema, table string, needsCheck bool) {
	switch s := stmt.(type) {
	case *Explain, *Pragma:
		return "", "", "", false
	case *Select:
		if s.From.Table == "" {
			return "", "", "", false
		}
		schema, table = splitObjectName(s.From.Table)
		return storage.PermSelect, schema, table, true
	case *Insert:
		schema, table = splitObjectName(s.Table)
		return storage.PermInsert, schema, table, true
	case *Update:
		schema, table = splitObjectName(s.Table)
		return storage.PermUpdate, schema, table, true
	case *Delete:
		schema, table = splitObjectName(s.Table)
		return storage.PermDelete, schema, table, true
	case *CreateTable:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *DropTable:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *AlterTable:
		schema, table = splitObjectName(s.Table)
		return storage.PermDDL, schema, table, true
	case *CreateIndex:
		schema, table = splitObjectName(s.Table)
		return storage.PermDDL, schema, table, true
	case *DropIndex:
		schema, table = splitObjectName(s.Table)
		return storage.PermDDL, schema, table, true
	case *CreateView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *DropView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *CreateMaterializedView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *DropMaterializedView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *RefreshMaterializedView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *AlterViewMaterialize:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *AlterMaterializedViewToView:
		schema, table = splitObjectName(s.Name)
		return storage.PermDDL, schema, table, true
	case *CreateTrigger:
		schema, table = splitObjectName(s.Table)
		return storage.PermDDL, schema, table, true
	case *DropTrigger:
		// DropTrigger only names the trigger, not its table — a schema-wide
		// DDL check is the best available granularity here.
		return storage.PermDDL, "*", "*", true
	case *CreateJob, *AlterJob, *DropJob:
		return storage.PermDDL, "*", "*", true
	case *CreateUser, *DropUser, *AlterUser, *CreateRole, *DropRole,
		*GrantPrivilege, *RevokePrivilege, *GrantRoleStmt, *RevokeRoleStmt:
		// User/role/grant management requires a wildcard (schema="*",
		// table="*") DDL grant specifically — i.e. only an
		// administrator-scoped role, not merely "DDL on some table" — so a
		// role that can create tables in one schema can't also mint new
		// users. There's no separate "ADMIN" permission distinct from a
		// wildcard PermDDL grant; that's a deliberate simplification.
		return storage.PermDDL, "*", "*", true
	}
	return "", "", "", false
}

// executeCreateUser handles CREATE USER. See storage.CatalogManager.CreateUser.
func executeCreateUser(env ExecEnv, s *CreateUser) (*ResultSet, error) {
	if err := env.db.Catalog().CreateUser(s.Name, s.Password, s.Roles); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropUser(env ExecEnv, s *DropUser) (*ResultSet, error) {
	if err := env.db.Catalog().DropUser(s.Name); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeAlterUser(env ExecEnv, s *AlterUser) (*ResultSet, error) {
	if s.SetEnabled != nil {
		if err := env.db.Catalog().SetUserDisabled(s.Name, *s.SetEnabled); err != nil {
			return nil, err
		}
	}
	if s.NewPassword != nil {
		// There's no separate "change password" catalog method — reuse
		// CreateUser's bcrypt hashing by dropping and recreating the
		// account, preserving its current role memberships and enabled
		// state exactly as they were.
		cat := env.db.Catalog()
		existing, ok := cat.GetUser(s.Name)
		if !ok {
			return nil, fmt.Errorf("user %q does not exist", s.Name)
		}
		if err := cat.DropUser(s.Name); err != nil {
			return nil, err
		}
		if err := cat.CreateUser(s.Name, *s.NewPassword, existing.Roles); err != nil {
			return nil, err
		}
		if existing.Disabled {
			if err := cat.SetUserDisabled(s.Name, true); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func executeCreateRole(env ExecEnv, s *CreateRole) (*ResultSet, error) {
	if err := env.db.Catalog().CreateRole(s.Name); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeDropRole(env ExecEnv, s *DropRole) (*ResultSet, error) {
	if err := env.db.Catalog().DropRole(s.Name); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeGrantPrivilege(env ExecEnv, s *GrantPrivilege) (*ResultSet, error) {
	for _, perm := range s.Permissions {
		if err := env.db.Catalog().GrantPermission(s.RoleName, perm, s.Schema, s.Table); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func executeRevokePrivilege(env ExecEnv, s *RevokePrivilege) (*ResultSet, error) {
	for _, perm := range s.Permissions {
		if err := env.db.Catalog().RevokePermission(s.RoleName, perm, s.Schema, s.Table); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func executeGrantRoleStmt(env ExecEnv, s *GrantRoleStmt) (*ResultSet, error) {
	if err := env.db.Catalog().GrantRoleToUser(s.UserName, s.RoleName); err != nil {
		return nil, err
	}
	return nil, nil
}

func executeRevokeRoleStmt(env ExecEnv, s *RevokeRoleStmt) (*ResultSet, error) {
	if err := env.db.Catalog().RevokeRoleFromUser(s.UserName, s.RoleName); err != nil {
		return nil, err
	}
	return nil, nil
}
