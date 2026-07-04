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
// no-op unless db.Catalog().HasUsers() is true, so every existing embedder
// and test that never creates a user sees zero behavior change.
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
// immediately if db has no users defined (RBAC off) — see the package doc
// comment above and storage.CatalogManager.HasUsers.
func checkPermission(ctx context.Context, db *storage.DB, stmt Statement) error {
	if !db.Catalog().HasUsers() {
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
	}
	return "", "", "", false
}
