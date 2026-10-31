package tinysql

import (
	"context"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Permission enumerates RBAC operations that can be granted to roles.
type Permission = storage.Permission

// Grant authorizes a Permission on a schema/table pair, with "*" as wildcard.
type Grant = storage.Grant

// CatalogRole is a named RBAC role and its grants.
type CatalogRole = storage.CatalogRole

// CatalogUser is an RBAC user principal. PasswordHash is opaque.
type CatalogUser = storage.CatalogUser

// CatalogManager stores schema, RBAC, function, trigger, view, and job metadata.
type CatalogManager = storage.CatalogManager

// CatalogTable describes a table registered in the catalog.
type CatalogTable = storage.CatalogTable

// CatalogColumn describes a table column registered in the catalog.
type CatalogColumn = storage.CatalogColumn

// CatalogView describes a logical SQL view.
type CatalogView = storage.CatalogView

// CatalogMaterializedView describes a cached materialized view.
type CatalogMaterializedView = storage.CatalogMaterializedView

// CatalogDependency records a dependency between catalog objects.
type CatalogDependency = storage.CatalogDependency

// CatalogFunction describes a scalar, aggregate, or table function.
type CatalogFunction = storage.CatalogFunction

// CatalogTrigger describes a trigger registered in the catalog.
type CatalogTrigger = storage.CatalogTrigger

const (
	PermSelect Permission = storage.PermSelect
	PermInsert Permission = storage.PermInsert
	PermUpdate Permission = storage.PermUpdate
	PermDelete Permission = storage.PermDelete
	PermDDL    Permission = storage.PermDDL
	PermAll    Permission = storage.PermAll
)

// WithUser returns a context carrying the acting username for RBAC
// permission checks (see CREATE USER/CREATE ROLE/GRANT below). Pass the
// result to Execute/ExecuteCompiled in place of a plain context.
//
// RBAC is opt-in: until the first CREATE USER statement runs against a
// database, every Execute call is permitted regardless of context — so
// existing code that never touches users/roles sees no behavior change.
// Once a user exists, every subsequent Execute call requires one via
// WithUser, or it's rejected with an "access denied" error.
//
// Example:
//
//	tinysql.Execute(ctx, db, "default", mustParse(`CREATE ROLE admin_role`))
//	tinysql.Execute(ctx, db, "default", mustParse(`GRANT ALL ON * TO ROLE admin_role`))
//	tinysql.Execute(ctx, db, "default", mustParse(`CREATE USER admin WITH PASSWORD 'x' ROLE admin_role`))
//	// RBAC is now active; every further call needs an authenticated context:
//	adminCtx := tinysql.WithUser(ctx, "admin")
//	tinysql.Execute(adminCtx, db, "default", stmt)
func WithUser(ctx context.Context, username string) context.Context {
	return engine.WithUser(ctx, username)
}

// UserFromContext returns the username set by WithUser, if any.
func UserFromContext(ctx context.Context) (string, bool) {
	return engine.UserFromContext(ctx)
}

// ParsePermission validates and normalizes an RBAC permission keyword.
func ParsePermission(s string) (Permission, error) {
	return storage.ParsePermission(s)
}
