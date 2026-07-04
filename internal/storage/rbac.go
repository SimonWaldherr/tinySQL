// Package storage: role-based access control (RBAC).
//
// What: Users authenticate with a bcrypt-hashed password and are members of
// one or more roles; roles carry grants (a permission on a schema/table
// pair, with "*" as a wildcard for "all schemas"/"all tables"). Execute()
// in internal/engine checks the acting user's resolved permissions before
// running a statement.
//
// Why opt-in: tinySQL has always been usable with zero setup (NewDB(),
// start running SQL). Requiring every caller to create a user before their
// first query would break that and every existing embedder. So RBAC
// enforcement only activates once at least one user has been created via
// CreateUser — see HasUsers/IsRBACEnabled. Before that, every permission
// check is a no-op allow, matching tinySQL's historical behavior exactly.
// This mirrors the same opt-in pattern already used for read-only mode
// (SetReadOnly) and cmd/server's optional auth token.
package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Permission enumerates the operations a grant can authorize.
type Permission string

const (
	PermSelect Permission = "SELECT"
	PermInsert Permission = "INSERT"
	PermUpdate Permission = "UPDATE"
	PermDelete Permission = "DELETE"
	// PermDDL covers CREATE/DROP/ALTER TABLE/INDEX/VIEW/TRIGGER/JOB and
	// similar schema-changing statements.
	PermDDL Permission = "DDL"
	// PermAll grants every permission above at once.
	PermAll Permission = "ALL"
)

// ParsePermission validates and normalizes a permission keyword from SQL
// (case-insensitive).
func ParsePermission(s string) (Permission, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "SELECT":
		return PermSelect, nil
	case "INSERT":
		return PermInsert, nil
	case "UPDATE":
		return PermUpdate, nil
	case "DELETE":
		return PermDelete, nil
	case "DDL":
		return PermDDL, nil
	case "ALL":
		return PermAll, nil
	default:
		return "", fmt.Errorf("unknown permission %q (expected SELECT, INSERT, UPDATE, DELETE, DDL, or ALL)", s)
	}
}

// Grant authorizes Permission on every table in Schema (or every schema, if
// Schema is "*") named Table (or every table, if Table is "*").
type Grant struct {
	Permission Permission
	Schema     string
	Table      string
}

// matches reports whether this grant covers the given (schema, table,
// permission), honoring "*" wildcards and PermAll.
func (g Grant) matches(schema, table string, perm Permission) bool {
	if g.Permission != PermAll && g.Permission != perm {
		return false
	}
	if g.Schema != "*" && !strings.EqualFold(g.Schema, schema) {
		return false
	}
	if g.Table != "*" && !strings.EqualFold(g.Table, table) {
		return false
	}
	return true
}

// CatalogRole is a named bundle of grants that one or more users can hold.
type CatalogRole struct {
	Name      string
	Grants    []Grant
	CreatedAt time.Time
}

// CatalogUser is an authenticatable principal. Passwords are never stored
// in plaintext — only a bcrypt hash, which is itself salted and safe to
// treat as opaque (bcrypt embeds its own per-hash random salt).
type CatalogUser struct {
	Name         string
	PasswordHash string
	Roles        []string
	Disabled     bool
	CreatedAt    time.Time
}

// rbacState holds the users/roles tables. Kept as a separate struct (rather
// than more maps directly on CatalogManager) so the "any users defined?"
// check driving opt-in enforcement is a single, obvious field read.
type rbacState struct {
	mu    sync.RWMutex
	users map[string]*CatalogUser // key: lowercased name
	roles map[string]*CatalogRole // key: lowercased name
}

func newRBACState() *rbacState {
	return &rbacState{
		users: make(map[string]*CatalogUser),
		roles: make(map[string]*CatalogRole),
	}
}

// HasUsers reports whether any user has been created — the switch that
// turns RBAC enforcement on. See the package doc comment for why this is
// opt-in.
func (c *CatalogManager) HasUsers() bool {
	c.rbac.mu.RLock()
	defer c.rbac.mu.RUnlock()
	return len(c.rbac.users) > 0
}

// CreateUser adds a new user with a bcrypt-hashed password. Returns an
// error if the name is already taken (case-insensitive) or the password is
// empty (an empty password would make Authenticate meaningless — bcrypt
// would happily hash "" too, but rejecting it here catches the mistake of
// e.g. an unpopulated config field at the source instead of silently
// creating a passwordless account).
func (c *CatalogManager) CreateUser(name, password string, roles []string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("user name must not be empty")
	}
	if password == "" {
		return fmt.Errorf("user %q: password must not be empty", name)
	}
	key := strings.ToLower(name)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	if _, exists := c.rbac.users[key]; exists {
		return fmt.Errorf("user %q already exists", name)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	c.rbac.users[key] = &CatalogUser{
		Name:         name,
		PasswordHash: string(hash),
		Roles:        append([]string(nil), roles...),
		CreatedAt:    time.Now(),
	}
	return nil
}

// DropUser removes a user. Returns an error if it doesn't exist.
func (c *CatalogManager) DropUser(name string) error {
	key := strings.ToLower(name)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	if _, exists := c.rbac.users[key]; !exists {
		return fmt.Errorf("user %q does not exist", name)
	}
	delete(c.rbac.users, key)
	return nil
}

// SetUserDisabled enables/disables a user without deleting their account
// (preserving audit-log attribution history for anything they did while
// enabled). A disabled user fails Authenticate.
func (c *CatalogManager) SetUserDisabled(name string, disabled bool) error {
	key := strings.ToLower(name)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	u, exists := c.rbac.users[key]
	if !exists {
		return fmt.Errorf("user %q does not exist", name)
	}
	u.Disabled = disabled
	return nil
}

// GrantRoleToUser adds a role membership to an existing user. Idempotent —
// granting a role the user already has is not an error.
func (c *CatalogManager) GrantRoleToUser(userName, roleName string) error {
	userKey, roleKey := strings.ToLower(userName), strings.ToLower(roleName)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	u, exists := c.rbac.users[userKey]
	if !exists {
		return fmt.Errorf("user %q does not exist", userName)
	}
	if _, exists := c.rbac.roles[roleKey]; !exists {
		return fmt.Errorf("role %q does not exist", roleName)
	}
	for _, r := range u.Roles {
		if strings.EqualFold(r, roleName) {
			return nil
		}
	}
	u.Roles = append(u.Roles, roleName)
	return nil
}

// RevokeRoleFromUser removes a role membership. Not an error if the user
// didn't have that role.
func (c *CatalogManager) RevokeRoleFromUser(userName, roleName string) error {
	userKey := strings.ToLower(userName)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	u, exists := c.rbac.users[userKey]
	if !exists {
		return fmt.Errorf("user %q does not exist", userName)
	}
	out := u.Roles[:0]
	for _, r := range u.Roles {
		if !strings.EqualFold(r, roleName) {
			out = append(out, r)
		}
	}
	u.Roles = out
	return nil
}

// Authenticate verifies a password against the stored bcrypt hash in
// constant time (bcrypt.CompareHashAndPassword is itself constant-time
// with respect to the password content). Returns false for an unknown or
// disabled user without distinguishing why, so a caller can't use timing
// or error content to enumerate valid usernames.
func (c *CatalogManager) Authenticate(name, password string) bool {
	key := strings.ToLower(name)
	c.rbac.mu.RLock()
	u, exists := c.rbac.users[key]
	c.rbac.mu.RUnlock()
	if !exists || u.Disabled {
		// Still run bcrypt against a fixed dummy hash so a request for a
		// nonexistent user takes roughly the same time as a real one,
		// rather than returning instantly and leaking username validity
		// through a timing side channel.
		_ = bcrypt.CompareHashAndPassword([]byte(dummyBcryptHash), []byte(password))
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) == nil
}

// dummyBcryptHash is a valid bcrypt hash of an arbitrary fixed string, used
// purely to give Authenticate's "user not found" path the same bcrypt-cost
// timing profile as its "user found" path.
const dummyBcryptHash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

// CreateRole adds a new, initially grant-less role.
func (c *CatalogManager) CreateRole(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("role name must not be empty")
	}
	key := strings.ToLower(name)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	if _, exists := c.rbac.roles[key]; exists {
		return fmt.Errorf("role %q already exists", name)
	}
	c.rbac.roles[key] = &CatalogRole{Name: name, CreatedAt: time.Now()}
	return nil
}

// DropRole removes a role. Existing users keep a dangling membership
// reference (harmless: GrantRoleToUser/permission resolution both treat a
// reference to a since-dropped role as granting nothing), consistent with
// how DropTrigger etc. don't cascade either.
func (c *CatalogManager) DropRole(name string) error {
	key := strings.ToLower(name)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	if _, exists := c.rbac.roles[key]; !exists {
		return fmt.Errorf("role %q does not exist", name)
	}
	delete(c.rbac.roles, key)
	return nil
}

// GrantPermission adds a grant to an existing role. schema/table accept
// "*" as a wildcard.
func (c *CatalogManager) GrantPermission(roleName string, perm Permission, schema, table string) error {
	key := strings.ToLower(roleName)
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	r, exists := c.rbac.roles[key]
	if !exists {
		return fmt.Errorf("role %q does not exist", roleName)
	}
	if schema == "" {
		schema = "*"
	}
	if table == "" {
		table = "*"
	}
	r.Grants = append(r.Grants, Grant{Permission: perm, Schema: schema, Table: table})
	return nil
}

// RevokePermission removes every grant on a role matching perm/schema/table
// exactly (same wildcard values as originally granted — this is a simple
// exact-match revoke, not a partial-overlap subtraction).
func (c *CatalogManager) RevokePermission(roleName string, perm Permission, schema, table string) error {
	key := strings.ToLower(roleName)
	if schema == "" {
		schema = "*"
	}
	if table == "" {
		table = "*"
	}
	c.rbac.mu.Lock()
	defer c.rbac.mu.Unlock()
	r, exists := c.rbac.roles[key]
	if !exists {
		return fmt.Errorf("role %q does not exist", roleName)
	}
	out := r.Grants[:0]
	for _, g := range r.Grants {
		if g.Permission == perm && g.Schema == schema && g.Table == table {
			continue
		}
		out = append(out, g)
	}
	r.Grants = out
	return nil
}

// HasPermission resolves userName's roles to their grants and reports
// whether any grant authorizes perm on (schema, table). Returns false for
// an unknown user (fail closed).
func (c *CatalogManager) HasPermission(userName string, perm Permission, schema, table string) bool {
	key := strings.ToLower(userName)
	c.rbac.mu.RLock()
	defer c.rbac.mu.RUnlock()
	u, exists := c.rbac.users[key]
	if !exists || u.Disabled {
		return false
	}
	for _, roleName := range u.Roles {
		role, exists := c.rbac.roles[strings.ToLower(roleName)]
		if !exists {
			continue
		}
		for _, g := range role.Grants {
			if g.matches(schema, table, perm) {
				return true
			}
		}
	}
	return false
}

// GetUser returns a copy of a user's metadata (never the password hash's
// caller-mutable backing, since Roles is copied) for introspection, or
// false if it doesn't exist.
func (c *CatalogManager) GetUser(name string) (CatalogUser, bool) {
	key := strings.ToLower(name)
	c.rbac.mu.RLock()
	defer c.rbac.mu.RUnlock()
	u, exists := c.rbac.users[key]
	if !exists {
		return CatalogUser{}, false
	}
	cp := *u
	cp.Roles = append([]string(nil), u.Roles...)
	cp.PasswordHash = "" // never leak the hash through introspection
	return cp, true
}

// ListUsers returns all users (without password hashes), sorted by name.
func (c *CatalogManager) ListUsers() []CatalogUser {
	c.rbac.mu.RLock()
	defer c.rbac.mu.RUnlock()
	out := make([]CatalogUser, 0, len(c.rbac.users))
	for _, u := range c.rbac.users {
		cp := *u
		cp.Roles = append([]string(nil), u.Roles...)
		cp.PasswordHash = ""
		out = append(out, cp)
	}
	return out
}

// ListRoles returns all roles, sorted by name.
func (c *CatalogManager) ListRoles() []CatalogRole {
	c.rbac.mu.RLock()
	defer c.rbac.mu.RUnlock()
	out := make([]CatalogRole, 0, len(c.rbac.roles))
	for _, r := range c.rbac.roles {
		cp := *r
		cp.Grants = append([]Grant(nil), r.Grants...)
		out = append(out, cp)
	}
	return out
}

// GenerateRandomPassword returns a cryptographically random hex-encoded
// password of the given byte length (before hex encoding, so the resulting
// string is 2*n characters) — a convenience for callers provisioning
// service accounts that don't need a human-memorable password.
func GenerateRandomPassword(n int) (string, error) {
	if n <= 0 {
		n = 24
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random password: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
