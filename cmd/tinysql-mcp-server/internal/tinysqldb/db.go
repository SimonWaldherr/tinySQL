// Package tinysqldb manages tinySQL database connections for the MCP server.
// It opens a database/sql handle using the public tinySQL driver and exposes
// the tenant and DSN used by the server.
package tinysqldb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/driver" // registers the "tinysql" driver
)

// Config holds configuration for opening a tinySQL database.
type Config struct {
	// DSN is the full tinySQL connection string, e.g.
	//   mem://?tenant=default
	//   file:./data/db.dat?tenant=default&autosave=1
	//
	// If DSN is non-empty it takes precedence over DBPath.
	DSN string

	// DBPath is a convenience shorthand for a file-backed database.  If DSN is
	// empty the server constructs a file: DSN from DBPath, Tenant and Autosave.
	DBPath string

	// Tenant selects the tinySQL tenant namespace.  Defaults to "default".
	Tenant string

	// Autosave enables automatic file persistence for file-backed databases.
	Autosave bool

	// QueryTimeout is the per-query execution timeout.  0 means no timeout.
	QueryTimeout time.Duration

	// MaxRows is the maximum number of rows returned by read_query.
	// 0 means unlimited.
	MaxRows int

	// ReadOnly blocks mutating tools when true.
	ReadOnly bool
}

// Store wraps a *sql.DB and the resolved tenant name.
type Store struct {
	DB     *sql.DB
	Tenant string
	Cfg    Config
}

// Open opens a tinySQL database using the provided Config and returns a Store.
// If both DSN and DBPath are set, DSN takes precedence and DBPath is ignored.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	dsn, tenant, err := resolveDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("tinysqldb: open %q: %w", safeDSN(dsn), err)
	}

	pingCtx := ctx
	if pingCtx == nil {
		pingCtx = context.Background()
	}
	pingCtx, cancel := context.WithTimeout(pingCtx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("tinysqldb: ping %q: %w", safeDSN(dsn), err)
	}

	cfg.Tenant = tenant
	return &Store{DB: db, Tenant: tenant, Cfg: cfg}, nil
}

// Close releases the underlying database connection.
func (s *Store) Close() error {
	return s.DB.Close()
}

// resolveDSN derives the final DSN string and tenant from cfg.
func resolveDSN(cfg Config) (dsn, tenant string, err error) {
	tenant = strings.TrimSpace(cfg.Tenant)
	if tenant == "" {
		tenant = "default"
	}

	if cfg.DSN != "" {
		dsn = cfg.DSN
		// Extract tenant from DSN query params if not explicitly set by caller.
		if t := tenantFromDSN(cfg.DSN); t != "" && strings.TrimSpace(cfg.Tenant) == "" {
			tenant = t
		}
		return dsn, tenant, nil
	}

	if cfg.DBPath != "" {
		q := url.Values{}
		q.Set("tenant", tenant)
		if cfg.Autosave {
			q.Set("autosave", "1")
		}
		dsn = "file:" + cfg.DBPath + "?" + q.Encode()
		return dsn, tenant, nil
	}

	// Default: in-memory database.
	q := url.Values{}
	q.Set("tenant", tenant)
	dsn = "mem://?" + q.Encode()
	return dsn, tenant, nil
}

// tenantFromDSN parses the tenant query parameter from a tinySQL DSN string.
func tenantFromDSN(dsn string) string {
	i := strings.Index(dsn, "?")
	if i < 0 {
		return ""
	}
	q, err := url.ParseQuery(dsn[i+1:])
	if err != nil {
		return ""
	}
	return q.Get("tenant")
}

// safeDSN returns a sanitized DSN suitable for logging (no credentials).
// tinySQL DSNs do not carry passwords, but we strip query parameters anyway.
func safeDSN(dsn string) string {
	i := strings.Index(dsn, "?")
	if i < 0 {
		return dsn
	}
	return dsn[:i] + "?..."
}
