package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// OpenConfig describes connection, DSN and database/sql settings for OpenWithConfig.
//
// Best-practice split:
//   - DSN fields configure tinySQL driver/server behavior (tenant, autosave, pools, busy_timeout).
//   - database/sql fields configure connection pool behavior (max open/idle/lifetimes).
//   - PingTimeout validates startup connectivity with PingContext.
type OpenConfig struct {
	// Mode controls DSN scheme. Allowed values: "mem" (default) or "file".
	Mode string
	// FilePath is required when Mode is "file".
	FilePath string
	// Tenant maps to DSN option `tenant`. Empty falls back to "default".
	Tenant string
	// Autosave maps to DSN option `autosave=1` for file mode.
	Autosave bool
	// PoolReaders maps to DSN option `pool_readers`.
	PoolReaders int
	// PoolWriters maps to DSN option `pool_writers`.
	PoolWriters int
	// BusyTimeout maps to DSN option `busy_timeout` (e.g. 250ms, 2s).
	BusyTimeout time.Duration

	// database/sql pool settings.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// PingTimeout is used for startup health-check in OpenWithConfig.
	PingTimeout time.Duration
}

// DefaultOpenConfig returns sensible defaults for tinySQL.
func DefaultOpenConfig() OpenConfig {
	return OpenConfig{
		Mode:            "mem",
		Tenant:          "default",
		PoolReaders:     4,
		PoolWriters:     1,
		BusyTimeout:     250 * time.Millisecond,
		MaxOpenConns:    8,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		PingTimeout:     5 * time.Second,
	}
}

// DSN builds a tinySQL DSN from OpenConfig.
func (c OpenConfig) DSN() (string, error) {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		mode = "mem"
	}
	if err := validateOpenConfig(c, mode); err != nil {
		return "", err
	}

	tenant := strings.TrimSpace(c.Tenant)
	if tenant == "" {
		tenant = "default"
	}

	q := url.Values{}
	q.Set("tenant", tenant)
	if c.Autosave && mode == "file" {
		q.Set("autosave", "1")
	}
	if c.PoolReaders > 0 {
		q.Set("pool_readers", strconv.Itoa(c.PoolReaders))
	}
	if c.PoolWriters > 0 {
		q.Set("pool_writers", strconv.Itoa(c.PoolWriters))
	}
	if c.BusyTimeout > 0 {
		q.Set("busy_timeout", c.BusyTimeout.String())
	}

	if mode == "file" {
		return "file:" + filepath.Clean(c.FilePath) + "?" + q.Encode(), nil
	}
	return "mem://?" + q.Encode(), nil
}

// OpenWithConfig opens a tinySQL database using explicit settings and validates
// connectivity with PingContext.
func OpenWithConfig(ctx context.Context, cfg OpenConfig) (*sql.DB, error) {
	dsn, err := cfg.DSN()
	if err != nil {
		return nil, err
	}
	db, err := Open(dsn)
	if err != nil {
		return nil, err
	}

	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	pingCtx := ctx
	cancel := func() {}
	if cfg.PingTimeout > 0 {
		pingCtx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
	}
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func validateOpenConfig(c OpenConfig, mode string) error {
	switch mode {
	case "mem", "file":
	default:
		return fmt.Errorf("tinysql: unsupported mode %q (use mem or file)", c.Mode)
	}
	if mode == "file" && strings.TrimSpace(c.FilePath) == "" {
		return fmt.Errorf("tinysql: file mode requires FilePath")
	}
	if c.PoolReaders < 0 {
		return fmt.Errorf("tinysql: PoolReaders must be >= 0")
	}
	if c.PoolWriters < 0 {
		return fmt.Errorf("tinysql: PoolWriters must be >= 0")
	}
	if c.BusyTimeout < 0 {
		return fmt.Errorf("tinysql: BusyTimeout must be >= 0")
	}
	if c.MaxOpenConns < 0 {
		return fmt.Errorf("tinysql: MaxOpenConns must be >= 0")
	}
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("tinysql: MaxIdleConns must be >= 0")
	}
	if c.ConnMaxLifetime < 0 {
		return fmt.Errorf("tinysql: ConnMaxLifetime must be >= 0")
	}
	if c.ConnMaxIdleTime < 0 {
		return fmt.Errorf("tinysql: ConnMaxIdleTime must be >= 0")
	}
	if c.PingTimeout < 0 {
		return fmt.Errorf("tinysql: PingTimeout must be >= 0")
	}
	return nil
}
