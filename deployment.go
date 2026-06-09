package tinysql

import (
	"context"
	"fmt"
	"strings"
)

// DeploymentMode describes the intended runtime shape for a tinySQL instance.
// It is additive metadata and does not change existing NewDB/OpenDB behavior.
type DeploymentMode string

const (
	// DeploymentPackage is the lightweight in-process Go package profile.
	DeploymentPackage DeploymentMode = "package"
	// DeploymentEmbedded is the SQLite-like local embedded database profile.
	DeploymentEmbedded DeploymentMode = "embedded"
	// DeploymentServer is the networked DBMS server profile.
	DeploymentServer DeploymentMode = "server"
	// DeploymentEnterprise is the durable server profile with enterprise services enabled by default.
	DeploymentEnterprise DeploymentMode = "enterprise"
)

// String returns the stable string identifier for a deployment mode.
func (m DeploymentMode) String() string {
	if m == "" {
		return string(DeploymentPackage)
	}
	return string(m)
}

// ParseDeploymentMode parses a deployment mode string.
func ParseDeploymentMode(s string) (DeploymentMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "package", "pkg", "library", "lib":
		return DeploymentPackage, nil
	case "embedded", "embed", "sqlite", "sqlite-like", "local":
		return DeploymentEmbedded, nil
	case "server", "dbms", "daemon":
		return DeploymentServer, nil
	case "enterprise", "ent":
		return DeploymentEnterprise, nil
	default:
		return DeploymentPackage, fmt.Errorf("unknown deployment mode %q (valid: package, embedded, server, enterprise)", s)
	}
}

// DeploymentConfig configures a higher-level tinySQL product form.
// Existing callers can continue using NewDB/OpenDB directly; this wrapper is
// for applications that want an explicit package/embedded/server/enterprise shape.
type DeploymentConfig struct {
	Mode              DeploymentMode
	Tenant            string
	Storage           StorageConfig
	StartJobScheduler bool
	JobExecutor       JobExecutor
}

// Instance is a thin runtime wrapper around DB with deployment metadata.
type Instance struct {
	DB     *DB
	Mode   DeploymentMode
	Tenant string
}

// OpenDeployment opens a tinySQL instance using the supplied deployment config.
func OpenDeployment(cfg DeploymentConfig) (*Instance, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = DeploymentPackage
	}
	tenant := cfg.Tenant
	if tenant == "" {
		tenant = "default"
	}

	db, err := OpenDB(cfg.Storage)
	if err != nil {
		return nil, err
	}

	if cfg.StartJobScheduler {
		executor := cfg.JobExecutor
		if executor == nil {
			executor = NewSQLJobExecutor(db, tenant)
		}
		if err := db.StartJobScheduler(executor); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return &Instance{DB: db, Mode: mode, Tenant: tenant}, nil
}

// OpenPackage creates an in-memory package-mode instance.
func OpenPackage(tenant string) *Instance {
	if tenant == "" {
		tenant = "default"
	}
	return &Instance{DB: NewDB(), Mode: DeploymentPackage, Tenant: tenant}
}

// OpenEmbedded opens a SQLite-like embedded instance at path using WAL storage.
func OpenEmbedded(path, tenant string) (*Instance, error) {
	if path == "" {
		return nil, fmt.Errorf("embedded deployment requires a path")
	}
	return OpenDeployment(DeploymentConfig{
		Mode:   DeploymentEmbedded,
		Tenant: tenant,
		Storage: StorageConfig{
			Mode: ModeWAL,
			Path: path,
		},
	})
}

// OpenServer opens a server-profile instance. The caller decides storage policy.
func OpenServer(cfg StorageConfig, tenant string) (*Instance, error) {
	return OpenDeployment(DeploymentConfig{
		Mode:              DeploymentServer,
		Tenant:            tenant,
		Storage:           cfg,
		StartJobScheduler: true,
	})
}

// OpenEnterprise opens an enterprise-profile instance with durable storage required.
func OpenEnterprise(cfg StorageConfig, tenant string) (*Instance, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("enterprise deployment requires durable storage path")
	}
	if cfg.Mode == ModeMemory {
		return nil, fmt.Errorf("enterprise deployment requires durable storage mode")
	}
	return OpenDeployment(DeploymentConfig{
		Mode:              DeploymentEnterprise,
		Tenant:            tenant,
		Storage:           cfg,
		StartJobScheduler: true,
	})
}

// ExecuteSQL parses and executes one SQL statement against the instance tenant.
func (i *Instance) ExecuteSQL(ctx context.Context, sql string) (*ResultSet, error) {
	if i == nil || i.DB == nil {
		return nil, fmt.Errorf("nil tinySQL instance")
	}
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return Execute(ctx, i.DB, i.Tenant, stmt)
}

// Close releases instance resources.
func (i *Instance) Close() error {
	if i == nil || i.DB == nil {
		return nil
	}
	return i.DB.Close()
}
