// Driver and connector: one sql.Open creates one connector, which lazily opens
// one server. Every physical connection from that *sql.DB shares it, so
// transactions and prepared statements are the only per-connection state.
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// drv is the globally registered database/sql Driver. srv is intentionally
// reserved for the legacy empty-DSN embedding API (SetDefaultDB). It is not a
// cache for arbitrary DSNs: sharing it for mem:// or file: caused independent
// sql.Open calls to see the wrong database and, before Connector support,
// opening a physical connection could construct another full storage.DB.
type drv struct {
	mu  sync.RWMutex
	srv *server
}

var _ driver.DriverContext = (*drv)(nil)

// connector belongs to exactly one sql.Open call. sync.Once makes server
// creation lazy and guarantees that all physical connections allocated by that
// *sql.DB share one server and one storage.DB.
type connector struct {
	driver *drv
	cfg    cfg

	once       sync.Once
	srv        *server
	ownsServer bool // true when this connector opened srv.db and must close it
	err        error
}

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

// serverOpenHook is intentionally package-private test instrumentation. It
// observes actual storage.DB construction without becoming a production API.
var serverOpenHook struct {
	sync.RWMutex
	fn func(*storage.DB, cfg)
}

func notifyServerOpen(db *storage.DB, c cfg) {
	serverOpenHook.RLock()
	fn := serverOpenHook.fn
	serverOpenHook.RUnlock()
	if fn != nil {
		fn(db, c)
	}
}

// OpenConnector is the database/sql DriverContext entry point. database/sql
// calls it once per sql.Open, rather than calling Open once per physical
// connection, which is the ownership boundary required for bounded storage.
func (d *drv) OpenConnector(name string) (driver.Connector, error) {
	c, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	return &connector{driver: d, cfg: c}, nil
}

// Open remains for callers using driver.Driver directly. Normal database/sql
// use takes OpenConnector above.
func (d *drv) Open(name string) (driver.Conn, error) {
	// Keep the historical direct-driver embedding behavior: a caller that
	// constructs drv{srv: ...} owns that server explicitly. database/sql does
	// not use this branch because drv implements DriverContext.
	if c, err := parseDSN(name); err != nil {
		return nil, err
	} else {
		d.mu.RLock()
		s := d.srv
		d.mu.RUnlock()
		if s != nil {
			return &conn{srv: s, tenant: c.tenant}, nil
		}
	}
	c, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (c *connector) Driver() driver.Driver { return c.driver }

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	c.once.Do(func() {
		c.srv, c.err = c.openServer()
	})
	if c.err != nil {
		return nil, c.err
	}
	return &conn{srv: c.srv, tenant: c.cfg.tenant}, nil
}

func (c *connector) openServer() (*server, error) {
	// Preserve SetDefaultDB/OpenWithDB for the one historical empty-DSN path,
	// but never let it leak into named in-memory or file DSNs.
	if c.cfg.defaultDSN {
		c.driver.mu.RLock()
		s := c.driver.srv
		c.driver.mu.RUnlock()
		if s != nil {
			return s, nil
		}
	}

	var (
		db  *storage.DB
		err error
	)
	switch {
	case c.cfg.modeSet:
		if c.cfg.mode != storage.ModeMemory && c.cfg.filePath == "" {
			return nil, fmt.Errorf("tinysql: mode=%s requires a file: DSN with a path", c.cfg.mode)
		}
		sc := storage.DefaultStorageConfig(c.cfg.mode)
		sc.Path = c.cfg.filePath
		sc.MaxMemoryBytes = c.cfg.maxMemoryBytes
		sc.ReadOnly = c.cfg.readOnly
		sc.SyncOnMutate = c.cfg.syncOnMutate
		sc.CompressFiles = c.cfg.compressFiles
		sc.CheckpointEvery = c.cfg.checkpointEvery
		sc.CheckpointInterval = c.cfg.checkpointInterval
		sc.CheckpointMaxBytes = c.cfg.checkpointMaxBytes
		db, err = storage.OpenDB(sc)
	case c.cfg.filePath != "":
		if c.cfg.readOnly {
			return nil, fmt.Errorf("tinysql: read_only requires an explicit persistent mode (disk, index, hybrid, wal, advanced_wal, or json)")
		}
		db, err = storage.LoadFromFile(c.cfg.filePath)
	default:
		db = storage.NewDB()
		if c.cfg.readOnly {
			db.SetReadOnly(true)
		}
	}
	if err != nil {
		return nil, err
	}
	notifyServerOpen(db, c.cfg)
	c.ownsServer = true
	return newServer(db, c.cfg), nil
}

// Close releases the storage.DB this connector opened. database/sql invokes it
// from sql.DB.Close() because *connector implements io.Closer; without it the
// underlying DB (and any paged-index/WAL file handles or job scheduler) would
// leak until process exit. The default-DSN path returns a driver-owned server,
// which this connector does not own and must not close.
func (c *connector) Close() error {
	if !c.ownsServer || c.srv == nil || c.srv.db == nil {
		return nil
	}
	// flushPersist is a no-op whenever nothing is owed — in particular
	// whenever persist_debounce_ms was never set — so this unconditional
	// call never changes behavior for anyone not using that option. For a
	// connection that opted into debouncing, it forces any sync still
	// pending inside the current window to happen now, so a clean
	// sql.DB.Close() never leaves a debounced write unflushed: db.Close()
	// below only performs its own Sync for storage-backend modes (see
	// DB.Sync), and never for the legacy autosave-to-file scheme, so
	// skipping this would silently lose the last debounce window's write
	// for that scheme.
	flushErr := c.srv.flushPersist()
	if flushErr != nil {
		log.Printf("tinysql: final flush before close failed: %v", flushErr)
	}
	if err := c.srv.db.Close(); err != nil {
		return err
	}
	return flushErr
}
