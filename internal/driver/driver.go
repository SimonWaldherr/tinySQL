// Package driver provides a lightweight database/sql driver for tinySQL.
//
// The driver exposes tinySQL through the standard `database/sql` API and
// supports both in-memory and file-backed databases. Key features:
//
//   - DSN formats: `mem://` and `file:/path/to/db.gob?options` (see `parseDSN`).
//   - Optional Write-Ahead Log (WAL) and autosave for durability.
//   - `mode=` selects any storage.StorageMode (disk, json, hybrid, wal, ...)
//     via storage.OpenDB instead of the default GOB-snapshot behavior.
//   - Reader/writer pools and simple MVCC-style snapshots for transactions.
//   - Simple, safe placeholder binding: sequential `?` and numbered `$1`/`:1`.
//
// Use `sql.Open("tinysql", dsn)` to create a connection. Each sql.Open call
// creates one Connector which owns one lazily opened server/storage.DB. The
// physical connections subsequently created by database/sql share that server,
// while transactions and prepared statements remain connection-local. Separate
// sql.Open calls never share a DSN-backed database instance implicitly.
//
// See applyDSNOption and applyQueryOptions for available DSN options and
// defaults.
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// init registers the "tinysql" driver and pre-registers common GOB types.
// This enables database/sql.Open("tinysql", dsn) to work out of the box.
// Supported DSNs:
//   - mem://?tenant=default&pool_readers=4&busy_timeout=250ms
//   - file:/path/to/db.gob?tenant=default&autosave=1
//   - file:/path/to/dbdir?tenant=default&mode=json (or disk, hybrid, wal, ...)
//
// See parseDSN for all available options.
// defaultDrv is the package-global driver instance registered with database/sql.
var defaultDrv = &drv{}

// ErrTransactionConflict reports that a transaction attempted to commit a
// table that was changed after its snapshot was taken. Callers can use
// errors.Is to distinguish a retryable write conflict from other failures.
var ErrTransactionConflict = errors.New("tinysql: transaction conflict")

func init() {
	sql.Register("tinysql", defaultDrv)
	gob.Register(map[string]any{})
	gob.Register([]any{})
}

// SetDefaultDB allows external code to provide a storage.DB instance that will
// be used by the driver when opening connections. This is useful for embedding
// environments (WASM) that want to keep a reference to the underlying DB.
func SetDefaultDB(db *storage.DB) {
	if db == nil {
		return
	}
	// Create a default cfg with sane defaults
	c := cfg{
		tenant:      "default",
		maxReaders:  4,
		maxWriters:  1,
		busyTimeout: 250 * time.Millisecond,
	}
	// Pre-create server using provided DB so subsequent Open() calls reuse it.
	// Note: This allows embedding consumers to control the underlying DB
	// instance (for example tests or WASM hosts) while still using the
	// database/sql API.
	defaultDrv.mu.Lock()
	defaultDrv.srv = newServer(db, c)
	defaultDrv.mu.Unlock()
}

// CurrentDefaultDB returns the storage database currently backing the default
// driver server, if one exists.
func CurrentDefaultDB() *storage.DB {
	defaultDrv.mu.RLock()
	srv := defaultDrv.srv
	defaultDrv.mu.RUnlock()
	if srv == nil {
		return nil
	}
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.db
}

// OpenInMemory returns a *sql.DB backed by an in-memory tinySQL server.
// If tenant is empty the default tenant is used.
func OpenInMemory(tenant string) (*sql.DB, error) {
	dsn := "mem://"
	if tenant != "" {
		dsn += "?tenant=" + tenant
	}
	return sql.Open("tinysql", dsn)
}

// OpenInMemory is a convenience wrapper that returns a *sql.DB connected to
// an in-memory tinySQL server. Use this for tests and short-lived in-memory
// databases. The returned *sql.DB should be closed by the caller when done.

// cfg stores the connection parameters derived from a parsed DSN.
type cfg struct {
	// defaultDSN is true only for the empty DSN used by the legacy embedding
	// helpers. Named mem:// and file: DSNs must never inherit SetDefaultDB.
	defaultDSN  bool
	tenant      string
	filePath    string
	autosave    bool
	maxReaders  int
	maxWriters  int
	busyTimeout time.Duration
	// mode selects a storage.StorageMode other than the driver's original
	// in-memory-plus-GOB-snapshot behavior (e.g. "disk", "json", "wal").
	// modeSet distinguishes "not specified" (keep the original LoadFromFile/
	// NewDB + autosave path, unchanged) from an explicit "memory" (which
	// behaves the same but goes through storage.OpenDB).
	mode    storage.StorageMode
	modeSet bool

	maxMemoryBytes     int64
	readOnly           bool
	syncOnMutate       bool
	compressFiles      bool
	checkpointEvery    uint64
	checkpointInterval time.Duration
	checkpointMaxBytes int64
}

// parseDSN parses a tinySQL DSN into a driver configuration.
func parseDSN(dsn string) (cfg, error) {
	var c cfg
	c.tenant = "default"
	c.maxWriters = 1
	if dsn == "" {
		c.defaultDSN = true
		return c, nil
	}
	switch {
	case strings.HasPrefix(dsn, "mem://"):
		if i := strings.Index(dsn, "?"); i >= 0 {
			if err := applyQueryOptions(dsn[i+1:], &c); err != nil {
				return c, err
			}
		}
		return c, nil
	case strings.HasPrefix(dsn, "file:"):
		path := strings.TrimPrefix(dsn, "file:")
		q := ""
		if i := strings.Index(path, "?"); i >= 0 {
			q = path[i+1:]
			path = path[:i]
		}
		if path == "" {
			return c, fmt.Errorf("file: path required")
		}
		c.filePath = filepath.Clean(path)
		if q != "" {
			if err := applyQueryOptions(q, &c); err != nil {
				return c, err
			}
		}
		return c, nil
	default:
		return c, fmt.Errorf("unsupported DSN")
	}
}

// applyQueryOptions parses a URL-style query string (k=v&k2=v2) and applies
// options to the provided cfg using applyDSNOption. This consolidates repeated
// logic used for different DSN prefixes (mem:// and file:).
func applyQueryOptions(q string, c *cfg) error {
	values, err := url.ParseQuery(q)
	if err != nil {
		return fmt.Errorf("tinysql: invalid DSN query: %w", err)
	}
	for key, values := range values {
		if len(values) != 1 {
			return fmt.Errorf("tinysql: DSN option %q must occur once", key)
		}
		if err := applyDSNOption(c, key, values[0]); err != nil {
			return err
		}
	}
	return nil
}

// server coordinates access to the shared storage.DB and manages
// concurrency primitives plus optional persistence hooks.
type server struct {
	mu          sync.RWMutex
	db          *storage.DB
	filePath    string
	autosave    bool
	readerPool  chan struct{}
	writerPool  chan struct{}
	busyTimeout time.Duration
	// usesStorageBackend is true when db was opened via storage.OpenDB with
	// an explicit mode= DSN option, rather than the driver's original
	// LoadFromFile/NewDB + SaveToFile-on-close scheme. Such backends persist
	// via DB.Sync() (which flushes dirty tables to whatever backend is
	// attached — GOB, JSON, ...), not via a whole-database GOB snapshot.
	usesStorageBackend bool
}

func newServer(db *storage.DB, c cfg) *server {
	s := &server{
		db:                 db,
		filePath:           c.filePath,
		autosave:           c.autosave,
		busyTimeout:        c.busyTimeout,
		usesStorageBackend: c.modeSet && c.mode != storage.ModeMemory,
	}
	if c.maxReaders > 0 {
		s.readerPool = make(chan struct{}, c.maxReaders)
	}
	if c.maxWriters > 0 {
		s.writerPool = make(chan struct{}, c.maxWriters)
	}
	return s
}

func (s *server) acquireReader(ctx context.Context) error {
	return s.acquire(ctx, s.readerPool)
}

func (s *server) releaseReader() {
	s.release(s.readerPool)
}

func (s *server) acquireWriter(ctx context.Context) error {
	return s.acquire(ctx, s.writerPool)
}

func (s *server) releaseWriter() {
	s.release(s.writerPool)
}

//nolint:gocyclo // Connection throttling must cover timeout, context, and immediate acquisition paths.
func (s *server) acquire(ctx context.Context, pool chan struct{}) error {
	if pool == nil {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.busyTimeout <= 0 {
		select {
		case pool <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timeout := s.busyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remain := time.Until(deadline)
		if remain <= 0 {
			return ctx.Err()
		}
		if remain < timeout {
			timeout = remain
		}
	}
	select {
	case pool <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case pool <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("tinysql: busy timeout after %s", timeout)
	}
}

func (s *server) release(pool chan struct{}) {
	// Non-blocking release: if the pool is empty or nil, simply return.
	if pool == nil {
		return
	}
	select {
	case <-pool:
	default:
	}
}

// saveIfNeeded persists the database to disk when autosave is enabled.
// saveIfNeeded performs a best-effort persistence of the in-memory DB to
// disk when autosave is enabled. Errors are logged but not returned; callers
// typically call this from cleanup paths where returning an error would be
// inconvenient.
func (s *server) saveIfNeeded() {
	// A read-only open must be observational: physical connection closes and
	// database/sql pool churn must never create manifests, checkpoints, WAL
	// files, or rewritten snapshots.
	if s.db == nil || s.db.IsReadOnly() {
		return
	}
	if s.usesStorageBackend {
		// Disk-backed modes (ModeDisk, ModeJSON, ModeHybrid, ModeIndex)
		// persist via their attached backend's Sync, not a whole-database
		// GOB snapshot; ModeWAL/ModeAdvancedWAL rely on their own
		// checkpoint machinery and treat Sync as a no-op (see DB.Sync's
		// doc comment). Always sync here regardless of the autosave flag —
		// choosing a durable mode is itself the opt-in.
		if err := s.db.Sync(); err != nil {
			log.Printf("sync failed: %v", err)
		}
		return
	}
	if s.autosave && s.filePath != "" {
		if err := storage.SaveToFile(s.db, s.filePath); err != nil {
			log.Printf("autosave failed: %v", err)
		}
	}
}

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
	return c.srv.db.Close()
}

// ------------------- connection / transactions -------------------

type conn struct {
	srv    *server
	tenant string

	inTx       bool
	txBase     *storage.DB // Snapshot base used for conflict detection
	shadow     *storage.DB // Snapshot copy (MVCC-light)
	txReadOnly bool        // Active tx requested as read-only
	txDirty    bool        // A successful write ran against shadow.
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	// database/sql may call Prepare for arbitrary SQL. Failing to build the
	// optional prepared-AST fast path must not change its historical behavior:
	// QueryContext will use the text-binding fallback below.
	prepared, _ := buildPreparedQuery(query)
	return &stmt{c: c, sql: query, prepared: prepared}, nil
}
func (c *conn) Close() error              { c.srv.saveIfNeeded(); return nil }
func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTx {
		return nil, fmt.Errorf("tinysql: transaction already active")
	}
	// Only the default isolation level is supported; other levels are rejected.
	switch opts.Isolation {
	case driver.IsolationLevel(0): // Default
		// Allow default isolation
	default:
		return nil, fmt.Errorf("unsupported isolation level: %v", opts.Isolation)
	}
	// An immutable/read-only database never has a writer to conflict with.
	// Avoid DeepClone here: cloning a disk-backed ModeIndex catalog would both
	// defeat its memory bound and lose its backend reference. The shared,
	// immutable DB itself is the transaction snapshot.
	if c.srv.db.IsReadOnly() {
		c.inTx = true
		c.txBase = nil
		c.shadow = nil
		c.txReadOnly = true
		c.txDirty = false
		return &tx{c: c}, nil
	}

	// Create snapshot copy under read lock; writer blocks commit briefly.
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	// A read-only transaction produces no changes to merge, so it needs only a
	// single stable read snapshot and no conflict-detection base. A read-write
	// transaction needs a mutable shadow plus a lightweight version-only base
	// (SnapshotForTx copies rows once, not twice).
	var base, shadow *storage.DB
	if opts.ReadOnly {
		shadow = c.srv.db.DeepClone()
	} else {
		base, shadow = c.srv.db.SnapshotForTx()
	}
	c.srv.mu.RUnlock()

	c.inTx = true
	c.txBase = base
	c.shadow = shadow
	c.txReadOnly = opts.ReadOnly
	c.txDirty = false
	return &tx{c: c}, nil
}

// Ping implements driver.Pinger so database/sql can health-check the connection.
func (c *conn) Ping(ctx context.Context) error {
	if c.srv == nil {
		return fmt.Errorf("tinysql: no server")
	}
	if err := c.srv.acquireReader(ctx); err != nil {
		return err
	}
	c.srv.releaseReader()
	return nil
}

type tx struct{ c *conn }

func (t *tx) Commit() error {
	return t.c.commitTx()
}
func (t *tx) Rollback() error {
	return t.c.rollbackTx()
}

func (c *conn) commitTx() error {
	if !c.inTx {
		return fmt.Errorf("tinysql: no active transaction")
	}
	// Read-only transactions produce no changes to merge: their snapshot is
	// either the immutable shared database (shadow == nil) or a private read
	// clone (shadow != nil, txBase == nil). Either way there is nothing to
	// commit, so skip the writer lock and change-collection entirely.
	if c.txReadOnly {
		c.clearTxState()
		return nil
	}
	// A BEGIN/COMMIT pair with no successful write cannot change the shared
	// database, so it needs neither the writer slot nor change collection.
	if !c.txDirty {
		c.clearTxState()
		return nil
	}
	if c.shadow == nil {
		return fmt.Errorf("tinysql: no active transaction snapshot")
	}
	if err := c.srv.acquireWriter(context.Background()); err != nil {
		return err
	}
	defer c.srv.releaseWriter()
	// Atomic swap: writer lock, replace data, save, unlock.
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	oldDB := c.srv.db
	newDB := c.shadow
	changes := storage.CollectWALChanges(c.txBase, newDB)
	if err := c.detectTxConflicts(oldDB, changes); err != nil {
		c.clearTxState()
		return err
	}
	wal := oldDB.WAL()
	needCheckpoint := false
	var err error
	if wal != nil && len(changes) > 0 {
		needCheckpoint, err = wal.LogTransaction(changes)
		if err != nil {
			return err
		}
	}
	if err := oldDB.ApplyWALChanges(changes); err != nil {
		return err
	}
	if wal != nil && needCheckpoint {
		if err := wal.Checkpoint(oldDB); err != nil {
			return err
		}
	}
	c.srv.saveIfNeeded()

	c.clearTxState()
	return nil
}

func (c *conn) detectTxConflicts(current *storage.DB, changes []storage.WALChange) error {
	if c.txBase == nil {
		return nil
	}
	for _, ch := range changes {
		baseTable, baseErr := c.txBase.Get(ch.Tenant, ch.Name)
		currentTable, currentErr := current.Get(ch.Tenant, ch.Name)
		baseExists := baseErr == nil
		currentExists := currentErr == nil
		switch {
		case !baseExists && currentExists:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		case baseExists && !currentExists:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		case baseExists && currentExists && baseTable.Version != currentTable.Version:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		}
	}
	return nil
}

func (c *conn) rollbackTx() error {
	if !c.inTx {
		return fmt.Errorf("tinysql: no active transaction")
	}
	c.clearTxState()
	return nil
}

func (c *conn) clearTxState() {
	c.inTx = false
	c.txBase = nil
	c.shadow = nil
	c.txReadOnly = false
	c.txDirty = false
}

// ------------------- exec / query -------------------

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.execSQL(ctx, sqlStr)
}
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.querySQL(ctx, sqlStr)
}

// Non-context fallbacks
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.ExecContext(context.Background(), query, n)
}
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.QueryContext(context.Background(), query, n)
}

func (c *conn) currentDB() *storage.DB {
	if c.inTx && c.shadow != nil {
		return c.shadow
	}
	return c.srv.db
}

// parsedStmtCache is a bounded, process-wide cache of parsed statements
// keyed by the final (post-placeholder-binding) SQL text. Applications going
// through database/sql re-issue the same statement text on every call —
// dashboards, health checks, catalog queries, RAG query templates — and
// previously paid a full lex+parse each time. Only SELECT/EXPLAIN results
// are cached: they are the shapes that repeat, and read statements are
// naturally re-executed from a shared AST elsewhere (the public
// ParseSQL-once/Execute-many pattern), so cross-connection reuse is safe.
// Oversized statements (bulk INSERTs, inlined vector literals — unique per
// call) are parsed directly and never stored, so they cannot churn the
// cache.
var (
	parsedStmtMu       sync.RWMutex
	parsedStmtCache    = make(map[string]engine.Statement)
	parsedStmtInFlight = make(map[string]*parsedStmtCall)
)

// parsedStmtCall is a small, channel-based singleflight for cold parse-cache
// misses. It deliberately lives beside the cache instead of adding another
// dependency: the leader parses once, concurrent readers wait without holding
// a mutex, and every waiter receives the same immutable SELECT/EXPLAIN AST.
// DML is never shared this way because it can carry connection-local state.
type parsedStmtCall struct {
	done   chan struct{}
	stmt   engine.Statement
	err    error
	shared bool
}

const (
	parsedStmtCacheMaxEntries = 256
	parsedStmtCacheMaxSQLLen  = 8 << 10
)

func parseSQLCached(sqlStr string) (engine.Statement, error) {
	cacheable := len(sqlStr) <= parsedStmtCacheMaxSQLLen && parseCacheCandidate(sqlStr)
	if !cacheable {
		return engine.NewParser(sqlStr).ParseStatement()
	}

	parsedStmtMu.RLock()
	st, ok := parsedStmtCache[sqlStr]
	parsedStmtMu.RUnlock()
	if ok {
		return st, nil
	}

	// Register a leader while holding the short cache mutex. Waiters release
	// it before blocking, so a cold burst neither serializes readers nor causes
	// N identical lex/parse passes.
	parsedStmtMu.Lock()
	if st, ok := parsedStmtCache[sqlStr]; ok {
		parsedStmtMu.Unlock()
		return st, nil
	}
	if call := parsedStmtInFlight[sqlStr]; call != nil {
		parsedStmtMu.Unlock()
		<-call.done
		if call.err != nil {
			return nil, call.err
		}
		if call.shared {
			return call.stmt, nil
		}
		// A malformed read-shaped statement is not shared. This branch is
		// defensive: cache candidates are SELECT/EXPLAIN only.
		return engine.NewParser(sqlStr).ParseStatement()
	}
	call := &parsedStmtCall{done: make(chan struct{})}
	parsedStmtInFlight[sqlStr] = call
	parsedStmtMu.Unlock()

	p := engine.NewParser(sqlStr)
	var err error
	st, err = p.ParseStatement()
	if err == nil {
		switch st.(type) {
		case *engine.Select, *engine.Explain:
			call.shared = true
		}
	}

	parsedStmtMu.Lock()
	if err == nil && call.shared {
		if len(parsedStmtCache) >= parsedStmtCacheMaxEntries {
			// Random eviction via map iteration order; a bad eviction just
			// costs one re-parse.
			for k := range parsedStmtCache {
				if len(parsedStmtCache) < parsedStmtCacheMaxEntries {
					break
				}
				delete(parsedStmtCache, k)
			}
		}
		parsedStmtCache[sqlStr] = st
	}
	call.stmt = st
	call.err = err
	delete(parsedStmtInFlight, sqlStr)
	close(call.done)
	parsedStmtMu.Unlock()
	return st, err
}

// parseCacheCandidate keeps DML out of the cold-miss coordinator. A write
// cannot be shared safely between connections, and coordinating it would only
// serialize a burst of independent mutations. The parser remains the final
// authority; this inexpensive check merely limits the immutable read cache.
func parseCacheCandidate(sqlStr string) bool {
	sqlStr = strings.TrimLeft(sqlStr, " \t\r\n")
	end := 0
	for end < len(sqlStr) {
		ch := sqlStr[end]
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') {
			break
		}
		end++
	}
	if end == 0 {
		return false
	}
	switch strings.ToUpper(sqlStr[:end]) {
	case "SELECT", "EXPLAIN":
		return true
	default:
		return false
	}
}

//nolint:gocyclo // execSQL coordinates parsing, locking, WAL, and transaction paths.
func (c *conn) execSQL(ctx context.Context, sqlStr string) (driver.Result, error) {
	if res, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
		return res, err
	}
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}
	return c.execStatement(ctx, st)
}

func (c *conn) execTransactionControl(ctx context.Context, sqlStr string) (driver.Result, bool, error) {
	switch normalizeTransactionSQL(sqlStr) {
	case "BEGIN", "BEGIN TRANSACTION", "START TRANSACTION":
		if c.inTx {
			return nil, true, fmt.Errorf("tinysql: transaction already active")
		}
		if _, err := c.BeginTx(ctx, driver.TxOptions{}); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "BEGIN READ ONLY", "BEGIN TRANSACTION READ ONLY", "START TRANSACTION READ ONLY":
		if c.inTx {
			return nil, true, fmt.Errorf("tinysql: transaction already active")
		}
		if _, err := c.BeginTx(ctx, driver.TxOptions{ReadOnly: true}); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "COMMIT", "COMMIT TRANSACTION":
		if err := c.commitTx(); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	case "ROLLBACK", "ROLLBACK TRANSACTION":
		if err := c.rollbackTx(); err != nil {
			return nil, true, err
		}
		return driver.RowsAffected(0), true, nil
	default:
		return nil, false, nil
	}
}

func normalizeTransactionSQL(sqlStr string) string {
	s := strings.TrimSpace(sqlStr)
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return strings.Join(strings.Fields(strings.ToUpper(s)), " ")
}

// writeTargetTable returns the single table name modified by a DML/DDL statement.
func writeTargetTable(st engine.Statement) string {
	switch s := st.(type) {
	case *engine.Insert:
		return s.Table
	case *engine.Update:
		return s.Table
	case *engine.Delete:
		return s.Table
	case *engine.CreateTable:
		return s.Name
	case *engine.DropTable:
		return s.Name
	default:
		return ""
	}
}

// affectedRows extracts the affected-row count from an UPDATE/DELETE result.
// The engine returns a single {countCell: n} row for the plain form; a
// RETURNING clause instead projects one row per affected row.
func affectedRows(rs *engine.ResultSet, countCell string) int64 {
	if rs == nil {
		return 0
	}
	if len(rs.Rows) == 1 && len(rs.Cols) == 1 && rs.Cols[0] == countCell {
		switch n := rs.Rows[0][countCell].(type) {
		case int:
			return int64(n)
		case int64:
			return n
		case float64:
			return int64(n)
		}
	}
	return int64(len(rs.Rows))
}

func (c *conn) execStatement(ctx context.Context, st engine.Statement) (driver.Result, error) {
	// Only SELECT/EXPLAIN/PRAGMA are guaranteed read-only. Treat every other
	// parsed statement as a write for connection scheduling so DDL, indexes,
	// views, jobs and RBAC cannot bypass the writer gate.
	isWrite := true
	switch st.(type) {
	case *engine.Select, *engine.Explain, *engine.Pragma:
		isWrite = false
	}

	if isWrite {
		if c.srv.db.IsReadOnly() || (c.inTx && c.txReadOnly) {
			return nil, fmt.Errorf("tinysql: write attempted in read-only transaction")
		}
		var rs *engine.ResultSet
		if c.inTx {
			r, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
			if err != nil {
				return nil, err
			}
			rs = r
			c.txDirty = true
		} else {
			if err := c.srv.acquireWriter(ctx); err != nil {
				return nil, err
			}
			defer c.srv.releaseWriter()
			c.srv.mu.Lock()
			defer c.srv.mu.Unlock()
			base := c.srv.db
			wal := base.WAL()
			var needCheckpoint bool
			var err error
			if wal != nil {
				// Clone only the single table being modified instead of the
				// entire database. All other tables are shared by reference.
				target := writeTargetTable(st)
				shadow := base.ShallowCloneForTable(c.tenant, target)
				if rs, err = engine.Execute(ctx, shadow, c.tenant, st); err != nil {
					return nil, err
				}
				changes := storage.CollectWALChanges(base, shadow)
				if len(changes) > 0 {
					needCheckpoint, err = wal.LogTransaction(changes)
					if err != nil {
						return nil, err
					}
				}
				c.srv.db = shadow
				if needCheckpoint {
					if err := wal.Checkpoint(shadow); err != nil {
						return nil, err
					}
				}
			} else {
				if rs, err = engine.Execute(ctx, base, c.tenant, st); err != nil {
					return nil, err
				}
			}
			c.srv.saveIfNeeded()
		}
		// Report affected rows for UPDATE/DELETE. The engine returns a single
		// {updated|deleted: n} cell for the plain form; a RETURNING clause
		// projects one row per affected row. INSERT has no engine-side count.
		switch st.(type) {
		case *engine.Update:
			return driver.RowsAffected(affectedRows(rs, "updated")), nil
		case *engine.Delete:
			return driver.RowsAffected(affectedRows(rs, "deleted")), nil
		}
		return driver.RowsAffected(0), nil
	}

	// READS: unter RLock auf aktueller DB
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	defer c.srv.mu.RUnlock()
	_, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
	if err != nil {
		return nil, err
	}
	// no rows affected for pure reads
	return driver.RowsAffected(0), nil
}

func (c *conn) querySQL(ctx context.Context, sqlStr string) (driver.Rows, error) {
	if _, handled, err := c.execTransactionControl(ctx, sqlStr); handled {
		if err != nil {
			return nil, err
		}
		return emptyRows{}, nil
	}
	// Queries return a driver.Rows. For non-SELECT statements, execute them
	// and return an empty result set to satisfy the interface.
	st, err := parseSQLCached(sqlStr)
	if err != nil {
		return nil, err
	}

	// For non-result statements, execute via pre-parsed statement (no re-parse).
	_, isSelect := st.(*engine.Select)
	_, isExplain := st.(*engine.Explain)
	if !isSelect && !isExplain {
		if _, err = c.execStatement(ctx, st); err != nil {
			return nil, err
		}
		return emptyRows{}, nil
	}

	return c.queryStatement(ctx, st)
}

// queryStatement executes an already parsed SELECT/EXPLAIN statement. It is
// deliberately shared by normal and prepared queries so locking, snapshots,
// and database/sql row ownership remain identical.
func (c *conn) queryStatement(ctx context.Context, st engine.Statement) (driver.Rows, error) {
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	defer c.srv.mu.RUnlock()
	rs, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
	if err != nil {
		return nil, err
	}
	return &rows{rs: rs}, nil
}

// NamedValueChecker
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Normalize common Go types into database/sql primitive types.
	switch v := nv.Value.(type) {
	case time.Time:
		nv.Value = v.UTC().Format(time.RFC3339Nano)
	case []byte:
		// Keep BLOB parameters as bytes. bindPlaceholders emits a SQL X'...'
		// literal and the parser recreates []byte without a text/base64 round
		// trip.
		nv.Value = append([]byte(nil), v...)
	case int:
		nv.Value = int64(v)
	}
	return nil
}

// ------------------- stmt / rows -------------------

type stmt struct {
	c        *conn
	sql      string
	prepared *preparedQuery
}

// preparedQuery is an immutable prepared-statement template. Each execution
// borrows an exclusive AST plus literal slots from pool, which avoids both
// reparsing on warm workers and the former statement-wide mutex. A pooled AST
// never escapes queryStatement: engine.Execute has materialized its ResultSet
// before the execution state is returned to the pool.
type preparedQuery struct {
	markerSQL string
	markers   []string
	pool      sync.Pool
}

// preparedExecution is intentionally owned by exactly one goroutine between
// acquire and release. Keeping the bindable literals together with their AST
// makes concurrent prepared statements race-free without cloning the AST for
// every query. release restores markers so a pooled execution cannot retain
// caller BLOBs or other large parameter values.
type preparedExecution struct {
	statement engine.Statement
	params    []*engine.Literal
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), n)
}
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), n)
}
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.execSQL(ctx, sqlStr)
}
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.prepared != nil {
		return s.queryPrepared(ctx, args)
	}
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.querySQL(ctx, sqlStr)
}

func (s *stmt) queryPrepared(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != len(s.prepared.markers) {
		return nil, fmt.Errorf("tinysql: expected %d placeholder arguments, got %d", len(s.prepared.markers), len(args))
	}
	exec, err := s.prepared.acquire()
	if err != nil {
		return nil, err
	}
	defer s.prepared.release(exec)
	for i, arg := range args {
		exec.params[i].Val = driverValueLiteral(arg.Value)
	}
	return s.c.queryStatement(ctx, exec.statement)
}

func driverValueLiteral(v any) any {
	// bindPlaceholders previously converted integer arguments into a decimal
	// SQL literal, which the parser represented as int where possible. Keep
	// that type behaviour so prepared index seeks use the same canonical keys.
	switch value := v.(type) {
	case int64:
		if int64(int(value)) == value {
			return int(value)
		}
		return value
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}

const preparedMarkerPrefix = "__tinysql_prepared_param_"

// buildPreparedQuery recognizes positional placeholders outside SQL strings
// and validates the marker form once. Numbered placeholders deliberately keep
// the text fallback: their repeated/reordered binding needs a separate ordinal
// map.
func buildPreparedQuery(sqlText string) (*preparedQuery, error) {
	markerSQL, count, ok := markerSQLForPositionalParams(sqlText)
	if !ok || count == 0 {
		return nil, nil
	}
	markers := make([]string, count)
	for i := range markers {
		markers[i] = preparedMarkerPrefix + strconv.Itoa(i) + "__"
	}
	prepared := &preparedQuery{markerSQL: markerSQL, markers: markers}
	// Parse and validate once at Prepare time. Seeding the pool gives the
	// common single-goroutine path the original no-reparse behavior; additional
	// workers create isolated executions only when needed.
	exec, err := prepared.newExecution()
	if err != nil {
		return nil, err
	}
	prepared.pool.Put(exec)
	return prepared, nil
}

func (p *preparedQuery) acquire() (*preparedExecution, error) {
	if value := p.pool.Get(); value != nil {
		return value.(*preparedExecution), nil
	}
	return p.newExecution()
}

func (p *preparedQuery) release(exec *preparedExecution) {
	for i, literal := range exec.params {
		literal.Val = p.markers[i]
	}
	p.pool.Put(exec)
}

func (p *preparedQuery) newExecution() (*preparedExecution, error) {
	statement, err := engine.NewParser(p.markerSQL).ParseStatement()
	if err != nil {
		return nil, err
	}
	if _, ok := statement.(*engine.Select); !ok {
		return nil, fmt.Errorf("tinysql: prepared fast path supports SELECT only")
	}
	markerPositions := make(map[string]int, len(p.markers))
	for i, marker := range p.markers {
		markerPositions[marker] = i
	}
	params := make([]*engine.Literal, len(p.markers))
	collectPreparedLiterals(reflect.ValueOf(statement), markerPositions, params)
	for i, literal := range params {
		if literal == nil {
			return nil, fmt.Errorf("tinysql: positional parameter %d was not parsed as a literal", i+1)
		}
		literal.Parameter = true
	}
	return &preparedExecution{statement: statement, params: params}, nil
}

func markerSQLForPositionalParams(sqlText string) (string, int, bool) {
	var out strings.Builder
	out.Grow(len(sqlText) + 32)
	count := 0
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if ch == '\'' {
			out.WriteByte(ch)
			i++
			for i < len(sqlText) {
				b := sqlText[i]
				out.WriteByte(b)
				if b == '\'' {
					if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
						i++
						out.WriteByte(sqlText[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if ch == '?' {
			out.WriteByte('\'')
			out.WriteString(preparedMarkerPrefix)
			out.WriteString(strconv.Itoa(count))
			out.WriteString("__'")
			count++
			continue
		}
		// Keep $1/:1 on the established text-binding path. This also avoids
		// interpreting PostgreSQL casts or identifiers containing a colon.
		if ch == '$' || ch == ':' {
			if i+1 < len(sqlText) && sqlText[i+1] >= '0' && sqlText[i+1] <= '9' {
				return "", 0, false
			}
		}
		out.WriteByte(ch)
	}
	return out.String(), count, true
}

var literalPtrType = reflect.TypeOf((*engine.Literal)(nil))

func collectPreparedLiterals(v reflect.Value, markers map[string]int, params []*engine.Literal) {
	if !v.IsValid() {
		return
	}
	if v.Type() == literalPtrType {
		if v.IsNil() {
			return
		}
		literal := v.Interface().(*engine.Literal)
		if marker, ok := literal.Val.(string); ok {
			if i, ok := markers[marker]; ok {
				params[i] = literal
			}
		}
		return
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			collectPreparedLiterals(v.Elem(), markers, params)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanInterface() {
				collectPreparedLiterals(v.Field(i), markers, params)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectPreparedLiterals(v.Index(i), markers, params)
		}
	}
}

type rows struct {
	rs        *engine.ResultSet
	cachedRS  *engine.ResultSet
	lowerCols []string
	i         int
}

func (r *rows) Columns() []string { return r.rs.Cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.rs.Rows) {
		return io.EOF
	}
	if r.cachedRS != r.rs || len(r.lowerCols) != len(r.rs.Cols) {
		r.lowerCols = make([]string, len(r.rs.Cols))
		for i, c := range r.rs.Cols {
			r.lowerCols[i] = strings.ToLower(c)
		}
		r.cachedRS = r.rs
	}
	row := r.rs.Rows[r.i]
	for i := range r.rs.Cols {
		v := row[r.lowerCols[i]]
		switch vv := v.(type) {
		case nil:
			dest[i] = nil
		case int:
			dest[i] = int64(vv)
		case int64:
			dest[i] = vv
		case float64:
			dest[i] = vv
		case bool:
			dest[i] = vv
		case string:
			dest[i] = vv
		case time.Time:
			// RFC3339Nano to match the bind path (CheckNamedValue), so sub-second
			// precision survives a bind -> store -> scan round trip.
			dest[i] = vv.Format(time.RFC3339Nano)
		case []byte:
			// database/sql callers may retain Scan destinations; return an owned
			// slice just as the standard drivers do for binary columns.
			dest[i] = append([]byte(nil), vv...)
		default:
			b, _ := storage.JSONMarshal(vv)
			dest[i] = string(b)
		}
	}
	r.i++
	return nil
}

// Optional ColumnType* (informativ)
func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return "TEXT" }
func (r *rows) ColumnTypeNullable(i int) (bool, bool)   { return true, true }
func (r *rows) ColumnTypeScanType(i int) any            { return "interface{}" }

type emptyRows struct{}

func (emptyRows) Columns() []string                     { return []string{} }
func (emptyRows) Close() error                          { return nil }
func (emptyRows) Next([]driver.Value) error             { return io.EOF }
func (emptyRows) ColumnTypeDatabaseTypeName(int) string { return "TEXT" }
func (emptyRows) ColumnTypeNullable(int) (bool, bool)   { return true, true }
func (emptyRows) ColumnTypeScanType(int) any            { return "interface{}" }

// Placeholder Binding (einfach/sicher)
func bindPlaceholders(sqlStr string, args []driver.NamedValue) (string, error) {
	// Precompute literal strings for all args to avoid repeated formatting.
	lits := make([]string, len(args))
	for i := range args {
		lits[i] = sqlLiteral(args[i].Value)
	}
	used := make([]bool, len(lits))

	var sb strings.Builder
	sb.Grow(len(sqlStr) + len(lits)*8)
	argi := 0
	n := len(sqlStr)
	for i := 0; i < n; i++ {
		ch := sqlStr[i]
		// Copy quoted strings verbatim (single-quoted SQL literals)
		if ch == '\'' {
			sb.WriteByte(ch)
			i++
			for i < n {
				b := sqlStr[i]
				sb.WriteByte(b)
				if b == '\'' {
					// handle doubled single-quote escape inside SQL literal
					if i+1 < n && sqlStr[i+1] == '\'' {
						i++
						sb.WriteByte(sqlStr[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}

		// Sequential placeholder '?'
		if ch == '?' {
			if argi >= len(lits) {
				return "", fmt.Errorf("not enough args for placeholders")
			}
			sb.WriteString(lits[argi])
			used[argi] = true
			argi++
			continue
		}

		// Numbered placeholders: $1, $2 or :1, :2 (1-based)
		if (ch == '$' || ch == ':') && i+1 < n {
			j := i + 1
			num := 0
			const maxInt = int(^uint(0) >> 1)
			for j < n {
				c := sqlStr[j]
				if c < '0' || c > '9' {
					break
				}
				d := int(c - '0')
				if num > (maxInt-d)/10 {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sqlStr[i+1:j+1])
				}
				num = num*10 + d
				j++
			}
			if j > i+1 {
				if num <= 0 || num > len(lits) {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sqlStr[i+1:j])
				}
				sb.WriteString(lits[num-1])
				used[num-1] = true
				i = j - 1
				continue
			}
		}

		sb.WriteByte(ch)
	}

	// Ensure every provided arg was used by at least one placeholder.
	for i := range used {
		if !used[i] {
			return "", fmt.Errorf("too many args for placeholders: arg %d unused", i+1)
		}
	}
	return sb.String(), nil
}

// sqlLiteral converts a Go value into a SQL literal string suitable for
// substitution in a query.
func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		// 'f' (not 'g') so small/large magnitudes never render in scientific
		// notation (e.g. 1e-05), which the SQL lexer cannot tokenize.
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		// escape single quotes by doubling them
		s := strings.ReplaceAll(x, "'", "''")
		return "'" + s + "'"
	case []byte:
		return "X'" + hex.EncodeToString(x) + "'"
	default:
		// Fallback: attempt JSON marshal (handles slices/maps)
		b, err := json.Marshal(x)
		if err != nil {
			// On marshal error, fall back to fmt.Sprintf representation
			s := strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''")
			return "'" + s + "'"
		}
		s := strings.ReplaceAll(string(b), "'", "''")
		return "'" + s + "'"
	}
}

// applyDSNOption mutates the configuration in place for one URL-query option.
// Unknown or malformed options are errors: silently accepting a memory or
// durability setting is dangerous because callers believe a bound exists when
// it does not.
func applyDSNOption(c *cfg, key, value string) error {
	key = strings.ToLower(strings.TrimSpace(key))
	switch key {
	case "tenant":
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("tinysql: tenant must not be empty")
		}
		c.tenant = value
	case "autosave":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.autosave = v
	case "pool_readers", "read_pool", "reader_pool":
		n, err := parsePoolSize(value, "pool_readers")
		if err != nil {
			return err
		}
		c.maxReaders = n
	case "pool_writers", "write_pool", "writer_pool":
		n, err := parsePoolSize(value, "pool_writers")
		if err != nil {
			return err
		}
		c.maxWriters = n
	case "busy_timeout", "busytimeout":
		dur, err := parseBusyTimeout(value)
		if err != nil {
			return err
		}
		c.busyTimeout = dur
	case "mode":
		m, err := storage.ParseStorageMode(value)
		if err != nil {
			return err
		}
		c.mode = m
		c.modeSet = true
	case "max_memory_bytes":
		sz, err := parseByteSize(value, key, false)
		if err != nil {
			return err
		}
		c.maxMemoryBytes = sz
	case "read_only":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.readOnly = v
	case "sync_on_mutate":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.syncOnMutate = v
	case "compress_files":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.compressFiles = v
	case "checkpoint_every":
		v, err := parseNonNegativeUint(value, key)
		if err != nil {
			return err
		}
		c.checkpointEvery = v
	case "checkpoint_interval":
		d, err := parseNonNegativeDuration(value, key)
		if err != nil {
			return err
		}
		c.checkpointInterval = d
	case "checkpoint_max_bytes":
		sz, err := parseByteSize(value, key, true)
		if err != nil {
			return err
		}
		c.checkpointMaxBytes = sz
	default:
		return fmt.Errorf("tinysql: unsupported DSN option %q", key)
	}
	return nil
}

func parseDSNBool(value, key string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("tinysql: invalid %s boolean %q (use 0/1 or true/false)", key, value)
	}
}

func parseNonNegativeUint(value, key string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid %s value %q", key, value)
	}
	return v, nil
}

func parseNonNegativeDuration(value, key string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("tinysql: invalid %s duration %q", key, value)
	}
	return d, nil
}

// parseByteSize accepts a non-negative integer byte count or a binary/decimal
// suffix (KiB/MiB/GiB and KB/MB/GB). -1 is accepted only for options where it
// has an explicit documented meaning (checkpoint_max_bytes disables its size
// trigger). Values must fit an int64 so they can be handed directly to the
// storage layer without silent overflow.
func parseByteSize(value, key string, allowNegativeOne bool) (int64, error) {
	v := strings.TrimSpace(value)
	if allowNegativeOne && v == "-1" {
		return -1, nil
	}
	if v == "" || strings.HasPrefix(v, "-") {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	lower := strings.ToLower(v)
	multipliers := []struct {
		suffix string
		factor uint64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
		{"kb", 1000}, {"mb", 1000 * 1000}, {"gb", 1000 * 1000 * 1000}, {"tb", 1000 * 1000 * 1000 * 1000},
		{"b", 1},
	}
	factor := uint64(1)
	for _, unit := range multipliers {
		if strings.HasSuffix(lower, unit.suffix) {
			lower = strings.TrimSpace(strings.TrimSuffix(lower, unit.suffix))
			factor = unit.factor
			break
		}
	}
	if lower == "" {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	n, err := strconv.ParseUint(lower, 10, 64)
	if err != nil || n > uint64(^uint64(0))/factor || n*factor > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	return int64(n * factor), nil
}

func parsePoolSize(value, key string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid %s value %q", key, value)
	}
	if n < 0 {
		return 0, fmt.Errorf("tinysql: %s must be >= 0", key)
	}
	return n, nil
}

func parseBusyTimeout(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: busy_timeout must not be empty")
	}
	isNumeric := true
	for _, r := range value {
		if r < '0' || r > '9' {
			isNumeric = false
			break
		}
	}
	if isNumeric {
		switch value {
		case "":
			return 0, nil
		default:
			sz, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("tinysql: invalid busy_timeout value %q", value)
			}
			if sz < 0 {
				return 0, fmt.Errorf("tinysql: busy_timeout must be >= 0")
			}
			return time.Duration(sz) * time.Millisecond, nil
		}
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid busy_timeout value %q", value)
	}
	if dur < 0 {
		return 0, fmt.Errorf("tinysql: busy_timeout must be >= 0")
	}
	return dur, nil
}
