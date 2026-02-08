// Package driver implements a database/sql driver for tinySQL.
//
// What: A minimal driver that exposes tinySQL via the standard database/sql
// interfaces. It supports in-memory databases (mem://) and file-backed
// persistence (file:path?options) with optional WAL and autosave.
// How: A small server wrapper manages a storage.DB and concurrency via reader
// and writer pools. Connections create snapshots for transactions (MVCC-light)
// and serialize writes through a WAL when configured. Placeholders (?) are
// bound by simple string substitution with proper literal escaping.
// Why: Integrating with database/sql enables familiar APIs, tooling, and
// portability while keeping the implementation small and self-contained.
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
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
//
// See parseDSN for all available options.
var defaultDrv = &drv{}

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
	defaultDrv.srv = newServer(db, c)
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

// cfg stores the connection parameters derived from a parsed DSN.
type cfg struct {
	tenant      string
	filePath    string
	autosave    bool
	maxReaders  int
	maxWriters  int
	busyTimeout time.Duration
}

// parseDSN parses a tinySQL DSN into a driver configuration.
func parseDSN(dsn string) (cfg, error) {
	var c cfg
	c.tenant = "default"
	c.maxWriters = 1
	switch {
	case strings.HasPrefix(dsn, "mem://"):
		if i := strings.Index(dsn, "?"); i >= 0 {
			q := dsn[i+1:]
			for _, kv := range strings.Split(q, "&") {
				if kv == "" {
					continue
				}
				parts := strings.SplitN(kv, "=", 2)
				k := parts[0]
				v := ""
				if len(parts) == 2 {
					v = parts[1]
				}
				if err := applyDSNOption(&c, k, v); err != nil {
					return c, err
				}
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
		for _, kv := range strings.Split(q, "&") {
			if kv == "" {
				continue
			}
			parts := strings.SplitN(kv, "=", 2)
			k := parts[0]
			v := ""
			if len(parts) == 2 {
				v = parts[1]
			}
			if err := applyDSNOption(&c, k, v); err != nil {
				return c, err
			}
		}
		return c, nil
	default:
		if dsn == "" {
			return c, nil
		}
		return c, fmt.Errorf("unsupported DSN")
	}
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
}

func newServer(db *storage.DB, c cfg) *server {
	s := &server{
		db:          db,
		filePath:    c.filePath,
		autosave:    c.autosave,
		busyTimeout: c.busyTimeout,
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
	if pool == nil {
		return
	}
	select {
	case <-pool:
	default:
	}
}

// saveIfNeeded persists the database to disk when autosave is enabled.
func (s *server) saveIfNeeded() {
	if s.autosave && s.filePath != "" {
		_ = storage.SaveToFile(s.db, s.filePath)
	}
}

type drv struct{ srv *server }

func (d *drv) Open(name string) (driver.Conn, error) {
	c, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	var s *server
	if d.srv != nil {
		s = d.srv
	} else {
		var db *storage.DB
		if c.filePath != "" {
			db, err = storage.LoadFromFile(c.filePath)
			if err != nil {
				return nil, err
			}
		} else {
			db = storage.NewDB()
		}
		s = newServer(db, c)
	}
	return &conn{srv: s, tenant: c.tenant}, nil
}

// ------------------- connection / transactions -------------------

type conn struct {
	srv    *server
	tenant string

	inTx       bool
	shadow     *storage.DB // Snapshot copy (MVCC-light)
	txReadOnly bool        // Active tx requested as read-only
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { return &stmt{c: c, sql: query}, nil }
func (c *conn) Close() error                              { c.srv.saveIfNeeded(); return nil }
func (c *conn) Begin() (driver.Tx, error)                 { return c.BeginTx(context.Background(), driver.TxOptions{}) }

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Only the default isolation level is supported; other levels are rejected.
	switch opts.Isolation {
	case driver.IsolationLevel(0): // Default
		// Allow default isolation
	default:
		return nil, fmt.Errorf("unsupported isolation level: %v", opts.Isolation)
	}
	// Create snapshot copy under read lock; writer blocks commit briefly.
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	shadow := c.srv.db.DeepClone()
	c.srv.mu.RUnlock()

	c.inTx = true
	c.shadow = shadow
	c.txReadOnly = opts.ReadOnly
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
	if err := t.c.srv.acquireWriter(context.Background()); err != nil {
		return err
	}
	defer t.c.srv.releaseWriter()
	// Atomic swap: writer lock, replace data, save, unlock.
	t.c.srv.mu.Lock()
	defer t.c.srv.mu.Unlock()
	oldDB := t.c.srv.db
	newDB := t.c.shadow
	changes := storage.CollectWALChanges(oldDB, newDB)
	wal := oldDB.WAL()
	needCheckpoint := false
	var err error
	if wal != nil && len(changes) > 0 {
		needCheckpoint, err = wal.LogTransaction(changes)
		if err != nil {
			return err
		}
	}
	t.c.srv.db = newDB
	if wal != nil && needCheckpoint {
		if err := wal.Checkpoint(newDB); err != nil {
			return err
		}
	}
	t.c.srv.saveIfNeeded()

	t.c.inTx = false
	t.c.shadow = nil
	t.c.txReadOnly = false
	return nil
}
func (t *tx) Rollback() error {
	t.c.inTx = false
	t.c.shadow = nil
	t.c.txReadOnly = false
	return nil
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

//nolint:gocyclo // execSQL coordinates parsing, locking, WAL, and transaction paths.
func (c *conn) execSQL(ctx context.Context, sqlStr string) (driver.Result, error) {
	p := engine.NewParser(sqlStr)
	st, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}
	return c.execStatement(ctx, st)
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

func (c *conn) execStatement(ctx context.Context, st engine.Statement) (driver.Result, error) {

	// DDL/DML writes must run in tx snapshot or under lock
	isWrite := func(s engine.Statement) bool {
		switch s.(type) {
		case *engine.CreateTable, *engine.DropTable, *engine.Insert, *engine.Update, *engine.Delete:
			return true
		default:
			return false
		}
	}

	if isWrite(st) {
		if c.inTx && c.txReadOnly {
			return nil, fmt.Errorf("tinysql: write attempted in read-only transaction")
		}
		if c.inTx {
			_, err := engine.Execute(ctx, c.currentDB(), c.tenant, st)
			if err != nil {
				return nil, err
			}
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
				if _, err := engine.Execute(ctx, shadow, c.tenant, st); err != nil {
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
				if _, err := engine.Execute(ctx, base, c.tenant, st); err != nil {
					return nil, err
				}
			}
			c.srv.saveIfNeeded()
		}
		// Result-Affected Rows: nur für UPDATE/DELETE (Engine liefert es)
		if ud, ok := st.(*engine.Update); ok && ud != nil {
			return driver.RowsAffected(0), nil
		}
		if dl, ok := st.(*engine.Delete); ok && dl != nil {
			return driver.RowsAffected(0), nil
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
	// Queries return a driver.Rows. For non-SELECT statements, execute them
	// and return an empty result set to satisfy the interface.
	p := engine.NewParser(sqlStr)
	st, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	// For non-SELECT statements, execute via pre-parsed statement (no re-parse).
	if _, ok := st.(*engine.Select); !ok {
		if _, err := c.execStatement(ctx, st); err != nil {
			return nil, err
		}
		return emptyRows{}, nil
	}

	// SELECT
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
		nv.Value = base64.StdEncoding.EncodeToString(v)
	case int:
		nv.Value = int64(v)
	}
	return nil
}

// ------------------- stmt / rows -------------------

type stmt struct {
	c   *conn
	sql string
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
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.querySQL(ctx, sqlStr)
}

type rows struct {
	rs *engine.ResultSet
	i  int
}

func (r *rows) Columns() []string { return r.rs.Cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.rs.Rows) {
		return io.EOF
	}
	row := r.rs.Rows[r.i]
	for i, c := range r.rs.Cols {
		v := row[strings.ToLower(c)]
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
			dest[i] = vv.Format(time.RFC3339)
		default:
			b, _ := json.Marshal(vv)
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
	var sb strings.Builder
	sb.Grow(len(sqlStr) + len(args)*10)
	argi := 0
	for i := 0; i < len(sqlStr); i++ {
		ch := sqlStr[i]
		if ch == '\'' {
			sb.WriteByte(ch)
			i++
			for i < len(sqlStr) {
				sb.WriteByte(sqlStr[i])
				if sqlStr[i] == '\'' {
					if i+1 < len(sqlStr) && sqlStr[i+1] == '\'' {
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
		// Support traditional ? placeholders (sequential)
		if ch == '?' {
			if argi >= len(args) {
				return "", fmt.Errorf("not enough args for placeholders")
			}
			sb.WriteString(sqlLiteral(args[argi].Value))
			argi++
			continue
		}
		// Support numbered placeholders: $1, $2 or :1, :2 (1-based)
		if (ch == '$' || ch == ':') && i+1 < len(sqlStr) && sqlStr[i+1] >= '0' && sqlStr[i+1] <= '9' {
			j := i + 2
			for j < len(sqlStr) && sqlStr[j] >= '0' && sqlStr[j] <= '9' {
				j++
			}
			idxStr := sqlStr[i+1 : j]
			n, err := strconv.Atoi(idxStr)
			if err != nil || n <= 0 || n > len(args) {
				return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, idxStr)
			}
			sb.WriteString(sqlLiteral(args[n-1].Value))
			i = j - 1
			continue
		}
		sb.WriteByte(ch)
	}
	if argi != len(args) {
		return "", fmt.Errorf("too many args for placeholders")
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
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		s := strings.ReplaceAll(x, "'", "''")
		return "'" + s + "'"
	default:
		b, _ := json.Marshal(x)
		s := strings.ReplaceAll(string(b), "'", "''")
		return "'" + s + "'"
	}
}

// applyDSNOption mutates the configuration in place for a single DSN option.
func applyDSNOption(c *cfg, key, value string) error {
	key = strings.ToLower(key)
	switch key {
	case "tenant":
		if value != "" {
			c.tenant = value
		}
	case "autosave":
		if value == "" {
			c.autosave = false
			return nil
		}
		v := strings.ToLower(value)
		c.autosave = v == "1" || v == "true" || v == "yes" || v == "on"
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
		if value == "" {
			c.busyTimeout = 0
			return nil
		}
		dur, err := parseBusyTimeout(value)
		if err != nil {
			return err
		}
		c.busyTimeout = dur
	default:
		return nil
	}
	return nil
}

func parsePoolSize(value, key string) (int, error) {
	if value == "" {
		return 0, nil
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
	isNumeric := true
	for _, r := range value {
		if r < '0' || r > '9' {
			isNumeric = false
			break
		}
	}
	if isNumeric {
		switch {
		case value == "":
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
