package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ───────────────────────────────────────────────────────────────────────────
// DiskBackend – every table is a separate GOB file on disk
// ───────────────────────────────────────────────────────────────────────────

// diskFileFormat selects the per-table file encoding DiskBackend uses.
type diskFileFormat int

const (
	// diskFormatGob encodes each table with encoding/gob — compact, exact
	// round-trip of every Go type tinySQL stores (big.Rat, uuid.UUID, ...).
	diskFormatGob diskFileFormat = iota
	// diskFormatJSON encodes each table with encoding/json — larger and
	// slightly lossy for exotic types (big.Rat/uuid.UUID round-trip as
	// plain strings, see normalizeForJSON), but human-readable and easy to
	// inspect, diff, or edit with any text tool.
	diskFormatJSON
)

// DiskBackend stores each table as an individual file under a directory tree:
//
//	<dir>/<tenant>/<tablename>.tbl        (GOB-encoded diskTable, default)
//	<dir>/<tenant>/<tablename>.json       (JSON-encoded diskTable, ModeJSON)
//	<dir>/manifest.json                   (lightweight metadata index)
//
// Tables are loaded into memory on demand (via LoadTable) and written back
// on SaveTable / Sync. This minimises RAM usage while keeping the file
// format compatible with the existing GOB serialisation (or, for ModeJSON,
// a JSON encoding of the same diskTable structure).
type DiskBackend struct {
	mu     sync.RWMutex
	dir    string
	gzip   bool
	format diskFileFormat
	mode   StorageMode

	// encryptor, if non-nil, wraps every table file's encoded (and
	// optionally gzipped) bytes in AES-256-GCM before it's written to
	// disk, and unwraps on read. See SetEncryptor.
	encryptor *Encryptor

	// meta tracks every known table (on disk or in memory).
	meta map[string]map[string]*TableMeta // tenant → lower(name) → meta

	// versions tracks the version at last save so we can detect dirty tables.
	versions map[string]map[string]int // tenant → lower(name) → version

	// gen counts, per table (tenant NUL lower(name)), the installs and deletes
	// that replaced or removed its file. A snapshot writer compares it before
	// installing: see saveTableSnapshot.
	gen map[string]uint64

	// delGen counts, per table (same key as gen), the DeleteTable and
	// forgetTable calls: how often the table was dropped. HybridBackend uses it
	// to notice a DROP that ran after a snapshot of the table was installed.
	delGen map[string]uint64

	// tableLocks serializes the writers of one table's file (lockTableFile).
	// Always acquired before b.mu, never while holding it.
	tableLocks sync.Map

	// Manifest timestamp bookkeeping.
	manifestCreatedAt time.Time
	manifestDirty     bool

	// readOnly prevents every persistence operation, including Close/Sync.
	// DB.SetReadOnly protects SQL execution; this backend flag additionally
	// protects direct embedding callers and background cleanup paths.
	readOnly atomic.Bool

	// skipSearchIndexes keeps the FTS/ANN index images out of table files and
	// drops the ones found in files that carry them. See
	// StorageConfig.SkipSearchIndexPersistence.
	skipSearchIndexes atomic.Bool

	// rowBlocks writes table files in the row-block layout. See
	// StorageConfig.RowBlockFiles.
	rowBlocks atomic.Bool

	// stats
	syncCount     atomic.Int64
	tableWrites   atomic.Int64
	loadCount     atomic.Int64
	evictionCount atomic.Int64
}

// NewDiskBackend opens (or creates) a disk-backed storage directory using
// GOB-encoded per-table files (ModeDisk).
func NewDiskBackend(dir string, compress bool) (*DiskBackend, error) {
	return newDiskBackend(dir, compress, diskFormatGob, ModeDisk)
}

// NewJSONBackend opens (or creates) a disk-backed storage directory using
// JSON-encoded per-table files (ModeJSON). Functionally identical to
// NewDiskBackend — same manifest, same lazy loading, same dirty tracking —
// only the per-table file encoding differs. JSON files are larger than GOB
// and big.Rat/uuid.UUID values round-trip as plain strings (see
// normalizeForJSON), but the files can be read, diffed, or hand-edited with
// any text tool, which GOB files cannot.
func NewJSONBackend(dir string, compress bool) (*DiskBackend, error) {
	return newDiskBackend(dir, compress, diskFormatJSON, ModeJSON)
}

func newDiskBackend(dir string, compress bool, format diskFileFormat, mode StorageMode) (*DiskBackend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("disk backend: create dir: %w", err)
	}
	b := &DiskBackend{
		dir:      dir,
		gzip:     compress,
		format:   format,
		mode:     mode,
		meta:     make(map[string]map[string]*TableMeta),
		versions: make(map[string]map[string]int),
		gen:      make(map[string]uint64),
		delGen:   make(map[string]uint64),
	}
	if err := b.loadManifest(); err != nil {
		// If manifest doesn't exist, that's fine – new database.
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return b, nil
}

// SetEncryptor enables (or, passing nil, disables) AES-256-GCM encryption
// at rest for every table file this backend writes from this point on.
// Existing files already on disk are re-encrypted (or decrypted) lazily,
// the next time each is saved — SetEncryptor doesn't rewrite files that
// aren't touched again, so a backend directory can end up with a mix of
// encrypted and unencrypted files if the encryptor is changed mid-lifetime.
// For a clean transition, set the encryptor once at open time (see
// StorageConfig.EncryptionKey) rather than toggling it on a live backend.
func (b *DiskBackend) SetEncryptor(enc *Encryptor) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.encryptor = enc
}

// SetReadOnly makes the backend observational. It is normally set by OpenDB
// before the backend is exposed to callers.
func (b *DiskBackend) SetReadOnly(readOnly bool) {
	if b != nil {
		b.readOnly.Store(readOnly)
	}
}

// IsReadOnly reports whether persistence has been disabled for this backend.
func (b *DiskBackend) IsReadOnly() bool {
	return b != nil && b.readOnly.Load()
}

// SetSkipSearchIndexPersistence stops table files from carrying the derived
// FTS and ANN index images (see StorageConfig.SkipSearchIndexPersistence).
// OpenDB sets it before the backend is exposed; toggling it on a live backend
// only affects files written afterwards.
func (b *DiskBackend) SetSkipSearchIndexPersistence(skip bool) {
	if b != nil {
		b.skipSearchIndexes.Store(skip)
	}
}

// SetRowBlockFiles makes the backend write table files in the row-block layout
// (see StorageConfig.RowBlockFiles); files of either layout are read whatever
// the setting. It has no effect on JSON backends or with encryption.
func (b *DiskBackend) SetRowBlockFiles(on bool) {
	if b != nil {
		b.rowBlocks.Store(on)
	}
}

// SkipsSearchIndexPersistence reports whether table files omit the derived
// FTS and ANN index images. DB.Sync uses it (through
// searchIndexPersistenceSkipper) to stop the search-index generation counters
// from making a table look dirty.
func (b *DiskBackend) SkipsSearchIndexPersistence() bool {
	return b != nil && b.skipSearchIndexes.Load()
}

// ──── StorageBackend interface ────────────────────────────────────────────

// LoadTable reads a table from its GOB file on disk.
// Returns (nil, nil) if the table does not exist.
func (b *DiskBackend) LoadTable(tenant, name string) (*Table, error) {
	b.mu.RLock()
	meta := b.getMeta(tenant, name)
	b.mu.RUnlock()
	if meta == nil {
		return nil, nil
	}

	path, err := b.resolveTablePath(meta.FilePath)
	if err != nil {
		return nil, fmt.Errorf("disk backend: invalid file path for %s/%s: %w", tenant, name, err)
	}
	t, err := b.readTableFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("disk backend: load %s/%s: %w", tenant, name, err)
	}

	b.loadCount.Add(1)

	// Track version at load time
	tn := normalizeTenantKey(tenant)
	lc := strings.ToLower(name)
	b.mu.Lock()
	b.ensureVersionMap(tn)
	b.versions[tn][lc] = t.Version
	b.mu.Unlock()

	return t, nil
}

// SaveTable writes a table to its GOB file on disk and updates the manifest.
//
// The caller guarantees that t is not modified while the call runs (DB.SyncTable
// runs under the engine's content write lock; DB.Sync's fallback holds its read
// side). SaveTable itself holds b.mu only for the bookkeeping at the end, not
// while it encodes, so a slow save does not stall ListTableNames, TableExists or
// IsDirty, which every DML statement's foreign-key check goes through.
func (b *DiskBackend) SaveTable(tenant string, t *Table) error {
	_, err := b.saveTable(tenant, t)
	return err
}

// saveTable is SaveTable that also reports whether the table was installed. A
// DeleteTable (a DROP) that ran while the table was being encoded wins: the
// encoded file is discarded and nothing is reported as written, because
// installing it would bring the dropped table back on disk and in the manifest.
// The caller must then not mirror the table anywhere either (HybridBackend
// skips its pool Put).
func (b *DiskBackend) saveTable(tenant string, t *Table) (installed bool, err error) {
	return b.saveTableOpt(tenant, t, true)
}

// errTableFileBusy is returned by saveTableOpt(wait=false) when another writer
// holds the table's file. Nothing was written.
var errTableFileBusy = errors.New("storage: table file is being written by another save")

// saveTableOpt is saveTable; with wait unset it returns errTableFileBusy,
// having written nothing, if another writer (a snapshot write that may take
// minutes) holds the table's file, instead of waiting for it.
func (b *DiskBackend) saveTableOpt(tenant string, t *Table, wait bool) (installed bool, err error) {
	if b.IsReadOnly() {
		return false, ErrReadOnlyStorage
	}
	tn := normalizeTenantKey(tenant)
	relPath, absPath, err := b.buildTablePaths(tn, t.Name)
	if err != nil {
		return false, fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}
	lc := strings.ToLower(t.Name)
	key := tn + "\x00" + lc

	var unlock func()
	if wait {
		unlock = b.lockTableFile(tn, lc)
	} else {
		var ok bool
		if unlock, ok = b.tryLockTableFile(tn, lc); !ok {
			return false, errTableFileBusy
		}
	}
	defer unlock()

	// With the file lock held only a DeleteTable can change the generation
	// before this call installs (every other file writer takes the same lock).
	b.mu.RLock()
	gen := b.gen[key]
	b.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return false, fmt.Errorf("disk backend: mkdir: %w", err)
	}
	cfg := b.fileConfig()
	dt := tableToDiskImage(tn, t, 0, len(t.Rows), true, !b.skipSearchIndexes.Load())
	tmp := absPath + ".tmp"
	if err := cfg.writeTemp(tmp, dt); err != nil {
		return false, fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen[key] != gen {
		_ = os.Remove(tmp)
		return false, nil
	}
	size, err := installTableFile(tmp, absPath)
	if err != nil {
		return false, fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}
	b.tableWrites.Add(1)
	return true, b.recordTableSavedLocked(tn, lc, relPath, t.Name, t.Cols, len(t.Rows), t.Version, size)
}

// recordTableSavedLocked updates the metadata, the dirty-tracking version and
// the write generation after a table file was installed. The caller holds b.mu.
func (b *DiskBackend) recordTableSavedLocked(tn, lc, relPath, name string, cols []Column, rowCount, version int, size int64) error {
	if b.meta[tn] == nil {
		b.meta[tn] = make(map[string]*TableMeta)
	}
	prevMeta, hadMeta := b.meta[tn][lc]
	structuralChange := !hadMeta ||
		prevMeta.FilePath != relPath ||
		prevMeta.Name != name ||
		!reflect.DeepEqual(prevMeta.Cols, cols)

	b.meta[tn][lc] = &TableMeta{
		Tenant:   tn,
		Name:     name,
		Cols:     cols,
		RowCount: rowCount,
		Version:  version,
		DiskSize: size,
		FilePath: relPath,
	}

	// Track version
	b.ensureVersionMap(tn)
	b.versions[tn][lc] = version
	b.bumpGenLocked(tn, lc)

	// Persist manifest immediately only for structural changes (new table,
	// path/schema/name change). Non-structural row/version updates are
	// flushed by Sync/Close, which avoids rewriting the full manifest on
	// every DML write.
	b.manifestDirty = true
	if structuralChange {
		return b.saveManifestLocked()
	}
	return nil
}

// lockTableFile serializes writers of one table's file: the temporary file
// name is derived from the table, so two concurrent saves would share it.
// Writers hold this lock while they encode, which b.mu must not be held for.
func (b *DiskBackend) lockTableFile(tn, lc string) (unlock func()) {
	m, _ := b.tableLocks.LoadOrStore(tn+"\x00"+lc, new(sync.Mutex))
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// tryLockTableFile is lockTableFile that gives up instead of waiting.
func (b *DiskBackend) tryLockTableFile(tn, lc string) (unlock func(), ok bool) {
	m, _ := b.tableLocks.LoadOrStore(tn+"\x00"+lc, new(sync.Mutex))
	mu := m.(*sync.Mutex)
	if !mu.TryLock() {
		return nil, false
	}
	return mu.Unlock, true
}

// genKey is the key of a table in b.gen and b.tableLocks.
func (b *DiskBackend) genKey(tenant, name string) string {
	return normalizeTenantKey(tenant) + "\x00" + strings.ToLower(name)
}

// bumpGenLocked records that something replaced or removed a table's file. The
// caller holds b.mu.
func (b *DiskBackend) bumpGenLocked(tn, lc string) {
	if b.gen == nil {
		b.gen = make(map[string]uint64)
	}
	b.gen[tn+"\x00"+lc]++
}

// beginTableSnapshot returns the write generation a snapshot of the named
// table is based on. A snapshot may be installed only while the generation is
// unchanged: any other save of the table, and any DeleteTable, supersedes it.
func (b *DiskBackend) beginTableSnapshot(tenant, name string) snapshotToken {
	b.mu.RLock()
	defer b.mu.RUnlock()
	key := b.genKey(tenant, name)
	return snapshotToken{gen: b.gen[key], delGen: b.delGen[key]}
}

// deleteGeneration reports how often the named table was dropped (DeleteTable
// or forgetTable) since the backend was opened.
func (b *DiskBackend) deleteGeneration(tenant, name string) uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.delGen[b.genKey(tenant, name)]
}

// forgetTable records a DROP of a table the backend never persisted (a table
// created inside a transaction and dropped before any Sync wrote it). Nothing
// is on disk to remove, but a snapshot of the table that a Sync is writing right
// now must not install it afterwards, and DeleteTable is not called for tables
// the manifest does not know. It takes b.mu, which an installing snapshot holds
// across its check and rename, so the DROP is ordered against the install: the
// snapshot either installed first (and the caller's DeleteTable follows, because
// the table is then on disk) or sees the bumped generation and discards itself.
func (b *DiskBackend) forgetTable(tenant, name string) {
	tn := normalizeTenantKey(tenant)
	lc := strings.ToLower(name)
	b.mu.Lock()
	b.bumpGenLocked(tn, lc)
	b.bumpDelGenLocked(tn, lc)
	b.mu.Unlock()
}

func (b *DiskBackend) bumpDelGenLocked(tn, lc string) {
	if b.delGen == nil {
		b.delGen = make(map[string]uint64)
	}
	b.delGen[tn+"\x00"+lc]++
}

// saveTableSnapshot writes a detached snapshot (see takeTableSnapshot) to the
// table's file without any engine lock held and without holding b.mu while it
// encodes. The file is replaced atomically (temporary file, fsync, rename,
// directory fsync) exactly as SaveTable does, and the table's dirty-tracking
// version becomes the snapshot's version, so a table that changed after the
// snapshot was taken stays dirty.
//
// It returns errSnapshotSuperseded, having changed nothing, when the table was
// saved by someone else or deleted since the snapshot was based
// (beginTableSnapshot): installing it could roll the file back to older rows.
func (b *DiskBackend) saveTableSnapshot(s *tableSnapshot) error {
	if b.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	snap := s.table
	tn := normalizeTenantKey(s.tenant)
	relPath, absPath, err := b.buildTablePaths(tn, snap.Name)
	if err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", s.tenant, snap.Name, err)
	}
	lc := strings.ToLower(snap.Name)
	key := tn + "\x00" + lc

	unlock := b.lockTableFile(tn, lc)
	defer unlock()

	// Do not spend minutes encoding a snapshot that is already obsolete.
	b.mu.RLock()
	stale := b.gen[key] != s.token.gen
	b.mu.RUnlock()
	if stale {
		return errSnapshotSuperseded
	}

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return fmt.Errorf("disk backend: mkdir: %w", err)
	}
	cfg := b.fileConfig()
	dt := snapshotToDisk(tn, s, cfg.format == diskFormatGob)
	tmp := absPath + ".tmp"
	if err := cfg.writeTemp(tmp, dt); err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", s.tenant, snap.Name, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gen[key] != s.token.gen {
		_ = os.Remove(tmp)
		return errSnapshotSuperseded
	}
	size, err := installTableFile(tmp, absPath)
	if err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", s.tenant, snap.Name, err)
	}
	b.tableWrites.Add(1)
	return b.recordTableSavedLocked(tn, lc, relPath, snap.Name, snap.Cols, len(snap.Rows), snap.Version, size)
}

// DeleteTable removes a table file from disk and updates the manifest.
func (b *DiskBackend) DeleteTable(tenant, name string) error {
	if b.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	tn := normalizeTenantKey(tenant)

	b.mu.Lock()
	defer b.mu.Unlock()

	lc := strings.ToLower(name)
	// A snapshot of this table that is being written right now must not
	// resurrect it when it finishes.
	b.bumpGenLocked(tn, lc)
	b.bumpDelGenLocked(tn, lc)
	if tm := b.meta[tn]; tm != nil {
		if meta, ok := tm[lc]; ok {
			absPath, err := b.resolveTablePath(meta.FilePath)
			if err != nil {
				return fmt.Errorf("disk backend: delete %s/%s: %w", tenant, name, err)
			}
			_ = os.Remove(absPath) // best-effort
			delete(tm, lc)
		}
	}
	if vm := b.versions[tn]; vm != nil {
		delete(vm, lc)
	}
	b.manifestDirty = true

	return b.saveManifestLocked()
}

// ListTableNames returns all known table names for a tenant (on disk or in memory).
func (b *DiskBackend) ListTableNames(tenant string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tm := b.meta[strings.ToLower(tenant)]
	if tm == nil {
		return nil, nil
	}
	names := make([]string, 0, len(tm))
	for _, m := range tm {
		names = append(names, m.Name)
	}
	sort.Strings(names)
	return names, nil
}

// TableExists reports whether the table is known to the backend.
func (b *DiskBackend) TableExists(tenant, name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.getMeta(tenant, name) != nil
}

// Sync writes the manifest (individual table files are written immediately
// by SaveTable). The caller should call SaveTable for each dirty table before
// calling Sync, or use DB.Sync which does this automatically.
func (b *DiskBackend) Sync() error {
	if b.IsReadOnly() {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.syncCount.Add(1)
	if !b.manifestDirty {
		return nil
	}
	return b.saveManifestLocked()
}

// Close is like Sync but also cleans up any resources.
func (b *DiskBackend) Close() error {
	return b.Sync()
}

func (b *DiskBackend) Mode() StorageMode { return b.mode }

func (b *DiskBackend) Stats() BackendStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var diskTables int
	var diskBytes int64
	for _, tm := range b.meta {
		for _, m := range tm {
			diskTables++
			diskBytes += m.DiskSize
		}
	}
	return BackendStats{
		Mode:          b.mode,
		TablesOnDisk:  diskTables,
		DiskUsedBytes: diskBytes,
		SyncCount:     b.syncCount.Load(),
		TableWrites:   b.tableWrites.Load(),
		LoadCount:     b.loadCount.Load(),
		EvictionCount: b.evictionCount.Load(),
	}
}

// IsDirty reports whether the named table has been modified since it was
// last saved. Uses the version field for comparison.
func (b *DiskBackend) IsDirty(tenant, name string, currentVersion int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	lc := strings.ToLower(name)
	if vm := b.versions[strings.ToLower(tenant)]; vm != nil {
		if v, ok := vm[lc]; ok {
			return currentVersion != v
		}
	}
	return true // unknown → assume dirty
}

// GetMeta returns table metadata without loading the full table.
// Returns nil if the table is unknown.
func (b *DiskBackend) GetMeta(tenant, name string) *TableMeta {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.getMeta(tenant, name)
}

// ──── internal helpers ───────────────────────────────────────────────────

func (b *DiskBackend) getMeta(tenant, name string) *TableMeta {
	tm := b.meta[normalizeTenantKey(tenant)]
	if tm == nil {
		return nil
	}
	return tm[strings.ToLower(name)]
}

func (b *DiskBackend) ensureVersionMap(tenant string) {
	tn := normalizeTenantKey(tenant)
	if b.versions[tn] == nil {
		b.versions[tn] = make(map[string]int)
	}
}

func normalizeTenantKey(tenant string) string {
	return strings.ToLower(tenant)
}

func validatePathSegment(seg, field string, allowEmpty bool) error {
	if seg == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("%s must not be empty", field)
	}
	if seg == "." || seg == ".." {
		return fmt.Errorf("%s must not be %q", field, seg)
	}
	if strings.Contains(seg, "/") || strings.Contains(seg, "\\") {
		return fmt.Errorf("%s must not contain path separators", field)
	}
	if strings.IndexByte(seg, 0) >= 0 {
		return fmt.Errorf("%s contains invalid NUL byte", field)
	}
	return nil
}

func sanitizeRelativePath(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("path is empty")
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	clean := filepath.Clean(rel)
	if clean == "." {
		return "", fmt.Errorf("path is empty")
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes database directory")
	}
	if vol := filepath.VolumeName(clean); vol != "" {
		return "", fmt.Errorf("volume-qualified paths are not allowed")
	}
	return clean, nil
}

func (b *DiskBackend) resolveTablePath(rel string) (string, error) {
	cleanRel, err := sanitizeRelativePath(rel)
	if err != nil {
		return "", err
	}
	root := filepath.Clean(b.dir)
	full := filepath.Join(root, cleanRel)
	relToRoot, err := filepath.Rel(root, full)
	if err != nil {
		return "", err
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("resolved path escapes database directory")
	}
	return full, nil
}

func (b *DiskBackend) buildTablePaths(tenant, tableName string) (string, string, error) {
	if err := validatePathSegment(tenant, "tenant", true); err != nil {
		return "", "", err
	}
	lc := strings.ToLower(tableName)
	if err := validatePathSegment(lc, "table name", false); err != nil {
		return "", "", err
	}
	rel := filepath.Join(tenant, lc+b.fileExt())
	abs, err := b.resolveTablePath(rel)
	if err != nil {
		return "", "", err
	}
	return rel, abs, nil
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		// Directory fsync is not portable on Windows.
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		// Some filesystems do not support directory fsync.
		if errors.Is(err, os.ErrInvalid) ||
			errors.Is(err, os.ErrPermission) ||
			errors.Is(err, syscall.EINVAL) ||
			errors.Is(err, syscall.ENOTSUP) ||
			errors.Is(err, syscall.EPERM) {
			return nil
		}
		return err
	}
	return nil
}

func (b *DiskBackend) fileExt() string {
	switch b.format {
	case diskFormatJSON:
		if b.gzip {
			return ".json.gz"
		}
		return ".json"
	default:
		if b.gzip {
			return ".tbl.gz"
		}
		return ".tbl"
	}
}

func init() {
	// Ensure diskTable is registered for individual file encoding.
	safeGobRegister(diskTable{})
}

func (b *DiskBackend) readTableFile(path string) (*Table, error) {
	var r io.Reader
	var closers []io.Closer
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
	}()

	if b.encryptor != nil {
		// GCM authenticates the whole ciphertext as one unit, so the file
		// must be read in full before any of it can be decrypted — no
		// streaming decrypt-as-you-go the way the unencrypted path streams
		// decompression.
		ciphertext, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		plaintext, err := b.encryptor.Decrypt(ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decrypt table file %s: %w", path, err)
		}
		r = bytes.NewReader(plaintext)
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		closers = append(closers, f)
		reader := borrowDiskReader(f)
		defer releaseDiskReader(reader)
		r = reader
	}

	// A row-block file (see disk_rowblocks.go) is recognised by its magic,
	// whatever the configuration or the file name say; its compression, if any,
	// is per block. No classic gob stream starts with that byte sequence.
	if b.format != diskFormatJSON {
		var head []byte
		switch src := r.(type) {
		case *bytes.Reader: // decrypted plaintext
			head = make([]byte, len(rowBlockMagic))
			if n, _ := src.ReadAt(head, 0); n < len(head) {
				head = head[:n]
			}
		case interface{ Peek(int) ([]byte, error) }:
			head, _ = src.Peek(len(rowBlockMagic))
		}
		if hasRowBlockMagic(head) {
			dt, err := decodeRowBlockFile(r)
			if err != nil {
				return nil, err
			}
			return b.tableFromDisk(dt), nil
		}
	}

	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		closers = append(closers, gr)
		r = gr
	}

	var dt diskTable
	if b.format == diskFormatJSON {
		decoder := json.NewDecoder(r)
		decoder.UseNumber()
		if err := decoder.Decode(&dt); err != nil {
			return nil, err
		}

		// encoding/json represents []byte as base64 strings inside interface
		// cells. Restore native BLOBs using schema information, while leaving
		// SQLite-affinity columns (which may store literal strings) alone.
		for _, row := range dt.Rows {
			for i, col := range dt.Cols {
				if i < len(row) {
					if number, ok := row[i].(json.Number); ok && col.Type == IntType {
						// INT columns hold Go ints. Decoding them through
						// float64 changed their type and lost precision above
						// 2^53. A declared INT/INTEGER column has SQLite
						// integer affinity, which may also keep a REAL it could
						// not store losslessly; only that case stays float64.
						value, err := strconv.Atoi(string(number))
						switch {
						case err == nil:
							row[i] = value
						case col.Affinity == AffinityDefault:
							return nil, fmt.Errorf("decode INT column %q: %w", col.Name, err)
						default:
							real, err := number.Float64()
							if err != nil {
								return nil, fmt.Errorf("decode INT column %q: %w", col.Name, err)
							}
							row[i] = real
						}
					} else {
						value, err := normalizeJSONNumbers(row[i])
						if err != nil {
							return nil, fmt.Errorf("decode column %q: %w", col.Name, err)
						}
						row[i] = value
					}
				}
				if i >= len(row) || col.Type != BlobType || col.Affinity != AffinityDefault {
					continue
				}
				if encoded, ok := row[i].(string); ok {
					decoded, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						return nil, fmt.Errorf("decode BLOB column %q: %w", col.Name, err)
					}
					row[i] = decoded
				}
			}
		}
	} else {
		if err := gob.NewDecoder(r).Decode(&dt); err != nil {
			return nil, err
		}
	}
	return b.tableFromDisk(dt), nil
}

// tableFromDisk turns a decoded file image into a table, applying the
// configuration's view of the search-index images.
func (b *DiskBackend) tableFromDisk(dt diskTable) *Table {
	if b.skipSearchIndexes.Load() {
		// Images left by a version (or configuration) that persisted them are
		// not wanted: the engine rebuilds an index lazily from the rows, and
		// keeping a stale image would pin memory the flag exists to save.
		dt.FTSIndexes = nil
		dt.VectorIndexes = nil
	}
	// diskToTable normalizes vector cells regardless of source format: GOB
	// preserves []float64 exactly, JSON decodes numeric arrays as []any.
	return diskToTableOwned(dt)
}

// ──── Manifest (JSON) ───────────────────────────────────────────────────

type manifest struct {
	Version   int                              `json:"version"`
	CreatedAt time.Time                        `json:"created_at"`
	UpdatedAt time.Time                        `json:"updated_at"`
	Tenants   map[string]map[string]manifestTM `json:"tenants"`
}

type manifestTM struct {
	Name     string        `json:"name"`
	Cols     []manifestCol `json:"cols"`
	RowCount int           `json:"row_count"`
	Version  int           `json:"version"`
	DiskSize int64         `json:"disk_size"`
	FilePath string        `json:"file_path"`
}

type manifestCol struct {
	Name         string         `json:"name"`
	Type         ColType        `json:"type"`
	Constraint   ConstraintType `json:"constraint,omitempty"`
	PointerTable string         `json:"pointer_table,omitempty"`
	ForeignKey   *ForeignKeyRef `json:"foreign_key,omitempty"`
}

func (b *DiskBackend) manifestPath() string {
	return filepath.Join(b.dir, "manifest.json")
}

func (b *DiskBackend) loadManifest() error {
	data, err := os.ReadFile(b.manifestPath())
	if err != nil {
		return err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("disk backend: parse manifest: %w", err)
	}
	if m.CreatedAt.IsZero() {
		b.manifestCreatedAt = time.Now()
	} else {
		b.manifestCreatedAt = m.CreatedAt
	}
	for tn, tables := range m.Tenants {
		tn = normalizeTenantKey(tn)
		if err := validatePathSegment(tn, "manifest tenant", true); err != nil {
			return fmt.Errorf("disk backend: invalid tenant %q: %w", tn, err)
		}
		if b.meta[tn] == nil {
			b.meta[tn] = make(map[string]*TableMeta)
		}
		if b.versions[tn] == nil {
			b.versions[tn] = make(map[string]int)
		}
		for key, tm := range tables {
			lc := strings.ToLower(tm.Name)
			if lc == "" {
				lc = strings.ToLower(key)
			}
			if err := validatePathSegment(lc, "manifest table", false); err != nil {
				return fmt.Errorf("disk backend: invalid table %q for tenant %q: %w", tm.Name, tn, err)
			}
			cleanPath, err := sanitizeRelativePath(tm.FilePath)
			if err != nil {
				return fmt.Errorf("disk backend: invalid file path for %s/%s: %w", tn, tm.Name, err)
			}
			cols := make([]Column, len(tm.Cols))
			for i, c := range tm.Cols {
				cols[i] = Column{
					Name:         c.Name,
					Type:         c.Type,
					Constraint:   c.Constraint,
					PointerTable: c.PointerTable,
					ForeignKey:   c.ForeignKey,
				}
			}
			b.meta[tn][lc] = &TableMeta{
				Tenant:   tn,
				Name:     tm.Name,
				Cols:     cols,
				RowCount: tm.RowCount,
				Version:  tm.Version,
				DiskSize: tm.DiskSize,
				FilePath: cleanPath,
			}
			b.versions[tn][lc] = tm.Version
		}
	}
	b.manifestDirty = false
	return nil
}

func (b *DiskBackend) saveManifestLocked() error {
	if b.manifestCreatedAt.IsZero() {
		b.manifestCreatedAt = time.Now()
	}
	m := manifest{
		Version:   1,
		CreatedAt: b.manifestCreatedAt,
		UpdatedAt: time.Now(),
		Tenants:   make(map[string]map[string]manifestTM),
	}
	for tn, tables := range b.meta {
		m.Tenants[tn] = make(map[string]manifestTM)
		for lc, meta := range tables {
			cols := make([]manifestCol, len(meta.Cols))
			for i, c := range meta.Cols {
				cols[i] = manifestCol{
					Name:         c.Name,
					Type:         c.Type,
					Constraint:   c.Constraint,
					PointerTable: c.PointerTable,
					ForeignKey:   c.ForeignKey,
				}
			}
			m.Tenants[tn][lc] = manifestTM{
				Name:     meta.Name,
				Cols:     cols,
				RowCount: meta.RowCount,
				Version:  meta.Version,
				DiskSize: meta.DiskSize,
				FilePath: meta.FilePath,
			}
		}
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	path := b.manifestPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	b.manifestDirty = false
	return nil
}

// ──── Migration: import an existing GOB database into the disk backend ──

// ImportFromDB takes all tables from an existing in-memory DB and writes
// them as individual table files. This is the migration path from
// ModeMemory / GOB checkpoints to ModeDisk.
func (b *DiskBackend) ImportFromDB(db *DB) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			if err := b.SaveTable(tn, t); err != nil {
				return fmt.Errorf("import %s/%s: %w", tn, t.Name, err)
			}
		}
	}
	return nil
}
