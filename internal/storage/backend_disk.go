package storage

import (
	"bufio"
	"compress/gzip"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ───────────────────────────────────────────────────────────────────────────
// DiskBackend – every table is a separate GOB file on disk
// ───────────────────────────────────────────────────────────────────────────

// DiskBackend stores each table as an individual file under a directory tree:
//
//	<dir>/<tenant>/<tablename>.tbl      (GOB-encoded diskTable)
//	<dir>/manifest.json                 (lightweight metadata index)
//
// Tables are loaded into memory on demand (via LoadTable) and written back
// on SaveTable / Sync. This minimises RAM usage while keeping the file
// format compatible with the existing GOB serialisation.
type DiskBackend struct {
	mu   sync.RWMutex
	dir  string
	gzip bool

	// meta tracks every known table (on disk or in memory).
	meta map[string]map[string]*TableMeta // tenant → lower(name) → meta

	// versions tracks the version at last save so we can detect dirty tables.
	versions map[string]map[string]int // tenant → lower(name) → version

	// Manifest timestamp bookkeeping.
	manifestCreatedAt time.Time
	manifestDirty     bool

	// stats
	syncCount     atomic.Int64
	loadCount     atomic.Int64
	evictionCount atomic.Int64
}

// NewDiskBackend opens (or creates) a disk-backed storage directory.
// It reads the manifest to learn which tables exist without loading them.
func NewDiskBackend(dir string, compress bool) (*DiskBackend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("disk backend: create dir: %w", err)
	}
	b := &DiskBackend{
		dir:      dir,
		gzip:     compress,
		meta:     make(map[string]map[string]*TableMeta),
		versions: make(map[string]map[string]int),
	}
	if err := b.loadManifest(); err != nil {
		// If manifest doesn't exist, that's fine – new database.
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return b, nil
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
func (b *DiskBackend) SaveTable(tenant string, t *Table) error {
	tn := normalizeTenantKey(tenant)
	relPath, absPath, err := b.buildTablePaths(tn, t.Name)
	if err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	lc := strings.ToLower(t.Name)

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return fmt.Errorf("disk backend: mkdir: %w", err)
	}
	size, err := b.writeTableFile(absPath, tn, t)
	if err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}

	// Update metadata
	if b.meta[tn] == nil {
		b.meta[tn] = make(map[string]*TableMeta)
	}
	prevMeta, hadMeta := b.meta[tn][lc]
	structuralChange := !hadMeta ||
		prevMeta.FilePath != relPath ||
		prevMeta.Name != t.Name ||
		!reflect.DeepEqual(prevMeta.Cols, t.Cols)

	b.meta[tn][lc] = &TableMeta{
		Tenant:   tn,
		Name:     t.Name,
		Cols:     t.Cols,
		RowCount: len(t.Rows),
		Version:  t.Version,
		DiskSize: size,
		FilePath: relPath,
	}

	// Track version
	b.ensureVersionMap(tn)
	b.versions[tn][lc] = t.Version

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

// DeleteTable removes a table file from disk and updates the manifest.
func (b *DiskBackend) DeleteTable(tenant, name string) error {
	tn := normalizeTenantKey(tenant)

	b.mu.Lock()
	defer b.mu.Unlock()

	lc := strings.ToLower(name)
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

func (b *DiskBackend) Mode() StorageMode { return ModeDisk }

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
		Mode:          ModeDisk,
		TablesOnDisk:  diskTables,
		DiskUsedBytes: diskBytes,
		SyncCount:     b.syncCount.Load(),
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
	defer f.Close()
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
	if b.gzip {
		return ".tbl.gz"
	}
	return ".tbl"
}

// ──── File I/O ──────────────────────────────────────────────────────────

func (b *DiskBackend) writeTableFile(path, tenant string, t *Table) (int64, error) {
	dt := tableToDisk(tenant, t)

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}

	bw := bufio.NewWriterSize(f, 64*1024)
	var w io.Writer = bw
	var gz *gzip.Writer
	if b.gzip {
		gz = gzip.NewWriter(bw)
		w = gz
	}

	enc := gob.NewEncoder(w)
	encErr := enc.Encode(dt)

	// Close gzip layer first (flushes compressed data to bw)
	if gz != nil {
		if err := gz.Close(); err != nil && encErr == nil {
			encErr = err
		}
	}
	// Flush the bufio layer
	if err := bw.Flush(); err != nil && encErr == nil {
		encErr = err
	}
	// Sync and close the file
	if err := f.Sync(); err != nil && encErr == nil {
		encErr = err
	}
	if err := f.Close(); err != nil && encErr == nil {
		encErr = err
	}
	if encErr != nil {
		_ = os.Remove(tmp)
		return 0, encErr
	}

	// Atomic rename for crash safety
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return 0, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func init() {
	// Ensure diskTable is registered for individual file encoding.
	safeGobRegister(diskTable{})
}

func (b *DiskBackend) readTableFile(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var r io.Reader = bufio.NewReaderSize(f, 64*1024)
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		r = gr
	}

	var dt diskTable
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&dt); err != nil {
		return nil, err
	}
	return diskToTable(dt), nil
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
