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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

	path := filepath.Join(b.dir, meta.FilePath)
	t, err := b.readTableFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("disk backend: load %s/%s: %w", tenant, name, err)
	}

	b.loadCount.Add(1)

	// Track version at load time
	b.mu.Lock()
	b.ensureVersionMap(tenant)
	b.versions[tenant][strings.ToLower(name)] = t.Version
	b.mu.Unlock()

	return t, nil
}

// SaveTable writes a table to its GOB file on disk and updates the manifest.
func (b *DiskBackend) SaveTable(tenant string, t *Table) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	lc := strings.ToLower(t.Name)
	relPath := filepath.Join(tenant, lc+b.fileExt())
	absPath := filepath.Join(b.dir, relPath)

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return fmt.Errorf("disk backend: mkdir: %w", err)
	}
	size, err := b.writeTableFile(absPath, tenant, t)
	if err != nil {
		return fmt.Errorf("disk backend: save %s/%s: %w", tenant, t.Name, err)
	}

	// Update metadata
	if b.meta[tenant] == nil {
		b.meta[tenant] = make(map[string]*TableMeta)
	}
	b.meta[tenant][lc] = &TableMeta{
		Tenant:   tenant,
		Name:     t.Name,
		Cols:     t.Cols,
		RowCount: len(t.Rows),
		Version:  t.Version,
		DiskSize: size,
		FilePath: relPath,
	}

	// Track version
	b.ensureVersionMap(tenant)
	b.versions[tenant][lc] = t.Version

	return b.saveManifestLocked()
}

// DeleteTable removes a table file from disk and updates the manifest.
func (b *DiskBackend) DeleteTable(tenant, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	lc := strings.ToLower(name)
	if tm := b.meta[tenant]; tm != nil {
		if meta, ok := tm[lc]; ok {
			absPath := filepath.Join(b.dir, meta.FilePath)
			_ = os.Remove(absPath) // best-effort
			delete(tm, lc)
		}
	}
	if vm := b.versions[tenant]; vm != nil {
		delete(vm, lc)
	}

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
	tm := b.meta[strings.ToLower(tenant)]
	if tm == nil {
		return nil
	}
	return tm[strings.ToLower(name)]
}

func (b *DiskBackend) ensureVersionMap(tenant string) {
	tn := strings.ToLower(tenant)
	if b.versions[tn] == nil {
		b.versions[tn] = make(map[string]int)
	}
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

	info, err := os.Stat(path)
	if err != nil {
		return 0, nil
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
	for tn, tables := range m.Tenants {
		if b.meta[tn] == nil {
			b.meta[tn] = make(map[string]*TableMeta)
		}
		if b.versions[tn] == nil {
			b.versions[tn] = make(map[string]int)
		}
		for lc, tm := range tables {
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
				FilePath: tm.FilePath,
			}
			b.versions[tn][lc] = tm.Version
		}
	}
	return nil
}

func (b *DiskBackend) saveManifestLocked() error {
	m := manifest{
		Version:   1,
		CreatedAt: time.Now(),
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
	tmp := b.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, b.manifestPath())
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
