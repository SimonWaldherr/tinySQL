package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SimonWaldherr/tinySQL/internal/storage/pager"
)

// PagedIndexBackend adapts the pager B+Tree format to StorageBackend. Unlike
// ModeIndex's legacy GOB table files, it persists table rows and materialized
// secondary indexes in distinct B+Trees. Exact seeks can therefore read an
// index key, its row locator and only the requested rows/BLOBs.
//
// It is deliberately a separate mode: existing ModeIndex directories retain
// their GOB compatibility, while published `paged_index` artifacts have an
// explicit format boundary and can be opened safely read-only.
type PagedIndexBackend struct {
	page     *pager.PageBackend
	path     string
	readOnly atomic.Bool

	// metadata contains schema and index roots only (never rows/BLOBs). It is
	// immutable after a published read-only open and avoids a catalog B+Tree
	// traversal plus JSON decode on every prepared point lookup.
	metadataMu sync.RWMutex
	metadata   map[string]*pagedIndexTableMetadata
}

// pagedIndexTableMetadata is the immutable serving descriptor cached per
// table. It intentionally contains no rows or BLOBs: only schema and B+Tree
// locators needed to avoid re-reading the catalog on every point lookup.
type pagedIndexTableMetadata struct {
	table     *Table
	tableRoot pager.PageID
	indexRoot map[string]pager.PageID
}

const pagedIndexFilename = "tinysql.pages"

func NewPagedIndexBackend(dir string, maxMemoryBytes int64, readOnly bool) (*PagedIndexBackend, error) {
	if readOnly {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			if err == nil {
				err = fmt.Errorf("not a directory")
			}
			return nil, fmt.Errorf("read-only paged index requires existing directory %q: %w", dir, err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, pagedIndexFilename)
	if readOnly {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("read-only paged index requires published artifact %q: %w", path, err)
		}
	}
	maxPages := int(maxMemoryBytes / pager.DefaultPageSize)
	if maxPages < 1 {
		maxPages = 1
	}
	pb, err := pager.NewPageBackend(pager.PageBackendConfig{
		Path:          path,
		MaxCachePages: maxPages,
		ReadOnly:      readOnly,
	})
	if err != nil {
		return nil, err
	}
	b := &PagedIndexBackend{page: pb, path: path, metadata: make(map[string]*pagedIndexTableMetadata)}
	b.readOnly.Store(readOnly)
	return b, nil
}

func (b *PagedIndexBackend) LoadTable(tenant, name string) (*Table, error) {
	td, err := b.page.LoadTable(tenant, name)
	if err != nil || td == nil {
		return nil, err
	}
	t := NewTable(td.Name, pagerColumnsToStorage(td.Columns), td.IsTemp)
	t.Rows = td.Rows
	t.Version = td.Version
	for _, index := range td.Indexes {
		if err := t.CreateSecondaryIndex(index.Name, index.Columns, index.Unique); err != nil {
			return nil, fmt.Errorf("rebuild paged index %s: %w", index.Name, err)
		}
	}
	return t, nil
}

func (b *PagedIndexBackend) SaveTable(tenant string, t *Table) error {
	if b.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	td := &pager.TableData{
		Name:    t.Name,
		Columns: storageColumnsToPager(t.Cols),
		Rows:    t.Rows,
		IsTemp:  t.IsTemp,
		Version: t.Version,
		Indexes: make([]pager.IndexInfo, 0, len(t.Indexes)),
	}
	for _, index := range t.Indexes {
		td.Indexes = append(td.Indexes, pager.IndexInfo{
			Name:    index.Name,
			Columns: append([]string(nil), index.Columns...),
			Unique:  index.Unique,
			Entries: indexEntriesToPager(index.Entries),
		})
	}
	if err := b.page.SaveTable(tenant, td); err != nil {
		return err
	}
	b.dropMetadata(tenant, t.Name)
	return nil
}

func (b *PagedIndexBackend) DeleteTable(tenant, name string) error {
	if b.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	if err := b.page.DeleteTable(tenant, name); err != nil {
		return err
	}
	b.dropMetadata(tenant, name)
	return nil
}

func (b *PagedIndexBackend) ListTableNames(tenant string) ([]string, error) {
	return b.page.ListTableNames(tenant)
}

func (b *PagedIndexBackend) TableExists(tenant, name string) bool {
	return b.page.TableExists(tenant, name)
}

func (b *PagedIndexBackend) Sync() error {
	if b.IsReadOnly() {
		return nil
	}
	return b.page.Sync()
}

func (b *PagedIndexBackend) Close() error              { return b.page.Close() }
func (b *PagedIndexBackend) Mode() StorageMode         { return ModePagedIndex }
func (b *PagedIndexBackend) SetReadOnly(readOnly bool) { b.readOnly.Store(readOnly) }
func (b *PagedIndexBackend) IsReadOnly() bool          { return b != nil && b.readOnly.Load() }

func (b *PagedIndexBackend) Stats() BackendStats {
	stats := b.page.Stats()
	info, _ := os.Stat(b.path)
	var size int64
	if info != nil {
		size = info.Size()
	}
	var hitRate float64
	if total := stats.CacheHits + stats.CacheMisses; total > 0 {
		hitRate = float64(stats.CacheHits) / float64(total)
	}
	return BackendStats{
		Mode:             ModePagedIndex,
		DiskUsedBytes:    size,
		MemoryUsedBytes:  int64(stats.CachedPages+stats.TransientFrames) * int64(stats.PageSize),
		MemoryLimitBytes: int64(stats.PageSize) * int64(b.page.MaxCachePages()),
		CacheHitRate:     hitRate,
		SyncCount:        stats.SyncCount,
		LoadCount:        stats.LoadCount,
		PageReads:        stats.PageReads,
		CacheHits:        stats.CacheHits,
		CacheMisses:      stats.CacheMisses,
		CachedPages:      stats.CachedPages,
		PinnedPages:      stats.PinnedPages,
		TransientPages:   stats.TransientPages,
		TransientFrames:  stats.TransientFrames,
		MaxCachePages:    stats.MaxCachePages,
	}
}

// IndexMetadata creates a schema-only Table with physical index metadata. It
// is a cheap operation (catalog B+Tree only) and lets the SQL planner choose a
// page index before calling LoadTable.
func (b *PagedIndexBackend) IndexMetadata(tenant, name string) (*Table, error) {
	key := pagedMetadataKey(tenant, name)
	b.metadataMu.RLock()
	cached := b.metadata[key]
	b.metadataMu.RUnlock()
	if cached != nil {
		return cached.table, nil
	}
	entry, err := b.page.TableEntry(tenant, name)
	if err != nil || entry == nil {
		return nil, err
	}
	t := NewTable(entry.Table, pagerCatalogColumnsToStorage(entry.Columns), false)
	t.Version = entry.Version
	for _, index := range entry.Indexes {
		t.Indexes[strings.ToLower(index.Name)] = &SecondaryIndex{
			Name:    index.Name,
			Columns: append([]string(nil), index.Columns...),
			Unique:  index.Unique,
		}
	}
	metadata := &pagedIndexTableMetadata{
		table:     t,
		tableRoot: entry.RootPageID,
		indexRoot: make(map[string]pager.PageID, len(entry.Indexes)),
	}
	for _, index := range entry.Indexes {
		metadata.indexRoot[strings.ToLower(index.Name)] = index.RootPageID
	}
	b.metadataMu.Lock()
	if existing := b.metadata[key]; existing != nil {
		b.metadataMu.Unlock()
		return existing.table, nil
	}
	b.metadata[key] = metadata
	b.metadataMu.Unlock()
	return t, nil
}

// LookupIndexRows resolves a complete canonical composite key through the
// on-disk index and returns only matching rows. The caller owns the returned
// slices and can safely project/copy BLOBs without exposing page-cache memory.
func (b *PagedIndexBackend) LookupIndexRows(tenant, table, indexName string, values []any) ([][]any, bool, error) {
	key := pagedMetadataKey(tenant, table)
	b.metadataMu.RLock()
	metadata := b.metadata[key]
	b.metadataMu.RUnlock()
	if metadata == nil {
		// Populate the immutable locator cache once.
		if _, err := b.IndexMetadata(tenant, table); err != nil {
			return nil, false, err
		}
		b.metadataMu.RLock()
		metadata = b.metadata[key]
		b.metadataMu.RUnlock()
	}
	if metadata == nil {
		return nil, false, nil
	}
	root, ok := metadata.indexRoot[strings.ToLower(indexName)]
	if !ok || root == pager.InvalidPageID {
		return nil, false, nil
	}
	return b.page.LookupIndexRowsByRoot(metadata.tableRoot, root, indexName, CanonicalIndexKey(values))
}

func storageColumnsToPager(cols []Column) []pager.ColumnInfo {
	out := make([]pager.ColumnInfo, len(cols))
	for i, col := range cols {
		out[i] = pager.ColumnInfo{
			Name:         col.Name,
			Type:         int(col.Type),
			Constraint:   int(col.Constraint),
			PointerTable: col.PointerTable,
		}
		if col.ForeignKey != nil {
			out[i].FKTable = col.ForeignKey.Table
			out[i].FKColumn = col.ForeignKey.Column
		}
	}
	return out
}

func pagerColumnsToStorage(cols []pager.ColumnInfo) []Column {
	out := make([]Column, len(cols))
	for i, col := range cols {
		out[i] = Column{
			Name:         col.Name,
			Type:         ColType(col.Type),
			Constraint:   ConstraintType(col.Constraint),
			PointerTable: col.PointerTable,
		}
		if col.FKTable != "" {
			out[i].ForeignKey = &ForeignKeyRef{Table: col.FKTable, Column: col.FKColumn}
		}
	}
	return out
}

func pagerCatalogColumnsToStorage(cols []pager.CatalogColumn) []Column {
	out := make([]Column, len(cols))
	for i, col := range cols {
		out[i] = Column{
			Name:         col.Name,
			Type:         ColType(col.Type),
			Constraint:   ConstraintType(col.Constraint),
			PointerTable: col.PtrTable,
		}
		if col.FKTable != "" {
			out[i].ForeignKey = &ForeignKeyRef{Table: col.FKTable, Column: col.FKColumn}
		}
	}
	return out
}

func indexEntriesToPager(entries []IndexEntry) []pager.IndexEntry {
	out := make([]pager.IndexEntry, len(entries))
	for i, entry := range entries {
		out[i].Key = append([]byte(nil), entry.Key...)
		out[i].RowIDs = append([]int(nil), entry.RowIDs...)
	}
	return out
}

func (b *PagedIndexBackend) dropMetadata(tenant, name string) {
	b.metadataMu.Lock()
	delete(b.metadata, pagedMetadataKey(tenant, name))
	b.metadataMu.Unlock()
}

func pagedMetadataKey(tenant, name string) string {
	return strings.ToLower(tenant) + "\x00" + strings.ToLower(name)
}
