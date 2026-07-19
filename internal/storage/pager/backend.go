// Package pager — PageBackend integrates the page-based storage engine
// with the tinySQL StorageBackend interface.
package pager

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// ───────────────────────────────────────────────────────────────────────────
// PageBackend
// ───────────────────────────────────────────────────────────────────────────

// PageBackendConfig configures the page-based StorageBackend.
type PageBackendConfig struct {
	Path          string // database file path (.db)
	PageSize      int    // 0 = DefaultPageSize (8 KiB)
	MaxCachePages int    // buffer pool size (0 = default 1024)
	ReadOnly      bool
}

// PageBackend implements a disk-based key-value store backed by B+Trees,
// a WAL for crash safety, and a buffer pool for caching.
type PageBackend struct {
	mu      sync.RWMutex
	pager   *Pager
	catalog *Catalog
	config  PageBackendConfig
	closed  bool

	// Stats counters.
	syncCount atomic.Int64
	loadCount atomic.Int64
}

// NewPageBackend opens or creates a page-based database.
func NewPageBackend(cfg PageBackendConfig) (*PageBackend, error) {
	ps := cfg.PageSize
	if ps == 0 {
		ps = DefaultPageSize
	}

	walPath := cfg.Path + ".wal"

	pager, err := OpenPager(PagerConfig{
		DBPath:        cfg.Path,
		WALPath:       walPath,
		PageSize:      ps,
		MaxCachePages: cfg.MaxCachePages,
		ReadOnly:      cfg.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("open page backend: %w", err)
	}

	var cat *Catalog
	if cfg.ReadOnly {
		if pager.Superblock().CatalogRoot == InvalidPageID {
			_ = pager.Close()
			return nil, fmt.Errorf("read-only page backend has no published catalog")
		}
		cat, err = OpenCatalog(pager, 0)
		if err != nil {
			_ = pager.Close()
			return nil, fmt.Errorf("open read-only catalog: %w", err)
		}
	} else {
		// Open or create catalog within a transaction.
		txID, txErr := pager.BeginTx()
		if txErr != nil {
			_ = pager.Close()
			return nil, txErr
		}
		cat, err = OpenCatalog(pager, txID)
		if err != nil {
			_ = pager.Close()
			return nil, fmt.Errorf("open catalog: %w", err)
		}
		// Update superblock with catalog root.
		pager.UpdateSuperblock(func(sb *Superblock) {
			sb.CatalogRoot = cat.Root()
		})
		if err := pager.CommitTx(txID); err != nil {
			_ = pager.Close()
			return nil, err
		}
	}

	return &PageBackend{
		pager:   pager,
		catalog: cat,
		config:  cfg,
	}, nil
}

// ── Column conversion helpers ─────────────────────────────────────────────

func columnsToCatalog(cols []ColumnInfo) []CatalogColumn {
	out := make([]CatalogColumn, len(cols))
	for i, c := range cols {
		cc := CatalogColumn{
			Name:       c.Name,
			Type:       c.Type,
			Constraint: c.Constraint,
			PtrTable:   c.PointerTable,
		}
		if c.FKTable != "" {
			cc.FKTable = c.FKTable
			cc.FKColumn = c.FKColumn
		}
		out[i] = cc
	}
	return out
}

func catalogToColumns(cats []CatalogColumn) []ColumnInfo {
	out := make([]ColumnInfo, len(cats))
	for i, cc := range cats {
		out[i] = ColumnInfo{
			Name:         cc.Name,
			Type:         cc.Type,
			Constraint:   cc.Constraint,
			FKTable:      cc.FKTable,
			FKColumn:     cc.FKColumn,
			PointerTable: cc.PtrTable,
		}
	}
	return out
}

// ColumnInfo is a simplified, pager-internal column descriptor that does not
// import the storage package (to avoid circular dependencies).
type ColumnInfo struct {
	Name         string
	Type         int // ColType as int
	Constraint   int // ConstraintType as int
	FKTable      string
	FKColumn     string
	PointerTable string
}

// ── Table I/O ─────────────────────────────────────────────────────────────

// TableData is the pager-level representation of a table. Higher layers
// convert between TableData and storage.Table.
type TableData struct {
	Name    string
	Columns []ColumnInfo
	Rows    [][]any
	Indexes []IndexInfo
	IsTemp  bool
	Version int
}

// LoadTable retrieves all rows of a table from its B+Tree.
func (pb *PageBackend) LoadTable(tenant, name string) (*TableData, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	pb.loadCount.Add(1)

	entry, err := pb.catalog.GetEntry(tenant, name)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil // not found
	}

	// Read all rows from the table's B+Tree.
	bt := NewBTree(pb.pager, entry.RootPageID)
	rows := make([][]any, 0, entry.RowCount)
	err = bt.ScanRange(RowKey(0), nil, func(key, val []byte) bool {
		row, decErr := UnmarshalRow(val)
		if decErr != nil {
			return false
		}
		rows = append(rows, row)
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("load table %s/%s: %w", tenant, name, err)
	}

	return &TableData{
		Name:    entry.Table,
		Columns: catalogToColumns(entry.Columns),
		Rows:    rows,
		Indexes: cloneIndexInfos(entry.Indexes),
		Version: entry.Version,
	}, nil
}

// SaveTable persists all rows of a table into a B+Tree.
// This replaces the entire table contents (drop + recreate of the tree).
func (pb *PageBackend) SaveTable(tenant string, td *TableData) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	txID, err := pb.pager.BeginTx()
	if err != nil {
		return err
	}

	// Check if table already exists.
	existingEntry, _ := pb.catalog.GetEntry(tenant, td.Name)

	// Free old B+Tree pages if replacing an existing table.
	if existingEntry != nil {
		oldBT := NewBTree(pb.pager, existingEntry.RootPageID)
		oldBT.FreeAllPages()
		for _, index := range existingEntry.Indexes {
			if index.RootPageID != InvalidPageID {
				NewBTree(pb.pager, index.RootPageID).FreeAllPages()
			}
		}
	}

	// Create a new B+Tree for the table.
	bt, err := CreateBTree(pb.pager, txID)
	if err != nil {
		_ = pb.pager.AbortTx(txID)
		return err
	}

	// Insert all rows using compact binary encoding.
	var encBuf []byte
	for i, row := range td.Rows {
		key := RowKey(int64(i))
		encBuf = MarshalRow(row, encBuf)
		if err := bt.Insert(txID, key, encBuf); err != nil {
			_ = pb.pager.AbortTx(txID)
			return fmt.Errorf("insert row %d: %w", i, err)
		}
	}

	indexes := make([]IndexInfo, 0, len(td.Indexes))
	for _, index := range td.Indexes {
		indexTree, createErr := CreateBTree(pb.pager, txID)
		if createErr != nil {
			_ = pb.pager.AbortTx(txID)
			return fmt.Errorf("create index %s: %w", index.Name, createErr)
		}
		for _, entry := range index.Entries {
			if len(entry.Key) == 0 || len(entry.RowIDs) == 0 {
				_ = pb.pager.AbortTx(txID)
				return fmt.Errorf("index %s has invalid entry", index.Name)
			}
			if err := indexTree.Insert(txID, entry.Key, marshalRowIDs(entry.RowIDs)); err != nil {
				_ = pb.pager.AbortTx(txID)
				return fmt.Errorf("insert index %s: %w", index.Name, err)
			}
		}
		index.RootPageID = indexTree.Root()
		index.Entries = nil
		indexes = append(indexes, index)
	}

	// Update catalog.
	version := td.Version
	if existingEntry != nil {
		version = existingEntry.Version + 1
	}
	catEntry := CatalogEntry{
		Tenant:     tenant,
		Table:      td.Name,
		RootPageID: bt.Root(),
		Columns:    columnsToCatalog(td.Columns),
		Indexes:    indexes,
		RowCount:   int64(len(td.Rows)),
		Version:    version,
	}
	if err := pb.catalog.PutEntry(txID, catEntry); err != nil {
		_ = pb.pager.AbortTx(txID)
		return err
	}
	// Update superblock catalog root in case it changed.
	pb.pager.UpdateSuperblock(func(sb *Superblock) {
		sb.CatalogRoot = pb.catalog.Root()
	})

	if err := pb.pager.CommitTx(txID); err != nil {
		return err
	}

	return nil
}

// LookupIndexRows follows an immutable secondary-index B+Tree and loads only
// referenced row records from the table B+Tree. It intentionally bypasses
// LoadTable, which materializes all rows for compatibility callers.
func (pb *PageBackend) LookupIndexRows(tenant, table, indexName string, key []byte) ([][]any, bool, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	entry, err := pb.catalog.GetEntry(tenant, table)
	if err != nil || entry == nil {
		return nil, false, err
	}
	var index *IndexInfo
	for i := range entry.Indexes {
		if strings.EqualFold(entry.Indexes[i].Name, indexName) {
			index = &entry.Indexes[i]
			break
		}
	}
	if index == nil || index.RootPageID == InvalidPageID {
		return nil, false, nil
	}
	return pb.lookupIndexRowsLocked(entry.RootPageID, index.RootPageID, indexName, key)
}

// LookupIndexRowsByRoot is the locator form of LookupIndexRows. A caller that
// has already validated an immutable catalog entry can bypass the catalog
// B+Tree and JSON decode on every point lookup. Roots are only accepted from
// the same open PageBackend; they are not a portable public file-format API.
func (pb *PageBackend) LookupIndexRowsByRoot(tableRoot, indexRoot PageID, indexName string, key []byte) ([][]any, bool, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	if tableRoot == InvalidPageID || indexRoot == InvalidPageID {
		return nil, false, nil
	}
	return pb.lookupIndexRowsLocked(tableRoot, indexRoot, indexName, key)
}

func (pb *PageBackend) lookupIndexRowsLocked(tableRoot, indexRoot PageID, indexName string, key []byte) ([][]any, bool, error) {
	var rowIDs []int64
	found, err := NewBTree(pb.pager, indexRoot).GetValue(key, func(value []byte) error {
		var decodeErr error
		rowIDs, decodeErr = unmarshalRowIDs(value)
		return decodeErr
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		// The physical index exists and completed the negative lookup. Return
		// a handled empty result so callers never fall back to a table scan.
		return [][]any{}, true, nil
	}
	rows := make([][]any, 0, len(rowIDs))
	rowsTree := NewBTree(pb.pager, tableRoot)
	for _, rowID := range rowIDs {
		var row []any
		found, err := rowsTree.GetValue(RowKey(rowID), func(encoded []byte) error {
			var decodeErr error
			row, decodeErr = UnmarshalRow(encoded)
			return decodeErr
		})
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, fmt.Errorf("index %s refers to missing row %d", indexName, rowID)
		}
		rows = append(rows, row)
	}
	return rows, true, nil
}

// TableEntry returns table and index metadata without scanning row pages.
func (pb *PageBackend) TableEntry(tenant, table string) (*CatalogEntry, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	return pb.catalog.GetEntry(tenant, table)
}

func marshalRowIDs(ids []int) []byte {
	buf := make([]byte, 4+8*len(ids))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(ids)))
	for i, id := range ids {
		binary.BigEndian.PutUint64(buf[4+i*8:], uint64(id))
	}
	return buf
}

func unmarshalRowIDs(data []byte) ([]int64, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("short row locator")
	}
	n := int(binary.BigEndian.Uint32(data[:4]))
	if n < 1 || n > (len(data)-4)/8 || len(data) != 4+n*8 {
		return nil, fmt.Errorf("invalid row locator length")
	}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(binary.BigEndian.Uint64(data[4+i*8:]))
	}
	return ids, nil
}

func cloneIndexInfos(in []IndexInfo) []IndexInfo {
	out := make([]IndexInfo, len(in))
	for i, index := range in {
		out[i] = index
		out[i].Columns = append([]string(nil), index.Columns...)
		out[i].Entries = nil
	}
	return out
}

// DeleteTable removes a table from the catalog and frees its pages.
func (pb *PageBackend) DeleteTable(tenant, name string) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Look up the table first so we can free its B+Tree pages.
	entry, _ := pb.catalog.GetEntry(tenant, name)

	txID, err := pb.pager.BeginTx()
	if err != nil {
		return err
	}

	// Free the table's B+Tree (all internal, leaf, and overflow pages).
	if entry != nil {
		bt := NewBTree(pb.pager, entry.RootPageID)
		bt.FreeAllPages()
	}

	if err := pb.catalog.DeleteEntry(txID, tenant, name); err != nil {
		_ = pb.pager.AbortTx(txID)
		return err
	}
	return pb.pager.CommitTx(txID)
}

// ListTableNames returns all table names for a tenant.
func (pb *PageBackend) ListTableNames(tenant string) ([]string, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	return pb.catalog.ListTables(tenant)
}

// TableExists reports whether a table exists in the catalog.
func (pb *PageBackend) TableExists(tenant, name string) bool {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	entry, _ := pb.catalog.GetEntry(tenant, name)
	return entry != nil
}

// Sync performs a checkpoint.
func (pb *PageBackend) Sync() error {
	pb.syncCount.Add(1)
	return pb.pager.Checkpoint()
}

// Close performs a final checkpoint and closes all files.
func (pb *PageBackend) Close() error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.closed {
		return nil
	}
	pb.closed = true
	return pb.pager.Close()
}

// Pager returns the underlying pager (for inspection tools).
func (pb *PageBackend) Pager() *Pager { return pb.pager }

// Stats returns operational statistics.
func (pb *PageBackend) Stats() PageBackendStats {
	sb := pb.pager.Superblock()
	cache := pb.pager.CacheStats()
	return PageBackendStats{
		PageSize:        int(sb.PageSize),
		PageCount:       sb.PageCount,
		FreePages:       pb.pager.freeMgr.Count(),
		CheckpointLSN:   sb.CheckpointLSN,
		NextTxID:        sb.NextTxID,
		SyncCount:       pb.syncCount.Load(),
		LoadCount:       pb.loadCount.Load(),
		PageReads:       cache.PageReads,
		CacheHits:       cache.CacheHits,
		CacheMisses:     cache.CacheMisses,
		CachedPages:     cache.CachedPages,
		PinnedPages:     cache.PinnedPages,
		TransientPages:  cache.TransientPages,
		TransientFrames: cache.TransientFrames,
		MaxCachePages:   cache.MaxPages,
		DBPath:          pb.config.Path,
		WALPath:         pb.config.Path + ".wal",
	}
}

// PageBackendStats holds operational metrics.
type PageBackendStats struct {
	PageSize        int
	PageCount       uint64
	FreePages       int
	CheckpointLSN   LSN
	NextTxID        TxID
	SyncCount       int64
	LoadCount       int64
	PageReads       int64
	CacheHits       int64
	CacheMisses     int64
	CachedPages     int
	PinnedPages     int
	TransientPages  int
	TransientFrames int
	MaxCachePages   int
	DBPath          string
	WALPath         string
}

// DBPath returns the database file path.
func (pb *PageBackend) DBPath() string {
	return filepath.Clean(pb.config.Path)
}

// MaxCachePages reports the strict page-cache capacity used by this backend.
func (pb *PageBackend) MaxCachePages() int {
	if pb.config.MaxCachePages > 0 {
		return pb.config.MaxCachePages
	}
	return 1024
}
