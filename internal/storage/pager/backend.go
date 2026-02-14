// Package pager — PageBackend integrates the page-based storage engine
// with the tinySQL StorageBackend interface.
package pager

import (
	"fmt"
	"path/filepath"
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
	syncCount     atomic.Int64
	loadCount     atomic.Int64
	evictionCount atomic.Int64
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
	})
	if err != nil {
		return nil, fmt.Errorf("open page backend: %w", err)
	}

	// Open or create catalog within a transaction.
	txID, err := pager.BeginTx()
	if err != nil {
		pager.Close()
		return nil, err
	}
	cat, err := OpenCatalog(pager, txID)
	if err != nil {
		pager.Close()
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	// Update superblock with catalog root.
	pager.UpdateSuperblock(func(sb *Superblock) {
		sb.CatalogRoot = cat.Root()
	})
	if err := pager.CommitTx(txID); err != nil {
		pager.Close()
		return nil, err
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
	}

	// Create a new B+Tree for the table.
	bt, err := CreateBTree(pb.pager, txID)
	if err != nil {
		pb.pager.AbortTx(txID)
		return err
	}

	// Insert all rows using compact binary encoding.
	var encBuf []byte
	for i, row := range td.Rows {
		key := RowKey(int64(i))
		encBuf = MarshalRow(row, encBuf)
		// Copy because BTree may retain the buffer.
		val := make([]byte, len(encBuf))
		copy(val, encBuf)
		if err := bt.Insert(txID, key, val); err != nil {
			pb.pager.AbortTx(txID)
			return fmt.Errorf("insert row %d: %w", i, err)
		}
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
		RowCount:   int64(len(td.Rows)),
		Version:    version,
	}
	if err := pb.catalog.PutEntry(txID, catEntry); err != nil {
		pb.pager.AbortTx(txID)
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
		pb.pager.AbortTx(txID)
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
	return PageBackendStats{
		PageSize:      int(sb.PageSize),
		PageCount:     sb.PageCount,
		FreePages:     pb.pager.freeMgr.Count(),
		CheckpointLSN: sb.CheckpointLSN,
		NextTxID:      sb.NextTxID,
		SyncCount:     pb.syncCount.Load(),
		LoadCount:     pb.loadCount.Load(),
		DBPath:        pb.config.Path,
		WALPath:       pb.config.Path + ".wal",
	}
}

// PageBackendStats holds operational metrics.
type PageBackendStats struct {
	PageSize      int
	PageCount     uint64
	FreePages     int
	CheckpointLSN LSN
	NextTxID      TxID
	SyncCount     int64
	LoadCount     int64
	DBPath        string
	WALPath       string
}

// DBPath returns the database file path.
func (pb *PageBackend) DBPath() string {
	return filepath.Clean(pb.config.Path)
}
