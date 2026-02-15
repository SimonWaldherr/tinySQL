package pager

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"sort"
	"sync"
)

// ───────────────────────────────────────────────────────────────────────────
// System catalog — maps tenant/table names to B+Tree root pages
// ───────────────────────────────────────────────────────────────────────────
//
// The catalog is itself a B+Tree whose
//   key   = "tenant\x00tablename"
//   value = JSON-encoded CatalogEntry
//
// The catalog root page ID is stored in the superblock (CatalogRoot).

// CatalogEntry is the value stored in the system catalog B+Tree.
type CatalogEntry struct {
	Tenant     string          `json:"tenant"`
	Table      string          `json:"table"`
	RootPageID PageID          `json:"root_page_id"`
	Columns    []CatalogColumn `json:"columns"`
	RowCount   int64           `json:"row_count"`
	Version    int             `json:"version"`
}

// CatalogColumn describes a column in the system catalog.
type CatalogColumn struct {
	Name       string `json:"name"`
	Type       int    `json:"type"`       // ColType as int
	Constraint int    `json:"constraint"` // ConstraintType as int
	FKTable    string `json:"fk_table,omitempty"`
	FKColumn   string `json:"fk_col,omitempty"`
	PtrTable   string `json:"ptr_table,omitempty"`
}

// catalogKey constructs the catalog lookup key.
func catalogKey(tenant, table string) []byte {
	return []byte(tenant + "\x00" + table)
}

// Catalog manages the system catalog B+Tree.
type Catalog struct {
	mu    sync.RWMutex
	pager *Pager
	tree  *BTree
}

// OpenCatalog opens or creates the system catalog.
func OpenCatalog(p *Pager, txID TxID) (*Catalog, error) {
	sb := p.Superblock()
	cat := &Catalog{pager: p}

	if sb.CatalogRoot == InvalidPageID {
		// Brand new database — create catalog tree.
		bt, err := CreateBTree(p, txID)
		if err != nil {
			return nil, fmt.Errorf("create catalog tree: %w", err)
		}
		cat.tree = bt
		p.UpdateSuperblock(func(s *Superblock) {
			s.CatalogRoot = bt.Root()
		})
	} else {
		cat.tree = NewBTree(p, sb.CatalogRoot)
	}
	return cat, nil
}

// PutEntry upserts a catalog entry within the given transaction.
func (c *Catalog) PutEntry(txID TxID, entry CatalogEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := catalogKey(entry.Tenant, entry.Table)
	val, err := storage.JSONMarshal(entry)
	if err != nil {
		return err
	}
	return c.tree.Insert(txID, key, val)
}

// GetEntry retrieves a catalog entry. Returns nil if not found.
func (c *Catalog) GetEntry(tenant, table string) (*CatalogEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, found, err := c.tree.Get(catalogKey(tenant, table))
	if err != nil || !found {
		return nil, err
	}
	var entry CatalogEntry
	if err := json.Unmarshal(val, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// DeleteEntry removes a catalog entry within the given transaction.
func (c *Catalog) DeleteEntry(txID TxID, tenant, table string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.tree.Delete(txID, catalogKey(tenant, table))
	return err
}

// ListTables returns all table names for a tenant.
func (c *Catalog) ListTables(tenant string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	prefix := []byte(tenant + "\x00")
	var names []string
	err := c.tree.ScanRange(prefix, nil, func(key, val []byte) bool {
		// Check prefix match.
		if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
			return false // past our tenant
		}
		tableName := string(key[len(prefix):])
		names = append(names, tableName)
		return true
	})
	sort.Strings(names)
	return names, err
}

// Root returns the catalog tree's root page ID.
func (c *Catalog) Root() PageID { return c.tree.Root() }

// ───────────────────────────────────────────────────────────────────────────
// Table serialization to/from B+Tree
// ───────────────────────────────────────────────────────────────────────────
//
// Row data for each table is stored in its own B+Tree:
//   key   = 8-byte big-endian row ID (uint64)
//   value = JSON-encoded row ([]any)
//
// This is simple and correct for V1. Future versions can use a binary
// row format for better performance.

// RowKey creates a B+Tree key from a row index.
func RowKey(rowID int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(rowID))
	return buf[:]
}

// ParseRowKey extracts the row ID from a B+Tree key.
func ParseRowKey(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key))
}
