// Finding, adding and removing tables within a tenant, including the
// did-you-mean suggestion an unknown table name produces and the lazy load a
// disk-backed mode performs on a cache miss.
package storage

import (
	"fmt"
	"sort"
	"strings"
)

// getTenant returns the tenantDB for the given tenant name, creating it
// if necessary. Callers must hold db.mu (at least read-locked when only
// reading, write-locked when creating/modifying).
func (db *DB) getTenant(tn string) *tenantDB {
	tn = strings.ToLower(tn)
	td := db.tenants[tn]
	if td == nil {
		td = &tenantDB{tables: map[string]*Table{}}
		db.tenants[tn] = td
	}
	return td
}

// getTenantRO returns the tenantDB for reading. Returns nil if it does not
// exist (no allocation). Caller must hold db.mu.RLock().
func (db *DB) getTenantRO(tn string) *tenantDB {
	return db.tenants[strings.ToLower(tn)]
}

// Get returns a table by name for the given tenant.
// When a StorageBackend is attached, tables not found in memory are loaded
// from the backend on demand (lazy loading).
func (db *DB) Get(tn, name string) (*Table, error) {
	t, found := func() (*Table, bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		td := db.getTenantRO(tn)
		if td == nil {
			return nil, false
		}
		t, ok := td.tables[strings.ToLower(name)]
		return t, ok
	}()
	if found {
		return t, nil
	}

	// Not in memory – try the backend.
	if db.backend != nil {
		t, err := db.backend.LoadTable(tn, name)
		if err != nil {
			return nil, fmt.Errorf("backend load %s/%s: %w", tn, name, err)
		}
		if t != nil {
			// ModeIndex, ModeHybrid, and ModePagedIndex own loaded tables
			// through their own bounded pool rather than DB.tenants.
			// Retaining another pointer in DB.tenants would turn a cache
			// eviction into a no-op and make memory grow with every table
			// ever queried. This does not put mutations at risk: the
			// returned *Table is the very same pointer the backend's pool
			// holds, so an in-place INSERT/UPDATE/DELETE stays visible to
			// that pool. DB.Sync and DB.Close additionally consult the
			// backend's PooledTables (when it implements pooledTableSource)
			// to find and flush exactly these leases, and the pool itself
			// flushes a dirty table before dropping it under memory
			// pressure (see BufferPool.evictionSaver). So the caller's
			// reference is a query-scoped lease for memory-retention
			// purposes only — it remains valid while that statement holds
			// DB.contentMu and becomes collectible once both caller and
			// pool release it — never for durability.
			if !db.backendTablesEvictable() {
				db.mu.Lock()
				db.getTenant(tn).tables[strings.ToLower(t.Name)] = t
				db.mu.Unlock()
			}
			return t, nil
		}
	}

	return nil, db.noSuchTableError(tn, name)
}

// noSuchTableError builds the "no such table" error for Get, adding a
// "did you mean ...?" hint when an existing table name is a close typo
// match. This is a plain edit-distance heuristic (see suggestSimilar), not
// an AI feature — it only fires on the already-slow not-found path.
func (db *DB) noSuchTableError(tn, name string) error {
	if suggestion := suggestSimilar(name, db.candidateTableNames(tn)); suggestion != "" {
		return fmt.Errorf("no such table %q (tenant %q) - did you mean %q?", name, tn, suggestion)
	}
	return fmt.Errorf("no such table %q (tenant %q)", name, tn)
}

// candidateTableNames lists table names known for the tenant, both resident
// in memory and (if a backend is attached) on disk, for typo suggestions.
func (db *DB) candidateTableNames(tn string) []string {
	db.mu.RLock()
	td := db.getTenantRO(tn)
	var names []string
	if td != nil {
		names = make([]string, 0, len(td.tables))
		for _, t := range td.tables {
			names = append(names, t.Name)
		}
	}
	db.mu.RUnlock()
	if db.backend != nil {
		if diskNames, err := db.backend.ListTableNames(tn); err == nil {
			names = append(names, diskNames...)
		}
	}
	return names
}

// backendTablesEvictable reports modes whose backend, rather than DB.tenants,
// is the owner of lazily loaded tables. Keeping this policy explicit is
// important: schemas and manifest metadata stay resident, row payloads do
// not. Mutable tables created in the current process remain in the catalog
// until they are explicitly Evict'ed or the DB is reopened.
func (db *DB) backendTablesEvictable() bool {
	return db.backend != nil && (db.storageMode == ModeIndex || db.storageMode == ModeHybrid || db.storageMode == ModePagedIndex)
}

// Put adds a new table to the tenant; returns error if it already exists.
// When a StorageBackend is attached, the table is also checked against the
// backend to prevent duplicates, and optionally persisted immediately when
// SyncOnMutate is configured.
func (db *DB) Put(tn string, t *Table) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	exists := func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		td := db.getTenant(tn)
		lc := strings.ToLower(t.Name)
		if _, exists := td.tables[lc]; exists {
			return true
		}
		// Also check the backend for tables that may be on disk but not loaded.
		if db.backend != nil && db.backend.TableExists(tn, t.Name) {
			return true
		}
		td.tables[lc] = t
		return false
	}()
	if exists {
		return fmt.Errorf("table %q already exists (tenant %q)", t.Name, tn)
	}

	// Persist to backend if configured.
	if db.backend != nil {
		if err := db.backend.SaveTable(tn, t); err != nil {
			return fmt.Errorf("backend save %s/%s: %w", tn, t.Name, err)
		}
	}
	return nil
}

// Drop removes a table from the tenant (and from the backend if attached).
func (db *DB) Drop(tn, name string) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	onDisk, found := func() (bool, bool) {
		db.mu.Lock()
		defer db.mu.Unlock()
		td := db.getTenant(tn)
		lc := strings.ToLower(name)
		_, inMemory := td.tables[lc]
		onDisk := db.backend != nil && db.backend.TableExists(tn, name)
		if !inMemory && !onDisk {
			return false, false
		}
		delete(td.tables, lc)
		return onDisk, true
	}()
	if !found {
		return db.noSuchTableError(tn, name)
	}

	if db.backend != nil && onDisk {
		if err := db.backend.DeleteTable(tn, name); err != nil {
			return fmt.Errorf("backend delete %s/%s: %w", tn, name, err)
		}
	}
	return nil
}

// ListTables returns the tables in a tenant sorted by name.
// When a StorageBackend is attached, tables that exist on disk but are not
// currently loaded into memory are loaded on demand.
func (db *DB) ListTables(tn string) []*Table {
	// In the evictable modes the tenant catalog deliberately does not own
	// backend-loaded row data. ListTables is an explicit all-table operation,
	// so return transient table leases rather than repopulating that catalog.
	if db.backendTablesEvictable() {
		names, err := db.backend.ListTableNames(tn)
		if err != nil {
			return nil
		}
		out := make([]*Table, 0, len(names))
		for _, name := range names {
			if t, err := db.Get(tn, name); err == nil && t != nil {
				out = append(out, t)
			}
		}
		return out
	}

	// If a backend is attached, ensure we know about all tables on disk.
	if db.backend != nil {
		if diskNames, err := db.backend.ListTableNames(tn); err == nil {
			for _, n := range diskNames {
				lc := strings.ToLower(n)
				db.mu.RLock()
				td := db.getTenantRO(tn)
				inMem := td != nil && td.tables[lc] != nil
				db.mu.RUnlock()
				if !inMem {
					// Load from backend
					if t, err := db.backend.LoadTable(tn, n); err == nil && t != nil {
						if !db.backendTablesEvictable() {
							db.mu.Lock()
							db.getTenant(tn).tables[lc] = t
							db.mu.Unlock()
						}
					}
				}
			}
		}
	}

	db.mu.RLock()
	td := db.getTenantRO(tn)
	if td == nil || len(td.tables) == 0 {
		db.mu.RUnlock()
		return nil
	}
	names := make([]string, 0, len(td.tables))
	for k := range td.tables {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*Table, len(names))
	for i, n := range names {
		out[i] = td.tables[n]
	}
	db.mu.RUnlock()
	return out
}
