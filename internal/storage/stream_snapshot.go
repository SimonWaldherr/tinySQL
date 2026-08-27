package storage

import (
	"strings"
	"sync"
)

// PinTableForStream pins table as the immutable source of one direct result
// stream. The caller must already hold contentMu's read side while it obtains
// and pins the table, so a writer cannot slip between planning and the pin.
//
// The returned release function is idempotent. It must run when the producer
// no longer reads the table. A pin is intentionally unavailable for evictable
// backends: their query-scoped table leases live in backend-owned pools, which
// cannot be atomically swapped through DB.tenants. Paged read-only artifacts
// use their own cursor source instead of a Table pin.
func (db *DB) PinTableForStream(table *Table) (release func(), ok bool) {
	if db == nil || table == nil || db.backendTablesEvictable() {
		return nil, false
	}

	db.streamSnapshotMu.Lock()
	if db.streamSnapshots == nil {
		db.streamSnapshots = make(map[*Table]uint32)
	}
	db.streamSnapshots[table]++
	db.streamSnapshotMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.streamSnapshotMu.Lock()
			if refs := db.streamSnapshots[table]; refs <= 1 {
				delete(db.streamSnapshots, table)
			} else {
				db.streamSnapshots[table] = refs - 1
			}
			db.streamSnapshotMu.Unlock()
		})
	}, true
}

// DetachPinnedTableForWrite makes the current in-memory table mapping private
// to a writer when one or more streams still read its old instance. It must be
// called with contentMu's write side held and before statement planning or a
// rollback snapshot captures the table pointer. The caller can then freely
// mutate the table returned by Get while existing streams continue to read the
// old, immutable instance.
//
// A false result means no current table was pinned (or this DB uses an
// evictable backend), so no copy was needed.
func (db *DB) DetachPinnedTableForWrite(tenant, name string) bool {
	if db == nil || name == "" || db.backendTablesEvictable() {
		return false
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	tenantDB := db.getTenantRO(tenant)
	if tenantDB == nil {
		return false
	}
	key := canonicalTableName(name)
	table := tenantDB.tables[key]
	if table == nil {
		return false
	}

	db.streamSnapshotMu.Lock()
	pinned := db.streamSnapshots[table] != 0
	db.streamSnapshotMu.Unlock()
	if !pinned {
		return false
	}

	// DML only needs an independent row-header segment. Existing cells stay
	// shared with the immutable stream; writers replace changed row slices,
	// so they cannot mutate a streamed row through that sharing.
	tenantDB.tables[key] = cloneTableForStreamDML(table)
	return true
}

// DetachPinnedTableForSchemaWrite is the schema-mutation companion to
// DetachPinnedTableForWrite. ALTER TABLE can edit existing row cells in place
// while widening or shrinking them, so it deliberately retains a deep clone.
func (db *DB) DetachPinnedTableForSchemaWrite(tenant, name string) bool {
	if db == nil || name == "" || db.backendTablesEvictable() {
		return false
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	tenantDB := db.getTenantRO(tenant)
	if tenantDB == nil {
		return false
	}
	key := canonicalTableName(name)
	table := tenantDB.tables[key]
	if table == nil {
		return false
	}
	db.streamSnapshotMu.Lock()
	pinned := db.streamSnapshots[table] != 0
	db.streamSnapshotMu.Unlock()
	if !pinned {
		return false
	}
	tenantDB.tables[key] = cloneTable(table)
	return true
}

// IsTablePinnedForStream reports whether table is still the source of a live
// stream. Statement rollback uses it to avoid restoring state in place into a
// table a stream is concurrently reading.
func (db *DB) IsTablePinnedForStream(table *Table) bool {
	if db == nil || table == nil {
		return false
	}
	db.streamSnapshotMu.Lock()
	pinned := db.streamSnapshots[table] != 0
	db.streamSnapshotMu.Unlock()
	return pinned
}

func canonicalTableName(name string) string {
	// Table map keys have used strings.ToLower since DB's first implementation.
	// Keep the normalization local to this small COW facility so callers cannot
	// accidentally depend on a new public naming rule.
	return strings.ToLower(name)
}
