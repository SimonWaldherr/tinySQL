package storage

import "sync"

// Lazily shared transaction snapshots.
//
// A SQL transaction runs against a private shadow of the database. Building
// that shadow by copying every table made BEGIN cost O(rows in the whole
// database): a transaction that read one row of a 100000-row table paid for
// copying the row headers and the constraint-value index of that table (and of
// every other table) before it could run, and then threw the copy away.
//
// SnapshotForTxShared builds the shadow without copying anything. It maps every
// table of the shadow to the *live* *Table instance and pins that instance in
// the live database, using the copy-on-write protocol direct result streams
// already rely on (see PinTableForStream and DetachPinnedTableForWrite):
//
//   - A live writer that is about to modify a pinned table first swaps in a
//     private copy for itself, leaving the pinned instance byte-for-byte what it
//     was at BEGIN. The transaction therefore keeps reading its BEGIN-time
//     snapshot no matter what other connections commit afterwards.
//   - The shadow, on its own first write to a shared table, replaces its entry
//     with a private copy (cloneTableForTx, the same copy BEGIN used to make
//     eagerly) and drops the pin. A table the transaction never writes is never
//     copied.
//
// A table that is still shared when the transaction commits is by construction
// unchanged, so CollectWALChanges skips it; the tables the transaction did write
// are private copies that ApplyWALChanges installs into the live database.
//
// Backends whose tables are owned by an evictable pool cannot be pinned, so
// those databases keep using the eager SnapshotForTx / SnapshotForReadTx.

// canShareTablesWithTx reports whether tx shadows of db may share its tables.
func (db *DB) canShareTablesWithTx() bool {
	return db != nil && !db.backendTablesEvictable()
}

// SnapshotForTxShared is SnapshotForTx without the up-front table copy. The
// returned release function unpins the tables the shadow still shares; the
// caller must run it exactly when the transaction ends (commit or rollback),
// and may run it more than once. As for SnapshotForTx, the caller must hold
// LockContentForRead while taking the snapshot.
func (db *DB) SnapshotForTxShared() (base, shadow *DB, release func()) {
	if !db.canShareTablesWithTx() {
		base, shadow = db.SnapshotForTx()
		return base, shadow, func() {}
	}
	base = newShadowDB()
	shadow = newShadowDB()
	// The shadow needs the full schema and a private catalog; the base is only
	// read for table identity and Version. See SnapshotForTx.
	db.copyRuntimeState(shadow, true)
	markShadow(shadow)
	markShadow(base)
	return base, shadow, db.shareTablesInto(shadow, base)
}

// SnapshotForReadTxShared is SnapshotForReadTx without the up-front table copy.
// See SnapshotForTxShared for the release contract.
func (db *DB) SnapshotForReadTxShared() (shadow *DB, release func()) {
	if !db.canShareTablesWithTx() {
		return db.SnapshotForReadTx(), func() {}
	}
	shadow = newShadowDB()
	db.copyRuntimeState(shadow, true)
	markShadow(shadow)
	return shadow, db.shareTablesInto(shadow, nil)
}

// shareTablesInto maps every table of db into shadow (and, when base is not
// nil, records its identity and Version in base), pinning each table in db. It
// returns the idempotent function that undoes the pins still held.
func (db *DB) shareTablesInto(shadow, base *DB) (release func()) {
	// shadow and base are not visible to any other goroutine yet, so they are
	// filled without their own locks; only db's pin table is shared.
	db.mu.RLock()
	count := 0
	for _, tdb := range db.tenants {
		count += len(tdb.tables)
	}
	shadow.txShared = make(map[*Table]*DB, count)
	shadow.txBase = base
	db.streamSnapshotMu.Lock()
	if db.streamSnapshots == nil {
		db.streamSnapshots = make(map[*Table]uint32, count)
	}
	for tn, tdb := range db.tenants {
		// The source's tenant and table keys are already canonical, and its
		// table count is known, so the copies are sized once and filled without
		// re-lowering any name.
		shadowTables := make(map[string]*Table, len(tdb.tables))
		shadow.tenants[tn] = &tenantDB{tables: shadowTables}
		var baseTables map[string]*Table
		if base != nil {
			baseTables = make(map[string]*Table, len(tdb.tables))
			base.tenants[tn] = &tenantDB{tables: baseTables}
		}
		for key, t := range tdb.tables {
			if base != nil {
				// A pinned table cannot change, so while the shadow shares
				// it the live table is its own record of the Version it had
				// at BEGIN. recordBaseVersion swaps in a stub when the
				// shadow gives up the pin.
				baseTables[key] = t
			}
			shadowTables[key] = t
			shadow.txShared[t] = db
			db.streamSnapshots[t]++
		}
	}
	db.streamSnapshotMu.Unlock()
	db.mu.RUnlock()

	var once sync.Once
	return func() { once.Do(shadow.releaseSharedTables) }
}

// recordBaseVersion freezes, in this shadow's conflict-detection base, the
// Version of a shared table the shadow is about to stop pinning. Until then the
// base points at the live table itself; once the pin is gone that table may be
// changed by other writers, so the base keeps a stub instead. The caller still
// holds the pin.
func (db *DB) recordBaseVersion(tenant, key string, t *Table) {
	if db.txBase == nil {
		return
	}
	if td := db.txBase.getTenantRO(tenant); td != nil && td.tables[key] == t {
		td.tables[key] = versionStub(t)
	}
}

// versionStub is the conflict-detection base's record of a table: its name and
// Version, nothing else. The base is only ever asked whether a table existed at
// BEGIN and what Version it had (CollectWALChanges, the driver's conflict check),
// so building the schema maps and index tables cloneTableMeta makes for every
// table would only be paid for on each BEGIN and never read. It must not be
// handed to anything that executes against the table.
func versionStub(t *Table) *Table {
	return &Table{
		Name:          t.Name,
		Cols:          t.Cols,
		IsTemp:        t.IsTemp,
		Version:       t.Version,
		structVersion: t.structVersion,
		rowUpdateBase: t.rowUpdateBase,
		dirtyFrom:     -1,
	}
}

// releaseSharedTables drops every pin this shadow still holds on its source.
func (db *DB) releaseSharedTables() {
	db.streamSnapshotMu.Lock()
	remaining := db.txShared
	db.txShared = nil
	private := db.txPrivate
	db.txPrivate = nil
	db.streamSnapshotMu.Unlock()
	// Copies the transaction made but did not commit hand their unused twins
	// back; a committed copy has no origin left and is skipped.
	for _, t := range private {
		db.returnTwins(t)
	}
	for t, src := range remaining {
		src.unpinTxTable(t)
	}
}

// unpinTxTable removes one pin that a transaction shadow held on t.
func (db *DB) unpinTxTable(t *Table) {
	db.streamSnapshotMu.Lock()
	db.unpinLocked(t)
	db.streamSnapshotMu.Unlock()
}

// unpinLocked drops one reference to t. The caller holds streamSnapshotMu.
func (db *DB) unpinLocked(t *Table) {
	if refs := db.streamSnapshots[t]; refs <= 1 {
		delete(db.streamSnapshots, t)
	} else {
		db.streamSnapshots[t] = refs - 1
	}
}

// pinnedLocked reports whether t must not be written in place: a stream reads
// it, or it is a table this transaction shadow still shares with the database
// it was taken from. The caller holds streamSnapshotMu.
func (db *DB) pinnedLocked(t *Table) bool {
	if db.streamSnapshots[t] != 0 {
		return true
	}
	_, shared := db.txShared[t]
	return shared
}

// isTxShared reports whether t is still shared with the database this
// transaction shadow was taken from, which means the transaction has not
// written it.
func (db *DB) isTxShared(t *Table) bool {
	if db == nil {
		return false
	}
	db.streamSnapshotMu.Lock()
	_, shared := db.txShared[t]
	db.streamSnapshotMu.Unlock()
	return shared
}

// takeTxShared reports whether t is a table this shadow still shares with its
// source and, if so, removes it from the shared set. It returns the source
// database, whose pin the caller must release
// after installing a private copy.
func (db *DB) takeTxShared(t *Table) (*DB, bool) {
	db.streamSnapshotMu.Lock()
	defer db.streamSnapshotMu.Unlock()
	src, shared := db.txShared[t]
	if !shared {
		return nil, false
	}
	delete(db.txShared, t)
	return src, true
}

// privateCopyOfShared returns the table a transaction shadow writes to in place
// of the shared table t. schemaWrite selects the deep copy ALTER TABLE needs;
// every other writer replaces whole row slices and can share scalar rows.
//
// The source's content read lock is held while copying: the source database
// owns t and may update bookkeeping on it that a copy would otherwise read
// concurrently.
func privateCopyOfShared(src *DB, t *Table, schemaWrite bool) *Table {
	src.LockContentForRead()
	defer src.UnlockContentForRead()
	if schemaWrite {
		return cloneTable(t)
	}
	return cloneTableForSharedTx(t)
}

// DetachAllPinnedForWrite makes every pinned table of db private to a writer,
// for statements whose effect on existing tables is not known to be confined to
// one named table. It is the conservative companion to DetachPinnedTableForWrite
// and must be called under the same conditions.
func (db *DB) DetachAllPinnedForWrite() {
	if db == nil || db.backendTablesEvictable() {
		return
	}
	type ref struct{ tenant, name string }
	var pinned []ref
	db.mu.RLock()
	db.streamSnapshotMu.Lock()
	if len(db.streamSnapshots) > 0 || len(db.txShared) > 0 {
		for tn, tdb := range db.tenants {
			for _, t := range tdb.tables {
				if db.pinnedLocked(t) {
					pinned = append(pinned, ref{tn, t.Name})
				}
			}
		}
	}
	db.streamSnapshotMu.Unlock()
	db.mu.RUnlock()
	for _, r := range pinned {
		db.DetachPinnedTableForSchemaWrite(r.tenant, r.name)
	}
}
