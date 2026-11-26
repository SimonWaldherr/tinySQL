// DB.Sync without stopping the world: tables are written from detached
// snapshots instead of from the live objects under an exclusive lock.
//
// A table-file save used to run with the caller holding every writer (and, in
// the embedding application, every reader) out for the whole write: encoding a
// table of a million 768-dimensional vectors takes minutes. The snapshot path
// keeps the engine's content lock for the few milliseconds it takes to copy the
// table's row headers, schema and statistics and to lend its secondary
// indexes, then encodes and writes with no engine lock held.
//
// The copy is sufficient because of one invariant of this package: DML never
// writes through an existing row's cells. UPDATE replaces a row slice at its
// position, DELETE builds a new outer slice, INSERT appends; so the outer slice
// copied under the content lock describes the table at one instant forever,
// however many statements follow (the rows-only statement snapshots rely on the
// same fact, see SnapshotForRowsOnlyStatement).
package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// errSnapshotSuperseded is returned by a backend that did not install a
// snapshot because the table's file was replaced or removed after the
// snapshot's basis was taken (see DiskBackend.beginTableSnapshot). The caller
// retries from a fresh snapshot; nothing was written.
var errSnapshotSuperseded = errors.New("storage: table snapshot superseded")

// snapshotToken identifies the persisted state a snapshot builds on. The
// backend fills it in before the snapshot is taken and checks it again when
// the snapshot is installed.
type snapshotToken struct {
	// gen is DiskBackend's write generation of the table.
	gen uint64
	// delGen is DiskBackend's drop count of the table.
	delGen uint64
	// dirtySeq is HybridBackend's MarkDirty sequence of the table.
	dirtySeq uint64
}

// tableSnapshot is a detached, immutable image of one table, taken under the
// engine's content read lock and written without it.
type tableSnapshot struct {
	tenant string
	// table holds copies of the live table's row headers, columns, constraints,
	// statistics and versions, and its secondary indexes lent read-only. Nothing
	// reachable from it is modified after takeTableSnapshot returns.
	table *Table
	// live is the table the snapshot was taken from, for the backend that
	// mirrors the live object into a cache (HybridBackend).
	live  *Table
	token snapshotToken
	// ftsGeneration and vectorGeneration are the search-index generations the
	// attached images were copied at; they are marked persisted once written.
	ftsGeneration    int
	vectorGeneration int
	// lists are the skip lists this snapshot shares with live indexes; see
	// release.
	lists []*SkipList
	// imageless marks a snapshot written without search-index images because the
	// table object was replaced after its predecessor's images were copied; the
	// table's images are flagged for the next Sync once it is written.
	imageless bool
}

// syncTableKey identifies a table by tenant and name, whichever object currently
// stands for it.
func syncTableKey(tenant, name string) string {
	return strings.ToLower(tenant) + "\x00" + strings.ToLower(name)
}

// flagSearchImagesForNextSync marks the table's existing FTS and ANN images as
// not persisted, so the next Sync copies and writes them. It takes the
// derived-state lock, so the caller must not hold the content lock: an index
// build holds that lock for its whole duration.
//
// It lowers the PERSISTED counters and never touches the generations. A
// statement that holds the content write lock reads the generations without the
// derived-state lock (SnapshotForRowsOnlyStatement and its rollback) on the
// premise that only a search run by that statement can move them; raising a
// generation from here, with no content lock held, would be a data race with
// that read and would make a rolled-back statement drop the table's images
// because a Sync, not a search, moved the counter. The persisted counters are
// only ever read and written under the derived-state lock. A table whose
// counters already differ is dirty anyway and is left alone. The next Sync sees
// generation != persisted, copies the images at that generation and
// MarkFTSIndexesPersisted(generation) accepts it (generation > persisted and
// generation <= current), so the flag clears exactly once.
func (t *Table) flagSearchImagesForNextSync() {
	t.DerivedLock()
	defer t.DerivedUnlock()
	if len(t.FTSIndexes) > 0 && t.ftsPersistedGeneration == t.ftsGeneration {
		t.ftsPersistedGeneration = t.ftsGeneration - 1
	}
	if len(t.VectorIndexes) > 0 && t.vectorPersistedGeneration == t.vectorGeneration {
		t.vectorPersistedGeneration = t.vectorGeneration - 1
	}
}

// snapshotSaver is implemented by backends that can write a tableSnapshot
// (DiskBackend, and HybridBackend through it). Backends without it (paged
// index, SQLite, memory) keep the older path: DB.Sync hands them the live table.
type snapshotSaver interface {
	// beginTableSnapshot returns the token to attach to a snapshot of the
	// named table. DB.Sync calls it under the content read lock, immediately
	// before taking the snapshot.
	beginTableSnapshot(tenant, name string) snapshotToken
	// saveTableSnapshot persists the snapshot atomically, or returns
	// errSnapshotSuperseded having changed nothing.
	saveTableSnapshot(s *tableSnapshot) error
}

// searchImages are deep copies of a table's persisted FTS and ANN images and
// the generations they were copied at, taken under one derived-state lock hold.
type searchImages struct {
	fts              map[string]*FTSIndex
	vec              map[string]*VectorIndex
	ftsGeneration    int
	vectorGeneration int
}

// copySearchImages deep-copies the table's FTS and ANN images. The images
// describe the rows at the (Version, StructVersion, BuiltRows) recorded inside
// them and are validated against the table when they are used after a reload
// (an image behind the table is extended or rebuilt, one ahead of it is
// rejected), so they need not be copied at the same instant as the rows: the
// caller copies them first and the rows afterwards, which can only leave an
// image older than the rows, never newer. The copy takes only the table's
// derived-state lock, not the engine's content lock.
func (t *Table) copySearchImages() searchImages {
	t.DerivedLock()
	defer t.DerivedUnlock()
	return searchImages{
		fts:              cloneFTSIndexes(t.FTSIndexes),
		vec:              cloneVectorIndexes(t.VectorIndexes),
		ftsGeneration:    t.ftsGeneration,
		vectorGeneration: t.vectorGeneration,
	}
}

// takeTableSnapshot copies live's row headers, schema, statistics and versions
// and lends its secondary indexes. The caller holds the content read lock, so
// no statement runs against the table meanwhile. The work is proportional to
// the row count only through one memmove of the row headers (24 bytes per row).
func takeTableSnapshot(tenant string, live *Table, token snapshotToken) *tableSnapshot {
	snap := NewTable(live.Name, cloneColumnsDeep(live.Cols), live.IsTemp)
	snap.CheckConstraints = CloneCheckConstraints(live.CheckConstraints)
	snap.Version = live.Version
	snap.structVersion = live.structVersion
	snap.Stats = cloneTableStats(live.Stats)
	rows := make([][]any, len(live.Rows))
	copy(rows, live.Rows)
	snap.Rows = rows
	indexes, lists := lendSecondaryIndexes(live.Indexes)
	snap.Indexes = indexes
	return &tableSnapshot{tenant: tenant, table: snap, live: live, token: token, lists: lists}
}

// cloneColumnsDeep copies a column list including everything reachable from a
// column through a pointer: ALTER TABLE ... RENAME COLUMN rewrites a
// ForeignKeyRef in place (under the content write lock) on the child tables
// that reference the renamed column, and the unlocked encoder of a snapshot
// would otherwise read that string header while it changes.
func cloneColumnsDeep(in []Column) []Column {
	out := make([]Column, len(in))
	copy(out, in)
	for i := range out {
		if fk := out[i].ForeignKey; fk != nil {
			cp := *fk
			out[i].ForeignKey = &cp
		}
		if out[i].DefaultValue != nil {
			out[i].DefaultValue = cloneCell(out[i].DefaultValue)
		}
	}
	return out
}

// attachSearchImages stores the copies on the snapshot's table, ready for
// snapshotFileImage, and remembers their generations.
func (s *tableSnapshot) attachSearchImages(images searchImages) {
	s.table.FTSIndexes = images.fts
	s.table.VectorIndexes = images.vec
	s.ftsGeneration = images.ftsGeneration
	s.vectorGeneration = images.vectorGeneration
}

// lendSecondaryIndexes returns read-only views of in for a snapshot without
// copying them. An index that never built its skip list only has Entries, which
// nothing modifies in place (a writer first hydrates a list, and materialize
// installs a new slice), so the view aliases them. An index with a skip list is
// lent the list the way borrowSecondaryIndexes lends it to a transaction copy:
// the list becomes shared and read-only, and whoever writes first, the live
// index or a later borrower, copies it. The shared lists are returned so
// release can give the snapshot's share back.
//
// The caller holds the content read lock; writers hold its write side, so none
// runs a hydrateForWrite against these indexes concurrently.
func lendSecondaryIndexes(in map[string]*SecondaryIndex) (map[string]*SecondaryIndex, []*SkipList) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]*SecondaryIndex, len(in))
	var lists []*SkipList
	for key, idx := range in {
		if idx == nil {
			continue
		}
		idx.mu.Lock()
		view := &SecondaryIndex{Name: idx.Name, Columns: append([]string(nil), idx.Columns...), Unique: idx.Unique}
		if idx.fast == nil {
			view.Entries = idx.Entries
		} else {
			idx.shared = true
			idx.fast.sharers.Add(1)
			view.fast = idx.fast
			view.shared = true
			lists = append(lists, idx.fast)
		}
		idx.mu.Unlock()
		out[key] = view
	}
	return out, lists
}

// release gives back the snapshot's share of every skip list it borrowed, so
// the live index does not copy a list nobody reads any more on its next write.
// It must be called after the snapshot has been written (or abandoned), under
// the content read lock: the decrement must not interleave with the live
// index's own check-then-decrement in hydrateForWrite, which only runs under
// the write lock.
func (s *tableSnapshot) release() {
	for _, list := range s.lists {
		for {
			n := list.sharers.Load()
			if n <= 0 {
				// The live index (or another borrower) already took its own
				// copy and consumed the count that stood for this share.
				break
			}
			if list.sharers.CompareAndSwap(n, n-1) {
				break
			}
		}
	}
	s.lists = nil
}

// snapshotFileImage builds the table metadata and passes the immutable rows
// directly to the file writer, which converts them as required by the format.
func snapshotFileImage(tn string, s *tableSnapshot) diskTable {
	t := s.table
	dt := diskTable{
		Tenant:           tn,
		Name:             t.Name,
		IsTemp:           t.IsTemp,
		CheckConstraints: t.CheckConstraints,
		Version:          t.Version,
		StructVersion:    t.structVersion,
		Cols:             make([]diskColumn, len(t.Cols)),
		Rows:             t.Rows,
		Indexes:          materializeSecondaryIndexesForEncode(t.Indexes),
		FTSIndexes:       t.FTSIndexes,
		VectorIndexes:    t.VectorIndexes,
		Stats:            t.Stats,
	}
	for i, c := range t.Cols {
		dt.Cols[i] = diskColumn(c)
	}
	return dt
}

// syncSnapshotHook, when set by a test, runs after DB.Sync released the content
// lock with its snapshots taken and before any of them is written: the window
// in which concurrent statements run during a real Sync.
var syncSnapshotHook atomic.Pointer[func()]

// syncImagesHook, when set by a test, runs after DB.Sync copied the search
// index images and before it takes its snapshots: the window in which rows
// change after an image was copied.
var syncImagesHook atomic.Pointer[func()]

// maxSnapshotAttempts bounds the retries of a snapshot that was superseded by
// another save of the same table (a SyncTable under the write lock, a pool
// eviction saving the table, a Replace). Every retry starts from a fresh
// snapshot, so a superseded one never installs older rows. There is no
// last-resort write of the live table under the content lock: that would stop
// the world for the length of the write. A Sync that keeps being superseded
// returns an error instead, with every table that was written intact and every
// other table still dirty, and the caller simply syncs again.
const maxSnapshotAttempts = 5

// syncEntry is one table DB.Sync decided to write.
type syncEntry struct {
	tenant           string
	table            *Table
	ftsGeneration    int
	vectorGeneration int
}

// collectDirtyTables lists the tables that need a write: those the backend's
// dirty tracker reports as changed, and, unless the backend keeps search
// images out of its files, those whose FTS or ANN images changed. "Resident" is
// two sets: db.tenants (every non-evictable mode, plus any table Put into this
// process) and, for the evictable modes ModeIndex/ModeHybrid/ModePagedIndex,
// whatever the backend's own bounded pool currently holds. DB.Get returns a
// query-scoped lease for the latter without ever registering it in db.tenants
// (see backendTablesEvictable), so skipping pooledTableSource here would
// silently drop any mutation made on such a lease: Sync/Close would keep
// returning nil while the table was never actually re-saved.
//
// With wait unset the caller holds the content lock and the search-index
// state is read without waiting for any table's derived-state lock (see
// searchIndexPersistenceStateNoWait); with it set the caller has the tables to
// itself (a backend without snapshot support, called under the caller's own
// exclusion) and the state is read exactly.
func (db *DB) collectDirtyTables(dc dirtyTracker, hasDirtyTracker, skipSearch, wait bool) []syncEntry {
	var out []syncEntry
	seen := make(map[string]bool) // tenant\x00lower(table name)
	state := func(t *Table) (int, bool, int, bool) {
		if wait {
			return t.searchIndexPersistenceState(skipSearch)
		}
		return t.searchIndexPersistenceStateNoWait(skipSearch)
	}

	db.mu.RLock()
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			seen[tn+"\x00"+strings.ToLower(t.Name)] = true
			ftsGeneration, ftsDirty, vectorGeneration, vectorDirty := state(t)
			if hasDirtyTracker && !dc.IsDirty(tn, t.Name, t.Version) && !ftsDirty && !vectorDirty {
				continue
			}
			out = append(out, syncEntry{tenant: tn, table: t, ftsGeneration: ftsGeneration, vectorGeneration: vectorGeneration})
		}
	}
	db.mu.RUnlock()

	// db.tenants entries take precedence: a table already covered above
	// (e.g. HybridBackend/PagedIndexBackend's SaveTable mirrors a Put into
	// their own pool too) must not be saved twice.
	if ps, ok := db.backend.(pooledTableSource); ok {
		for _, ref := range ps.PooledTables() {
			key := strings.ToLower(ref.Tenant) + "\x00" + strings.ToLower(ref.Table.Name)
			if seen[key] {
				continue
			}
			seen[key] = true
			ftsGeneration, ftsDirty, vectorGeneration, vectorDirty := state(ref.Table)
			if hasDirtyTracker && !dc.IsDirty(ref.Tenant, ref.Table.Name, ref.Table.Version) && !ftsDirty && !vectorDirty {
				continue
			}
			out = append(out, syncEntry{tenant: ref.Tenant, table: ref.Table, ftsGeneration: ftsGeneration, vectorGeneration: vectorGeneration})
		}
	}
	return out
}

// flushDirtyTables writes every dirty table to the backend.
func (db *DB) flushDirtyTables() error {
	dc, hasDirtyTracker := db.backend.(dirtyTracker)
	skipSearch := db.skipsSearchIndexPersistence()
	if saver, ok := db.backend.(snapshotSaver); ok && hasDirtyTracker {
		return db.flushDirtyTablesSnapshot(saver, dc, skipSearch)
	}

	// Backends without snapshot support get the live table, as they always
	// did: the caller must keep writers out for the duration of Sync.
	for _, e := range db.collectDirtyTables(dc, hasDirtyTracker, skipSearch, true) {
		if err := db.backend.SaveTable(e.tenant, e.table); err != nil {
			return err
		}
		e.table.MarkFTSIndexesPersisted(e.ftsGeneration)
		e.table.MarkVectorIndexesPersisted(e.vectorGeneration)
	}
	return nil
}

// flushDirtyTablesSnapshot is flushDirtyTables for a snapshotSaver: every dirty
// table is snapshotted in one critical section under the content read lock (so
// the written tables are mutually consistent), and written afterwards with no
// engine lock held. A Sync that returns nil has put a snapshot of every table
// that was dirty when it started on disk atomically; a table that changes
// while it runs simply stays dirty for the next Sync.
//
// Nothing in the critical section waits for anything but the copy of row
// headers: the search-index state is read with TryLock, and the images (deep
// copies that can take seconds) are taken before it. A table that became dirty
// only after the images were copied is left for the next Sync; it changed
// after this Sync began, so skipping it keeps the promise above.
//
// "Became dirty after the images were copied" is decided by the table's name
// (tenant and lower-cased table name), not by its object: between the image
// copy and the snapshot critical section a committed transaction, a stream-DML
// clone or a Replace can install a NEW object under the name of a table that
// was dirty when this Sync began (its predecessor was in the first list and had
// its images copied). The new object contains all of the predecessor's changes,
// so skipping it would leave a table that was dirty at the start unwritten
// while Sync returns nil, and a writer that swaps the object during every
// Sync's image copy would starve the table for good. Such a table is written
// WITHOUT search-index images: the predecessor's copies are never reused for a
// different object, the file carries rows and secondary indexes only (the
// images are derived and are rebuilt on the next use after a reopen), and once
// the file is installed the table's existing images are flagged as changed, so
// that the next Sync copies and writes them (outside the content lock, like
// every image copy). A table that was not in the first list under any object
// is still left for the next Sync. Sync therefore never fails or starves
// because of such a swap; the cost is that a table whose object is swapped
// during the image copy of every Sync keeps its images out of the file for as
// long as that continues, while its rows and indexes are durable each time.
func (db *DB) flushDirtyTablesSnapshot(saver snapshotSaver, dc dirtyTracker, skipSearch bool) error {
	for attempt := 1; attempt <= maxSnapshotAttempts; attempt++ {
		// The FTS/ANN images are deep copies that can be large, so they are
		// taken before the snapshot critical section, under only each table's
		// derived-state lock (see copySearchImages for why that ordering is
		// sound). The tables to copy for are found under a brief read lock of
		// their own: Version is only stable under it. Skipped entirely when the
		// backend keeps the images out of its files.
		var images map[*Table]searchImages
		// The tables that were dirty when this attempt began, by name: an
		// object swapped in under one of these names during the image copy is
		// not "dirtied after the Sync began" (see below).
		var provisionalKeys map[string]bool
		if !skipSearch {
			db.contentMu.RLock()
			provisional := db.collectDirtyTables(dc, true, skipSearch, false)
			db.contentMu.RUnlock()
			if len(provisional) == 0 {
				return nil
			}
			copyStart := time.Now()
			images = make(map[*Table]searchImages, len(provisional))
			provisionalKeys = make(map[string]bool, len(provisional))
			for _, e := range provisional {
				provisionalKeys[syncTableKey(e.tenant, e.table.Name)] = true
				images[e.table] = e.table.copySearchImages()
			}
			db.lastImageCopy.Store(int64(time.Since(copyStart)))
			if hook := syncImagesHook.Load(); hook != nil {
				(*hook)()
			}
		}

		started := time.Now()
		db.contentMu.RLock()
		entries := db.collectDirtyTables(dc, true, skipSearch, false)
		// A stable order makes a failing write deterministic to reason about.
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].tenant != entries[j].tenant {
				return entries[i].tenant < entries[j].tenant
			}
			return strings.ToLower(entries[i].table.Name) < strings.ToLower(entries[j].table.Name)
		})
		snaps := make([]*tableSnapshot, 0, len(entries))
		for _, e := range entries {
			var img searchImages
			imageless := false
			if !skipSearch {
				var ok bool
				if img, ok = images[e.table]; !ok {
					// No image copy for this object. If a table of that name was
					// already dirty when this Sync began, this object is the
					// successor of the one that was copied (a committed
					// transaction, a stream-DML clone or a Replace installed a
					// new object under the same name) and holds all of its
					// predecessor's changes: skipping it would break the promise
					// that a nil return put every table that was dirty at the
					// start on disk, and would do so forever if the swap happens
					// during every Sync's image copy. It is written without
					// images. Any other table changed after this Sync began and
					// is left for the next one.
					if !provisionalKeys[syncTableKey(e.tenant, e.table.Name)] {
						continue
					}
					imageless = true
				}
			}
			snap := takeTableSnapshot(e.tenant, e.table, saver.beginTableSnapshot(e.tenant, e.table.Name))
			switch {
			case skipSearch:
			case imageless:
				// Empty image maps: the file carries rows and secondary indexes
				// only. The predecessor's copy is never reused for the new
				// object (a re-created table can carry the same Version numbers
				// and a stale image would be accepted on reopen); the images
				// are derived and are rebuilt on use.
				snap.imageless = true
			default:
				snap.attachSearchImages(img)
				delete(images, e.table) // the snapshot owns the copy now
			}
			snaps = append(snaps, snap)
		}
		db.contentMu.RUnlock()
		images = nil // copies that no snapshot took over (an object swapped out meanwhile) are garbage now, not held for the whole write
		db.lastSnapshotHold.Store(int64(time.Since(started)))
		if len(snaps) == 0 {
			return nil
		}
		if hook := syncSnapshotHook.Load(); hook != nil {
			(*hook)()
		}

		superseded := false
		for i, snap := range snaps {
			err := saver.saveTableSnapshot(snap)
			db.releaseSnapshot(snap)
			snap.dropPayload() // written (or failed): do not hold the rows and images until every table is done
			switch {
			case err == nil:
				switch {
				case skipSearch: // with images skipped the derived-state lock is never touched
				case snap.imageless:
					// The file carries no images for this object: the next
					// Sync must copy and write them (no content lock is held).
					snap.live.flagSearchImagesForNextSync()
				default:
					snap.live.MarkFTSIndexesPersisted(snap.ftsGeneration)
					snap.live.MarkVectorIndexesPersisted(snap.vectorGeneration)
				}
			case errors.Is(err, errSnapshotSuperseded):
				superseded = true
			default:
				for _, rest := range snaps[i+1:] {
					db.releaseSnapshot(rest)
				}
				return err
			}
		}
		if !superseded {
			return nil
		}
	}
	return fmt.Errorf("storage: sync could not settle: a table's file was replaced by another save %d times in a row; call Sync again", maxSnapshotAttempts)
}

// releaseSnapshot gives back the snapshot's borrowed index lists under the
// content read lock. See tableSnapshot.release.
func (db *DB) releaseSnapshot(s *tableSnapshot) {
	if len(s.lists) == 0 {
		return
	}
	db.contentMu.RLock()
	s.release()
	db.contentMu.RUnlock()
}

// LastSyncSnapshotHold reports how long the most recent Sync held the content
// lock's read side to take its table snapshots: the longest a writer can have
// been delayed by that Sync. Diagnostic, for measurements and tests.
func (db *DB) LastSyncSnapshotHold() time.Duration {
	return time.Duration(db.lastSnapshotHold.Load())
}

// dropPayload lets the garbage collector reclaim the snapshot's row headers and
// search-index copies once its table has been written, instead of keeping every
// table's copy alive until the whole Sync finishes. Only the live table and the
// bookkeeping fields remain.
func (s *tableSnapshot) dropPayload() {
	s.table.Rows = nil
	s.table.FTSIndexes = nil
	s.table.VectorIndexes = nil
	s.table.Indexes = nil
}

// LastSyncImageCopy reports how long the most recent Sync spent deep-copying
// the FTS/ANN images of the tables it wrote. The copy holds each table's
// derived-state lock, which statements on that table that use a constraint
// index, an FTS index or an ANN index also take, so this is the longest those
// statements can have waited for that Sync (zero when images are skipped; see
// StorageConfig.SkipSearchIndexPersistence). Diagnostic, for measurements.
func (db *DB) LastSyncImageCopy() time.Duration {
	return time.Duration(db.lastImageCopy.Load())
}
