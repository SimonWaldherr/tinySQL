package storage

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// ChangeWatch is a bounded coalescing wakeup, never an event log.
// A nil/empty table list receives all writes, including unknown write sets.
type ChangeWatch struct {
	C                   <-chan struct{}
	db                  *DB
	ch                  chan struct{}
	tables              []TableRef
	notified, coalesced atomic.Uint64
	broadcasts          atomic.Uint64
}
type ChangeWatchStats struct{ Notifications, Coalesced, Broadcasts uint64 }

func (w *ChangeWatch) Stats() ChangeWatchStats {
	return ChangeWatchStats{w.notified.Load(), w.coalesced.Load(), w.broadcasts.Load()}
}
func normalizeWatchTable(ref TableRef) TableRef {
	return TableRef{strings.ToLower(ref.Tenant), strings.ToLower(ref.Table)}
}
func (w *ChangeWatch) removeScopeLocked() {
	delete(w.db.watchGlobal, w.ch)
	for _, ref := range w.tables {
		delete(w.db.watchTables[ref], w.ch)
		if len(w.db.watchTables[ref]) == 0 {
			delete(w.db.watchTables, ref)
		}
	}
}
func (w *ChangeWatch) SetTables(refs []TableRef) {
	w.db.watchMu.Lock()
	defer w.db.watchMu.Unlock()
	if w.db.watchers[w.ch] != w {
		return
	}
	w.removeScopeLocked()
	w.tables = nil
	if len(refs) == 0 {
		w.db.watchGlobal[w.ch] = struct{}{}
		return
	}
	for _, ref := range refs {
		ref = normalizeWatchTable(ref)
		if w.db.watchTables[ref] == nil {
			w.db.watchTables[ref] = make(map[chan struct{}]struct{})
		}
		w.db.watchTables[ref][w.ch] = struct{}{}
		w.tables = append(w.tables, ref)
	}
}
func (w *ChangeWatch) Close() {
	w.db.watchMu.Lock()
	defer w.db.watchMu.Unlock()
	if w.db.watchers[w.ch] != w {
		return
	}
	w.removeScopeLocked()
	delete(w.db.watchers, w.ch)
	w.db.watchCount.Add(-1)
	close(w.ch)
}
func (db *DB) WatchTableChanges(refs []TableRef) (*ChangeWatch, error) {
	db.watchMu.Lock()
	if db.watchClosed {
		db.watchMu.Unlock()
		return nil, fmt.Errorf("database is closed")
	}
	if db.watchers == nil {
		db.watchers = make(map[chan struct{}]*ChangeWatch)
		db.watchTables = make(map[TableRef]map[chan struct{}]struct{})
		db.watchGlobal = make(map[chan struct{}]struct{})
	}
	ch := make(chan struct{}, 1)
	w := &ChangeWatch{C: ch, ch: ch, db: db}
	db.watchers[ch] = w
	db.watchGlobal[ch] = struct{}{}
	db.watchCount.Add(1)
	db.watchMu.Unlock()
	w.SetTables(refs)
	return w, nil
}

// WatchChanges registers a global wakeup. Writers never wait for consumers.
func (db *DB) WatchChanges() (<-chan struct{}, func(), error) {
	w, err := db.WatchTableChanges(nil)
	if err != nil {
		return nil, nil, err
	}
	return w.C, w.Close, nil
}
func (db *DB) notifyChanges() { db.notifyTableChanges(nil) }
func (db *DB) notifyTableChanges(refs []TableRef) {
	if db.watchCount.Load() == 0 {
		return
	}
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	send := func(ch chan struct{}) {
		w := db.watchers[ch]
		w.notified.Add(1)
		select {
		case ch <- struct{}{}:
		default:
			w.coalesced.Add(1)
		}
	}
	if len(refs) == 0 {
		for ch := range db.watchers {
			db.watchers[ch].broadcasts.Add(1)
			send(ch)
		}
		return
	}
	for ch := range db.watchGlobal {
		send(ch)
	}
	if len(refs) == 1 {
		for ch := range db.watchTables[normalizeWatchTable(refs[0])] {
			send(ch)
		}
		return
	}
	// Table-scoped rollback snapshots currently contain one table. Deduplicate
	// when a caller supplies multiple related tables.
	seen := make(map[chan struct{}]bool)
	for _, ref := range refs {
		for ch := range db.watchTables[normalizeWatchTable(ref)] {
			if !seen[ch] {
				seen[ch] = true
				send(ch)
			}
		}
	}
}

// UnlockContentForWriteTables publishes a proven write set; nil broadcasts.
func (db *DB) UnlockContentForWriteTables(refs []TableRef) {
	db.contentMu.Unlock()
	db.notifyTableChanges(refs)
}
func (db *DB) closeChangeWatchers() {
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	db.watchClosed = true
	db.watchCount.Store(0)
	for ch := range db.watchers {
		close(ch)
		delete(db.watchers, ch)
	}
	db.watchTables = nil
	db.watchGlobal = nil
}

// Internal catalog bookkeeping changes no physical table data. Only listeners
// with unknown/catalog dependencies need a wakeup.
func (db *DB) notifyGlobalChanges() {
	if db.watchCount.Load() == 0 {
		return
	}
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	for ch := range db.watchGlobal {
		w := db.watchers[ch]
		w.notified.Add(1)
		select {
		case ch <- struct{}{}:
		default:
			w.coalesced.Add(1)
		}
	}
}

// HasChangeWatchers lets writers avoid constructing write-set notifications
// when nobody is listening. A later registration still receives a global wakeup.
func (db *DB) HasChangeWatchers() bool { return db.watchCount.Load() != 0 }
