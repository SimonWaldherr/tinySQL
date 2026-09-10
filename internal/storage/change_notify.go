package storage

import "fmt"

// WatchChanges registers a coalescing wakeup, not a transaction log. Consumers
// must inspect committed state under the content read lock. Writers never wait
// for a listener. Cancel unregisters and closes the channel; it is idempotent.
func (db *DB) WatchChanges() (<-chan struct{}, func(), error) {
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	if db.watchClosed {
		return nil, nil, fmt.Errorf("database is closed")
	}
	if db.watchers == nil {
		db.watchers = make(map[chan struct{}]struct{})
	}
	ch := make(chan struct{}, 1)
	db.watchers[ch] = struct{}{}
	db.watchCount.Add(1)
	cancel := func() {
		db.watchMu.Lock()
		defer db.watchMu.Unlock()
		if _, ok := db.watchers[ch]; ok {
			delete(db.watchers, ch)
			db.watchCount.Add(-1)
			close(ch)
		}
	}
	return ch, cancel, nil
}

func (db *DB) notifyChanges() {
	if db.watchCount.Load() == 0 {
		return
	}
	db.watchMu.Lock()
	defer db.watchMu.Unlock()
	for ch := range db.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
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
}
