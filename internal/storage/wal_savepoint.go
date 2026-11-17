// Savepoint support for AdvancedWAL transactions.
//
// An AdvancedWAL transaction is replayed by appending every logged insert and
// by locating each logged update or delete by position and before-image (see
// applyOperation). Records are never retracted, so a row operation that a
// ROLLBACK TO SAVEPOINT discards is still in the log. LogSavepointRestore
// therefore logs the inverse operations that turn the transaction's current
// rows back into the rows the savepoint saw. Replaying the transaction then
// reproduces the savepoint state, and recovery never resurrects a discarded row.
package storage

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// ErrSavepointSchemaRollback reports that a rollback to a savepoint would have
// to undo a CREATE, DROP or ALTER TABLE on an AdvancedWAL database. Row
// operations can be logged in reverse, but a table definition is captured only
// by checkpoints, so such a rollback cannot be described to the log.
var ErrSavepointSchemaRollback = errors.New("tinysql: ROLLBACK TO SAVEPOINT cannot undo a CREATE, DROP or ALTER TABLE on a mode=advancedwal database")

// AdoptAmbientWALTx makes db a continuation of src's AdvancedWAL transaction.
//
// Copies made by SnapshotForWriteTx carry the runtime state of their source but
// not its per-transaction WAL state (see copyRuntimeState). A shadow rebuilt
// from a savepoint therefore has to adopt the ambient transaction explicitly:
// otherwise its next write would open a second transaction and the records
// already logged under the first would never be committed.
func (db *DB) AdoptAmbientWALTx(src *DB) {
	if db == nil || src == nil {
		return
	}
	if txID, ok := src.AmbientWALTx(); ok {
		db.ambientWALTx.Store(uint64(txID))
	}
	if src.advancedWALMetadataDirty.Load() {
		db.advancedWALMetadataDirty.Store(true)
	}
}

// LogSavepointRestore logs, into db's ambient AdvancedWAL transaction, the row
// operations that turn db's table rows back into saved's table rows. saved is
// the copy taken when the savepoint was created.
//
// It does nothing unless an AdvancedWAL is attached and an ambient transaction
// is open: without one, no row operation of this transaction has been logged
// yet, so nothing needs to be undone. Schema differences are checked before any
// record is written, so a rejected rollback leaves the log untouched.
//
// The compensation is positional. Tables are compared row by row from the top;
// everything after the first differing row is deleted from the end, and the
// savepoint's remaining rows are appended. That is the same order in which
// replay would have to reproduce them, because replay appends inserts and
// deletes preserve order. Rows that the transaction never touched are not
// logged again.
func (db *DB) LogSavepointRestore(saved *DB) error {
	if db == nil || saved == nil {
		return nil
	}
	wal := db.AdvancedWAL()
	if wal == nil {
		return nil
	}
	txID, open := db.AmbientWALTx()
	if !open {
		return nil
	}

	current := db.savepointTableSet()
	before := saved.savepointTableSet()
	keys := make([]savepointTableKey, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	for key := range before {
		if _, ok := current[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tenant != keys[j].tenant {
			return keys[i].tenant < keys[j].tenant
		}
		return keys[i].name < keys[j].name
	})

	for _, key := range keys {
		cur, inCurrent := current[key]
		old, inSaved := before[key]
		if inCurrent && inSaved && reflect.DeepEqual(cur.Cols, old.Cols) {
			continue
		}
		name := key.name
		if inCurrent {
			name = cur.Name
		} else if inSaved {
			name = old.Name
		}
		return fmt.Errorf("%w: table %q", ErrSavepointSchemaRollback, name)
	}

	for _, key := range keys {
		cur := current[key]
		old := before[key]
		common := 0
		for common < len(cur.Rows) && common < len(old.Rows) && rowsEqual(cur.Rows[common], old.Rows[common]) {
			common++
		}
		for i := len(cur.Rows) - 1; i >= common; i-- {
			if _, err := wal.LogDelete(txID, key.tenant, cur.Name, int64(i), cur.Rows[i], cur.Cols); err != nil {
				return err
			}
		}
		for i := common; i < len(old.Rows); i++ {
			if _, err := wal.LogInsert(txID, key.tenant, cur.Name, int64(i), old.Rows[i], cur.Cols); err != nil {
				return err
			}
		}
	}
	return nil
}

// savepointTableKey identifies a table across tenants with the case-insensitive
// name the catalog uses.
type savepointTableKey struct {
	tenant string
	name   string
}

// savepointTableSet returns every table of db keyed by tenant and lower-case
// name. It holds the structure lock only while collecting the pointers.
func (db *DB) savepointTableSet() map[savepointTableKey]*Table {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make(map[savepointTableKey]*Table)
	for tenant, td := range db.tenants {
		for name, t := range td.tables {
			out[savepointTableKey{tenant: tenant, name: strings.ToLower(name)}] = t
		}
	}
	return out
}
