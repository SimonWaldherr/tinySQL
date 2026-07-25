// The unit of work the write-ahead log records: which tables a change touched,
// how to compute that from two database states, and how to apply it. Shared by
// the log itself and by the SQL driver's commit path.
package storage

import (
	"fmt"
	"sort"
	"strings"
)

type walRecordType uint8

const (
	walRecordBegin walRecordType = iota + 1
	walRecordApplyTable
	walRecordDropTable
	walRecordCommit
	walRecordAppendRows // delta: only the new rows appended by INSERT
	walRecordUpdateRows // delta: only the rows an UPDATE replaced in place
)

type walRecord struct {
	Seq       uint64
	TxID      uint64
	Tenant    string
	TableName string
	Table     *diskTable
	Type      walRecordType
	WrittenAt int64
	// RowIndexes positions Table.Rows for walRecordUpdateRows: row i of the
	// record replaces row RowIndexes[i] of the table. Unused by other types.
	RowIndexes []int
}

type walOperation struct {
	tenant     string
	name       string
	drop       bool
	appendOnly bool
	// rowIndexes is set for an update-rows delta, positioning table.Rows.
	rowIndexes []int
	table      *diskTable
}

// WALChange describes a persistent change that will be written to the WAL.
type WALChange struct {
	Tenant string
	Name   string
	Table  *Table
	Drop   bool
}

// CollectWALChanges computes the delta between two MVCC snapshots.
func CollectWALChanges(prev, next *DB) []WALChange {
	if prev == nil || next == nil {
		return nil
	}
	// Estimate capacity based on number of tables in next
	var estCapacity int
	for _, tdb := range next.tenants {
		estCapacity += len(tdb.tables)
	}
	changes := make([]WALChange, 0, estCapacity)
	for tn, nextTenant := range next.tenants {
		prevTenant := prev.tenants[tn]
		for key, nt := range nextTenant.tables {
			var pv *Table
			if prevTenant != nil {
				pv = prevTenant.tables[key]
			}
			if pv == nil || pv.Version != nt.Version {
				changes = append(changes, WALChange{Tenant: tn, Name: nt.Name, Table: nt})
			}
		}
	}
	for tn, prevTenant := range prev.tenants {
		nextTenant := next.tenants[tn]
		for key, pt := range prevTenant.tables {
			if nextTenant == nil || nextTenant.tables[key] == nil {
				changes = append(changes, WALChange{Tenant: tn, Name: pt.Name, Drop: true})
			}
		}
	}
	if len(changes) <= 1 {
		return changes
	}
	sort.SliceStable(changes, func(i, j int) bool {
		if changes[i].Tenant == changes[j].Tenant {
			return strings.ToLower(changes[i].Name) < strings.ToLower(changes[j].Name)
		}
		return strings.ToLower(changes[i].Tenant) < strings.ToLower(changes[j].Tenant)
	})
	return changes
}

// ApplyWALChanges applies a set of table-level changes to the database. It is
// used by the SQL driver to commit a transaction by merging the transaction's
// delta into the latest shared database instead of replacing the whole DB with
// an older snapshot.
func (db *DB) ApplyWALChanges(changes []WALChange) error {
	for _, ch := range changes {
		if ch.Drop {
			db.mu.Lock()
			td := db.getTenant(ch.Tenant)
			delete(td.tables, strings.ToLower(ch.Name))
			db.mu.Unlock()
			if db.backend != nil && db.backend.TableExists(ch.Tenant, ch.Name) {
				if err := db.backend.DeleteTable(ch.Tenant, ch.Name); err != nil {
					return fmt.Errorf("backend delete %s/%s: %w", ch.Tenant, ch.Name, err)
				}
			}
			continue
		}
		if ch.Table == nil {
			continue
		}
		db.mu.Lock()
		db.getTenant(ch.Tenant).tables[strings.ToLower(ch.Name)] = ch.Table
		db.mu.Unlock()
	}
	return nil
}
