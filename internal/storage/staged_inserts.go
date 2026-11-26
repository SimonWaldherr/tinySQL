package storage

import (
	"fmt"
	"math/big"
	"strings"
)

func stagedInsertKey(tenant, table string) TableRef {
	return TableRef{Tenant: normalizeTenantKey(tenant), Table: strings.ToLower(table)}
}

// StagedInsertTable returns the private, typed buffer for this owner. The
// caller must keep the content write lock while using the result.
func (db *DB) StagedInsertTable(tenant, table, owner string) *Table {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.stagedInserts[stagedInsertKey(tenant, table)][owner]
}

// AppendStagedInsertRows takes ownership of the rows. The caller must hold
// the content write lock and validate column compatibility before appending.
func (db *DB) AppendStagedInsertRows(tenant, owner string, target *Table, rows [][]any) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.closing {
		return fmt.Errorf("database is closed or closing")
	}
	key := stagedInsertKey(tenant, target.Name)
	if db.stagedInserts == nil {
		db.stagedInserts = make(map[TableRef]map[string]*Table)
	}
	byOwner := db.stagedInserts[key]
	if byOwner == nil {
		byOwner = make(map[string]*Table)
		db.stagedInserts[key] = byOwner
	}
	stage := byOwner[owner]
	if stage == nil {
		stage = NewTable(target.Name, cloneColumnsDeep(target.Cols), true)
		stage.Rows = rows
		byOwner[owner] = stage
		return nil
	}
	stage.Rows = append(stage.Rows, rows...)
	return nil
}

// DiscardStagedInserts releases this owner's buffer and returns its row count.
// The caller must hold the content write lock.
func (db *DB) DiscardStagedInserts(tenant, table, owner string) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := stagedInsertKey(tenant, table)
	byOwner := db.stagedInserts[key]
	stage := byOwner[owner]
	if stage == nil {
		return 0
	}
	delete(byOwner, owner)
	if len(byOwner) == 0 {
		delete(db.stagedInserts, key)
	}
	return len(stage.Rows)
}

func (db *DB) discardAllStagedInserts(tenant, table string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.stagedInserts, stagedInsertKey(tenant, table))
}

// CloneRowCell makes a private copy of mutable database values, including
// bound VECTOR, BLOB and JSON cells.
func CloneRowCell(value any) any {
	switch value := value.(type) {
	case *big.Rat:
		if value != nil {
			return new(big.Rat).Set(value)
		}
		return value
	case big.Rat:
		return *new(big.Rat).Set(&value)
	case []any:
		out := make([]any, len(value))
		for i, child := range value {
			out[i] = CloneRowCell(child)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, child := range value {
			out[key] = CloneRowCell(child)
		}
		return out
	default:
		return cloneCell(value)
	}
}
