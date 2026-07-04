// Automatic AdvancedWAL logging for INSERT/UPDATE/DELETE.
//
// Before this, tinySQL had two WAL implementations — the basic WALManager
// (whole-table snapshot diffing, used by internal/driver's explicit
// BeginTx/Commit) and AdvancedWAL (row-level, LSN-based, with real
// REDO/recovery) — but nothing in the engine's own INSERT/UPDATE/DELETE path
// ever called AdvancedWAL's LogInsert/LogUpdate/LogDelete. A caller using
// ModeAdvancedWAL and writing through the ordinary tinysql.Execute API (not
// through internal/driver's transaction machinery) got no durability
// logging at all — a silent gap, since nothing errored or warned about it.
//
// walAuto wraps each mutating statement in its own implicit, autocommitted
// WAL transaction (LogBegin before the row loop, LogCommit after) and logs
// every row change as it happens. It is a complete no-op — zero calls, zero
// overhead beyond one nil check — when no AdvancedWAL is attached, which
// remains the default for ModeMemory/ModeDisk/ModeJSON/ModeHybrid/ModeWAL.
package engine

import "github.com/SimonWaldherr/tinySQL/internal/storage"

// walAuto tracks the AdvancedWAL transaction (if any) for one
// INSERT/UPDATE/DELETE statement. Its zero value is inert: every method is a
// no-op when wal is nil, so callers can construct one unconditionally and
// only pay for the nil check.
type walAuto struct {
	wal   *storage.AdvancedWAL
	txID  storage.TxID
	table string
}

// beginWALAuto starts an implicit WAL transaction for a statement against
// table, if an AdvancedWAL is attached to env's DB. Callers must call
// commit() exactly once, after all rows for the statement have been logged
// (typically via defer, checking the returned error only where a mid-loop
// failure should abort the statement — see executeInsertAllColumns for the
// pattern).
func beginWALAuto(env ExecEnv, table string) (*walAuto, error) {
	wal := env.db.AdvancedWAL()
	if wal == nil {
		return &walAuto{}, nil
	}
	txID := wal.NewAutoTxID()
	if _, err := wal.LogBegin(txID); err != nil {
		return nil, err
	}
	return &walAuto{wal: wal, txID: txID, table: table}, nil
}

func (a *walAuto) logInsert(env ExecEnv, rowIdx int, row []any, cols []storage.Column) error {
	if a == nil || a.wal == nil {
		return nil
	}
	_, err := a.wal.LogInsert(a.txID, env.tenant, a.table, int64(rowIdx), row, cols)
	return err
}

func (a *walAuto) logUpdate(env ExecEnv, rowIdx int, before, after []any, cols []storage.Column) error {
	if a == nil || a.wal == nil {
		return nil
	}
	_, err := a.wal.LogUpdate(a.txID, env.tenant, a.table, int64(rowIdx), before, after, cols)
	return err
}

func (a *walAuto) logDelete(env ExecEnv, rowIdx int, before []any, cols []storage.Column) error {
	if a == nil || a.wal == nil {
		return nil
	}
	_, err := a.wal.LogDelete(a.txID, env.tenant, a.table, int64(rowIdx), before, cols)
	return err
}

// commit finalizes the implicit transaction. Call unconditionally (e.g. via
// defer) once all rows have been logged; it is a no-op when no AdvancedWAL
// is attached or beginWALAuto never started a transaction.
func (a *walAuto) commit() error {
	if a == nil || a.wal == nil {
		return nil
	}
	_, err := a.wal.LogCommit(a.txID)
	return err
}
