// Automatic WAL logging for INSERT/UPDATE/DELETE, for both WAL
// implementations tinySQL supports.
//
// tinySQL has two WAL implementations — the basic WALManager (whole-table
// snapshot diffing, used by ModeWAL) and AdvancedWAL (row-level, LSN-based,
// with real REDO/recovery, used by ModeAdvancedWAL) — but historically
// nothing in the engine's own INSERT/UPDATE/DELETE path called either one.
// Both were only driven by internal/driver's explicit BeginTx/Commit and
// autocommit paths. A caller using ModeWAL or ModeAdvancedWAL and writing
// through the ordinary tinysql.Execute API (not through internal/driver's
// connection machinery — e.g. cmd/server, or any direct embedder) got no
// durability logging at all: a silent gap, since nothing errored or warned
// about it, and DB.HealthCheck() still reported WALActive/AdvancedWALActive
// as true.
//
// walAuto wraps each mutating statement in its own implicit, autocommitted
// AdvancedWAL transaction (LogBegin before the row loop, LogCommit after)
// and logs every row change as it happens. maybeLogToWALManager instead
// diffs the statement's pre-execution snapshot against the now-mutated DB
// and logs the resulting table-level changes to WALManager, mirroring what
// internal/driver's commitTx/execStatement already do for driver-mediated
// access. Both are complete no-ops — zero calls, zero overhead beyond a nil
// check — when the corresponding WAL isn't attached.
package engine

import "github.com/SimonWaldherr/tinySQL/internal/storage"

// walAuto tracks the AdvancedWAL transaction (if any) for one
// INSERT/UPDATE/DELETE statement. Its zero value is inert: every method is a
// no-op when wal is nil, so callers can construct one unconditionally and
// only pay for the nil check.
type walAuto struct {
	wal            *storage.AdvancedWAL
	txID           storage.TxID
	table          string
	deferredCommit bool
}

// statementWAL groups the row records emitted by one outer Execute call into
// a single AdvancedWAL transaction. Nested DML from triggers shares this
// object, so its records cannot be committed if a later trigger fails and the
// statement is rolled back.
type statementWAL struct {
	wal     *storage.AdvancedWAL
	txID    storage.TxID
	started bool
	// ambient is set when txID belongs to a transaction the SQL driver owns.
	// Such a transaction must not be committed per statement.
	ambient bool
}

// newStatementWAL binds a statement to the AdvancedWAL, if one is attached.
//
// When db has an ambient transaction — the SQL driver opened one for a
// BEGIN…COMMIT block — the statement joins it and never writes a commit
// record of its own: the driver commits or aborts the whole block, and
// recovery replays it only in the committed case. Otherwise the statement is
// its own implicitly-committed transaction, which is the correct autocommit
// behavior.
func newStatementWAL(db *storage.DB) *statementWAL {
	wal := db.StatementAdvancedWAL()
	if wal == nil {
		return &statementWAL{}
	}
	if txID, ok := db.AmbientWALTx(); ok {
		return &statementWAL{wal: wal, txID: txID, started: true, ambient: true}
	}
	return &statementWAL{wal: wal}
}

func (s *statementWAL) begin() error {
	if s == nil || s.wal == nil || s.started {
		return nil
	}
	s.txID = s.wal.NewAutoTxID()
	if _, err := s.wal.LogBegin(s.txID); err != nil {
		return err
	}
	s.started = true
	return nil
}

func (s *statementWAL) commit() error {
	if s == nil || s.wal == nil || !s.started || s.ambient {
		return nil
	}
	_, err := s.wal.LogCommit(s.txID)
	return err
}

// beginWALAuto starts an implicit WAL transaction for a statement against
// table, if an AdvancedWAL is attached to env's DB. Callers must call
// commit() exactly once, after all rows for the statement have been logged
// (typically via defer, checking the returned error only where a mid-loop
// failure should abort the statement — see executeInsertAllColumns for the
// pattern).
func beginWALAuto(env ExecEnv, table string) (*walAuto, error) {
	if env.statementWAL != nil {
		if err := env.statementWAL.begin(); err != nil {
			return nil, err
		}
		if env.statementWAL.wal == nil {
			return &walAuto{}, nil
		}
		return &walAuto{
			wal:            env.statementWAL.wal,
			txID:           env.statementWAL.txID,
			table:          table,
			deferredCommit: true,
		}, nil
	}
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
	if a.deferredCommit {
		return nil
	}
	_, err := a.wal.LogCommit(a.txID)
	return err
}

// maybeLogToWALManager drives the basic WALManager (ModeWAL) for one
// successfully completed mutating statement. before is the metadata-only
// pre-execution image captured by executeStatement (see storage.DB.MetaSnapshot);
// db is the now-mutated database. It is the single place where a statement
// reaches WALManager: the SQL driver logs only whole transactions, and a
// transaction shadow reports no StatementWAL at all, so no change is ever
// written twice or written before it is committed.
//
// Diffing a metadata image rather than the statement's rollback snapshot is
// what lets DDL be logged: CREATE/DROP TABLE take no rollback snapshot, but
// they do change the set of tables and their versions.
//
// A no-op when no WALManager is this database's responsibility, when there is
// no pre-image, or when the statement changed no table.
func maybeLogToWALManager(db *storage.DB, before *storage.DB) error {
	wal := db.StatementWAL()
	if wal == nil || before == nil {
		return nil
	}
	changes := storage.CollectWALChanges(before, db)
	if len(changes) == 0 {
		return nil
	}
	needCheckpoint, err := wal.LogTransaction(changes)
	if err != nil {
		return err
	}
	if needCheckpoint {
		return wal.Checkpoint(db)
	}
	return nil
}
