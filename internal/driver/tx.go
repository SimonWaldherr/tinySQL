// Transactions. A transaction runs against a private shadow database and is
// merged into the live one at COMMIT: table changes through ApplyWALChanges, the
// catalog separately, both guarded by conflict detection against the state the
// transaction started from.
package driver

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func (c *conn) commitTx() error {
	if !c.inTx {
		return fmt.Errorf("tinysql: no active transaction")
	}
	// Read-only transactions produce no changes to merge: their snapshot is
	// either the immutable shared database (shadow == nil) or a private read
	// clone (shadow != nil, txBase == nil). Either way there is nothing to
	// commit, so skip the writer lock and change-collection entirely.
	if c.txReadOnly {
		c.clearTxState()
		return nil
	}
	// A BEGIN/COMMIT pair with no successful write cannot change the shared
	// database, so it needs neither the writer slot nor change collection.
	if !c.txDirty {
		if err := c.shadow.AbortAmbientWALTx(); err != nil {
			c.clearTxState()
			return err
		}
		c.clearTxState()
		return nil
	}
	if c.shadow == nil {
		return fmt.Errorf("tinysql: no active transaction snapshot")
	}
	if err := c.srv.acquireWriter(context.Background()); err != nil {
		return err
	}
	defer c.srv.releaseWriter()
	// writeMu serializes this commit against every other writer for its
	// whole span, mutation and persist alike -- see server.writeMu's doc
	// comment. commitTxApply's server.mu.Lock() is narrower: held only
	// while actually applying the transaction to the live DB, so a
	// concurrent SELECT is blocked only for that in-memory step, not for
	// the checkpoint/persist I/O that follows -- safe specifically because
	// writeMu still excludes every other writer while that I/O runs.
	c.srv.writeMu.Lock()
	defer c.srv.writeMu.Unlock()
	oldDB := c.srv.db
	newDB := c.shadow
	wal, needCheckpoint, err := c.commitTxApply(oldDB, newDB)
	if err != nil {
		return err
	}
	if wal != nil && needCheckpoint {
		if err := wal.Checkpoint(oldDB); err != nil {
			return err
		}
	}
	// The transaction is applied in memory at this point, so clear its state
	// either way — but do not report a successful COMMIT if the durable write
	// failed. An application that got "commit ok" and then lost the rows on
	// restart has no way to detect that; an error it can retry or alert on.
	persistErr := c.srv.persist()
	c.clearTxState()
	return persistErr
}

// commitTxApply performs every step of a commit that must exclude readers
// -- conflict detection, the WAL append (whose ResetDirty side effect isn't
// independently guarded, and whose cost is proportional to this
// transaction's own change set rather than the whole database), and
// actually applying the transaction's changes to oldDB (the live database)
// -- under server.mu.Lock(). It reports the WAL manager and whether a
// checkpoint is owed so the caller can run that, and persist(), afterward
// with server.mu released: both are safe to run concurrently with readers
// once this function returns, since the mutation they'd otherwise race
// with is already complete. The caller must already hold server.writeMu so
// no other writer's mutation can interleave with the checkpoint/persist
// tail.
//
// This function also reads (detectTxConflicts's Table.Version comparison)
// and writes (ApplyWALChanges/AdoptCatalog) oldDB's live table content
// directly, bypassing engine.Execute entirely -- so unlike an ordinary
// statement, it does not get contentMu for free from executeStatement.
// oldDB.LockContentForWrite() below is what closes that gap: without it, a
// job scheduler's engine.Execute call on the same *storage.DB (reachable
// when an app shares one DB between a driver server and StartJobScheduler
// via SetDefaultDB) could mutate a table's Rows/Version concurrently with
// this function reading or overwriting them, since the scheduler never
// touches server.mu/writeMu at all. See BeginTx (conn.go) for the
// equivalent gap on the snapshot/clone side.
func (c *conn) commitTxApply(oldDB, newDB *storage.DB) (wal *storage.WALManager, needCheckpoint bool, err error) {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	oldDB.LockContentForWrite()
	defer oldDB.UnlockContentForWrite()
	changes := storage.CollectWALChanges(c.txBase, newDB)
	// The transaction's catalog work — CREATE VIEW/TRIGGER/JOB, GRANT, and the
	// materialized-view staleness marks DML leaves behind — lives on the
	// shadow's private catalog copy and is not part of `changes`, which covers
	// tables only. It has to be published with the transaction rather than
	// silently dropped at COMMIT.
	//
	// The revision counter is only a gate: ordinary DML takes the catalog's
	// write lock even when it changes nothing there, so a bumped revision does
	// not by itself mean this transaction has catalog state to commit. Compare
	// the contents to find out.
	catalogChanged := false
	var shadowCatalog storage.CatalogSnapshot
	if newDB.Catalog().Revision() != c.txCatalogRev {
		shadowCatalog = newDB.SnapshotCatalog()
		catalogChanged = !shadowCatalog.Equal(c.txCatalog)
	}
	if err := c.detectTxConflicts(oldDB, changes); err != nil {
		_ = c.shadow.AbortAmbientWALTx()
		c.clearTxState()
		return nil, false, err
	}
	if catalogChanged && !oldDB.SnapshotCatalog().Equal(c.txCatalog) {
		// Another connection changed the catalog after this transaction began.
		// Installing this transaction's copy would discard that change, so
		// report a retryable conflict instead.
		_ = c.shadow.AbortAmbientWALTx()
		c.clearTxState()
		return nil, false, fmt.Errorf("%w on the system catalog", ErrTransactionConflict)
	}
	wal = oldDB.WAL()
	if wal != nil && len(changes) > 0 {
		needCheckpoint, err = wal.LogTransaction(changes)
		if err != nil {
			return nil, false, err
		}
	}
	if err := newDB.CommitAmbientWALTx(); err != nil {
		return nil, false, err
	}
	if err := oldDB.ApplyWALChanges(changes); err != nil {
		return nil, false, err
	}
	if catalogChanged {
		oldDB.AdoptCatalog(newDB.Catalog())
	}
	return wal, needCheckpoint, nil
}

func (c *conn) detectTxConflicts(current *storage.DB, changes []storage.WALChange) error {
	if c.txBase == nil {
		return nil
	}
	for _, ch := range changes {
		baseTable, baseErr := c.txBase.Get(ch.Tenant, ch.Name)
		currentTable, currentErr := current.Get(ch.Tenant, ch.Name)
		baseExists := baseErr == nil
		currentExists := currentErr == nil
		switch {
		case !baseExists && currentExists:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		case baseExists && !currentExists:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		case baseExists && currentExists && baseTable.Version != currentTable.Version:
			return fmt.Errorf("%w on table %q", ErrTransactionConflict, ch.Name)
		}
	}
	return nil
}

func (c *conn) rollbackTx() error {
	if !c.inTx {
		return fmt.Errorf("tinysql: no active transaction")
	}
	// Discarding the shadow is enough for the in-memory state, but ModeAdvancedWAL
	// has already written this block's row operations to disk. Record the abort so
	// recovery discards them instead of replaying rolled-back rows.
	err := c.shadow.AbortAmbientWALTx()
	c.clearTxState()
	return err
}

func (c *conn) clearTxState() {
	c.inTx = false
	c.txBase = nil
	c.shadow = nil
	c.txReadOnly = false
	c.txDirty = false
	c.txCatalogRev = 0
}
