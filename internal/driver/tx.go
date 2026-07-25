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
	// Atomic swap: writer lock, replace data, save, unlock.
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	oldDB := c.srv.db
	newDB := c.shadow
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
		return err
	}
	if catalogChanged && !oldDB.SnapshotCatalog().Equal(c.txCatalog) {
		// Another connection changed the catalog after this transaction began.
		// Installing this transaction's copy would discard that change, so
		// report a retryable conflict instead.
		_ = c.shadow.AbortAmbientWALTx()
		c.clearTxState()
		return fmt.Errorf("%w on the system catalog", ErrTransactionConflict)
	}
	wal := oldDB.WAL()
	needCheckpoint := false
	var err error
	if wal != nil && len(changes) > 0 {
		needCheckpoint, err = wal.LogTransaction(changes)
		if err != nil {
			return err
		}
	}
	if err := newDB.CommitAmbientWALTx(); err != nil {
		return err
	}
	if err := oldDB.ApplyWALChanges(changes); err != nil {
		return err
	}
	if catalogChanged {
		oldDB.AdoptCatalog(newDB.Catalog())
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
