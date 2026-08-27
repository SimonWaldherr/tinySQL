package engine

import "github.com/SimonWaldherr/tinySQL/internal/storage"

// detachStreamSnapshotsForWrite performs the copy-on-write half of a direct
// ResultStream table snapshot. Its caller must hold DB's content write lock.
//
// The top-level call in executeStatement runs before WAL/DML planning captures
// any table pointers. The matching call from execStmt covers trigger and stored
// procedure bodies, which reuse the outer content lock and therefore bypass
// executeStatement.
func detachStreamSnapshotsForWrite(db *storage.DB, tenant string, stmt Statement) {
	if db == nil || stmt == nil {
		return
	}

	switch s := stmt.(type) {
	case *Explain:
		if s.Analyze {
			detachStreamSnapshotsForWrite(db, tenant, s.Statement)
		}
	case *Insert:
		db.DetachPinnedTableForWrite(tenant, s.Table)
	case *Update:
		db.DetachPinnedTableForWrite(tenant, s.Table)
	case *Delete:
		db.DetachPinnedTableForWrite(tenant, s.Table)
	case *AlterTable:
		db.DetachPinnedTableForSchemaWrite(tenant, s.Table)
	case *CreateIndex:
		db.DetachPinnedTableForWrite(tenant, s.Table)
	case *DropIndex:
		table := s.Table
		if table == "" {
			schema, name := splitObjectName(s.Name)
			if index, ok := db.Catalog().GetIndexForTenant(tenant, schema, name); ok {
				table = index.Table
			}
		}
		db.DetachPinnedTableForWrite(tenant, table)
	}
}
