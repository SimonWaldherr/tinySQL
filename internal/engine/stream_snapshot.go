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
	case *CreateTable, *DropTable, *CreateView, *DropView,
		*CreateMaterializedView, *DropMaterializedView,
		*AlterViewMaterialize, *AlterMaterializedViewToView,
		*CreateJob, *AlterJob, *DropJob, *CreateTrigger, *DropTrigger,
		*CreateUser, *DropUser, *AlterUser, *CreateRole, *DropRole,
		*GrantPrivilege, *RevokePrivilege, *GrantRoleStmt, *RevokeRoleStmt:
		// These create, remove or re-point whole objects and the catalog; none
		// of them writes into the contents of a table that already exists, so
		// a pinned table (a stream's source, or one a transaction shadow
		// shares with the live database) can stay as it is. DROP TABLE only
		// discards derived caches.
	default:
		// ANALYZE, REFRESH MATERIALIZED VIEW, CALL and any statement type added
		// later: anything not listed above may reach into tables it never
		// names, so every pinned table is detached before it runs. Copying more
		// than necessary is safe; mutating a table a stream or a transaction
		// snapshot still reads is not.
		db.DetachAllPinnedForWrite()
	}
}
