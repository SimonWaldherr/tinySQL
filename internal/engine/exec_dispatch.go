package engine

import (
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// execStmt dispatches a statement while the caller already holds the content
// lock. Nested execution must use this function rather than Execute because
// sync.RWMutex is not reentrant.
func execStmt(env ExecEnv, stmt Statement) (*ResultSet, error) {
	if env.db.IsReadOnly() {
		if err := rejectIfMutating(stmt); err != nil {
			return nil, err
		}
	}
	switch s := stmt.(type) {
	case *Explain:
		return executeExplain(env, s)
	case *Analyze:
		return executeAnalyze(env, s)
	case *Pragma:
		return executePragma(env, s)
	case *CreateTable:
		return executeCreateTable(env, s)
	case *DropTable:
		return executeDropTable(env, s)
	case *CreateIndex:
		return executeCreateIndex(env, s)
	case *DropIndex:
		return executeDropIndex(env, s)
	case *CreateView:
		return executeCreateView(env, s)
	case *DropView:
		return executeDropView(env, s)
	case *CreateMaterializedView:
		return executeCreateMaterializedView(env, s)
	case *DropMaterializedView:
		return executeDropMaterializedView(env, s)
	case *RefreshMaterializedView:
		return executeRefreshMaterializedView(env, s)
	case *AlterViewMaterialize:
		return executeAlterViewMaterialize(env, s)
	case *AlterMaterializedViewToView:
		return executeAlterMaterializedViewToView(env, s)
	case *AlterTable:
		return executeAlterTable(env, s)
	case *Insert:
		return executeInsert(env, s)
	case *Update:
		return executeUpdate(env, s)
	case *Delete:
		return executeDelete(env, s)
	case *CallProcedure:
		return executeCallProcedure(env, s)
	case *Select:
		return executeSelect(env, s)
	case *CreateJob:
		return executeCreateJob(env, s)
	case *AlterJob:
		return executeAlterJob(env, s)
	case *DropJob:
		return executeDropJob(env, s)
	case *CreateTrigger:
		return executeCreateTrigger(env, s)
	case *DropTrigger:
		return executeDropTrigger(env, s)
	case *CreateUser:
		return executeCreateUser(env, s)
	case *DropUser:
		return executeDropUser(env, s)
	case *AlterUser:
		return executeAlterUser(env, s)
	case *CreateRole:
		return executeCreateRole(env, s)
	case *DropRole:
		return executeDropRole(env, s)
	case *GrantPrivilege:
		return executeGrantPrivilege(env, s)
	case *RevokePrivilege:
		return executeRevokePrivilege(env, s)
	case *GrantRoleStmt:
		return executeGrantRoleStmt(env, s)
	case *RevokeRoleStmt:
		return executeRevokeRoleStmt(env, s)
	}
	return nil, fmt.Errorf("unknown statement")
}

func isReadOnlyStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *Select, *Explain, *Pragma:
		return true
	default:
		return false
	}
}

func rejectIfMutating(stmt Statement) error {
	if isReadOnlyStatement(stmt) {
		return nil
	}
	return fmt.Errorf("database is in read-only mode: %T statements are not allowed", stmt)
}

func executeAnalyze(env ExecEnv, s *Analyze) (*ResultSet, error) {
	tables := make([]*storage.Table, 0)
	if s.Table != "" {
		table, err := env.db.Get(env.tenant, s.Table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	} else {
		tables = env.db.ListTables(env.tenant)
	}
	rows := make([]Row, 0, len(tables))
	for _, table := range tables {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		stats := table.Analyze()
		row := Row{}
		putVal(row, "table_name", table.Name)
		putVal(row, "row_count", stats.RowCount)
		putVal(row, "column_count", len(stats.Columns))
		putVal(row, "analyzed_at", stats.AnalyzedAt)
		rows = append(rows, row)
	}
	return &ResultSet{Cols: []string{"table_name", "row_count", "column_count", "analyzed_at"}, Rows: rows}, nil
}
