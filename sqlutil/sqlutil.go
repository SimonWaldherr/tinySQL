// Package sqlutil exposes stable SQL analysis helpers for tools that need to
// classify statements without depending on tinySQL's internal engine package.
package sqlutil

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// StatementKind is a coarse statement classification.
type StatementKind string

const (
	// StatementKind values classify SQL statements at a coarse, stable level.
	KindUnknown                StatementKind = "unknown"
	KindSelect                 StatementKind = "select"
	KindExplain                StatementKind = "explain"
	KindAnalyze                StatementKind = "analyze"
	KindPragma                 StatementKind = "pragma"
	KindInsert                 StatementKind = "insert"
	KindUpdate                 StatementKind = "update"
	KindDelete                 StatementKind = "delete"
	KindCreateTable            StatementKind = "create_table"
	KindDropTable              StatementKind = "drop_table"
	KindCreateIndex            StatementKind = "create_index"
	KindDropIndex              StatementKind = "drop_index"
	KindCreateView             StatementKind = "create_view"
	KindDropView               StatementKind = "drop_view"
	KindCreateMaterializedView StatementKind = "create_materialized_view"
	KindDropMaterializedView   StatementKind = "drop_materialized_view"
	KindRefreshMaterialized    StatementKind = "refresh_materialized_view"
	KindAlter                  StatementKind = "alter"
	KindCreateTrigger          StatementKind = "create_trigger"
	KindDropTrigger            StatementKind = "drop_trigger"
	KindCreateJob              StatementKind = "create_job"
	KindAlterJob               StatementKind = "alter_job"
	KindDropJob                StatementKind = "drop_job"
)

// Analysis describes a parsed SQL statement at a level suitable for UI guards,
// auto-run checks, and audit labels.
type Analysis struct {
	Kind            StatementKind
	ObjectName      string
	ReadOnly        bool
	ResultProducing bool
	Mutation        bool
	DDL             bool
	Job             bool
}

// Analyze parses sql and returns a coarse statement classification.
func Analyze(sql string) (Analysis, error) {
	stmt, err := engine.NewParser(sql).ParseStatement()
	if err != nil {
		return Analysis{Kind: KindUnknown}, err
	}
	return AnalyzeStatement(stmt), nil
}

// AnalyzeStatement classifies an already parsed statement.
func AnalyzeStatement(stmt engine.Statement) Analysis {
	switch s := stmt.(type) {
	case *engine.Select:
		return Analysis{Kind: KindSelect, ReadOnly: true, ResultProducing: true}
	case *engine.Explain:
		return Analysis{Kind: KindExplain, ReadOnly: true, ResultProducing: true}
	case *engine.Analyze:
		return Analysis{Kind: KindAnalyze, ObjectName: s.Table, Mutation: true, ResultProducing: true}
	case *engine.Pragma:
		return Analysis{Kind: KindPragma, ReadOnly: true, ResultProducing: true}
	case *engine.Insert:
		return Analysis{Kind: KindInsert, ObjectName: s.Table, Mutation: true}
	case *engine.Update:
		return Analysis{Kind: KindUpdate, ObjectName: s.Table, Mutation: true}
	case *engine.Delete:
		return Analysis{Kind: KindDelete, ObjectName: s.Table, Mutation: true}
	case *engine.CreateTable:
		return Analysis{Kind: KindCreateTable, ObjectName: s.Name, DDL: true}
	case *engine.DropTable:
		return Analysis{Kind: KindDropTable, ObjectName: s.Name, DDL: true}
	case *engine.CreateIndex:
		return Analysis{Kind: KindCreateIndex, ObjectName: s.Name, DDL: true}
	case *engine.DropIndex:
		return Analysis{Kind: KindDropIndex, ObjectName: s.Name, DDL: true}
	case *engine.CreateView:
		return Analysis{Kind: KindCreateView, ObjectName: s.Name, DDL: true}
	case *engine.DropView:
		return Analysis{Kind: KindDropView, ObjectName: s.Name, DDL: true}
	case *engine.CreateMaterializedView:
		return Analysis{Kind: KindCreateMaterializedView, ObjectName: s.Name, DDL: true}
	case *engine.DropMaterializedView:
		return Analysis{Kind: KindDropMaterializedView, ObjectName: s.Name, DDL: true}
	case *engine.RefreshMaterializedView:
		return Analysis{Kind: KindRefreshMaterialized, ObjectName: s.Name, Mutation: true}
	case *engine.AlterViewMaterialize:
		return Analysis{Kind: KindAlter, ObjectName: s.Name, DDL: true}
	case *engine.AlterMaterializedViewToView:
		return Analysis{Kind: KindAlter, ObjectName: s.Name, DDL: true}
	case *engine.CreateTrigger:
		return Analysis{Kind: KindCreateTrigger, ObjectName: s.Name, DDL: true}
	case *engine.DropTrigger:
		return Analysis{Kind: KindDropTrigger, ObjectName: s.Name, DDL: true}
	case *engine.CreateJob:
		return Analysis{Kind: KindCreateJob, ObjectName: s.Name, DDL: true, Job: true}
	case *engine.AlterJob:
		return Analysis{Kind: KindAlterJob, ObjectName: s.Name, DDL: true, Job: true}
	case *engine.DropJob:
		return Analysis{Kind: KindDropJob, ObjectName: s.Name, DDL: true, Job: true}
	default:
		return Analysis{Kind: KindUnknown}
	}
}

// IsReadOnly returns true when sql parses as a read-only statement.
func IsReadOnly(sql string) bool {
	a, err := Analyze(sql)
	return err == nil && a.ReadOnly
}

// IsResultProducing returns true for statements expected to return rows.
func IsResultProducing(sql string) bool {
	a, err := Analyze(sql)
	if err == nil {
		return a.ResultProducing
	}
	upper := strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "EXPLAIN")
}
