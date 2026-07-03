package engine

import (
	"fmt"
	"strings"
)

func executeExplain(env ExecEnv, s *Explain) (*ResultSet, error) {
	rows := make([]Row, 0, 8)
	addExplainStep(&rows, "PLAN", statementName(s.Statement))
	explainStatement(env, &rows, s.Statement)
	for i, row := range rows {
		row["step"] = i + 1
	}
	return &ResultSet{Cols: []string{"step", "operation", "detail"}, Rows: rows}, nil
}

func explainStatement(env ExecEnv, rows *[]Row, stmt Statement) {
	switch q := stmt.(type) {
	case *Select:
		explainSelect(env, rows, q, "")
	case *Insert:
		addExplainStep(rows, "INSERT", q.Table)
		if len(q.Returning) > 0 {
			addExplainStep(rows, "RETURNING", fmt.Sprintf("%d projection(s)", len(q.Returning)))
		}
	case *Update:
		addExplainStep(rows, "UPDATE", q.Table)
		addExplainStep(rows, "SET", fmt.Sprintf("%d column(s)", len(q.Sets)))
		if q.Where != nil {
			addExplainStep(rows, "FILTER", exprKind(q.Where))
		}
	case *Delete:
		addExplainStep(rows, "DELETE", q.Table)
		if q.Where != nil {
			addExplainStep(rows, "FILTER", exprKind(q.Where))
		}
	case *CreateView:
		addExplainStep(rows, "CREATE VIEW", q.Name)
		explainSelect(env, rows, q.Select, "view ")
	case *CreateMaterializedView:
		addExplainStep(rows, "CREATE MATERIALIZED VIEW", q.Name)
		if q.WithData {
			addExplainStep(rows, "MATERIALIZE", "with data")
		} else {
			addExplainStep(rows, "MATERIALIZE", "deferred")
		}
		if q.InvalidateOnChange {
			addExplainStep(rows, "INVALIDATE", "on base-object change")
		}
		explainSelect(env, rows, q.Select, "materialized view ")
	default:
		addExplainStep(rows, "EXECUTE", statementName(stmt))
	}
}

func explainSelect(env ExecEnv, rows *[]Row, sel *Select, prefix string) {
	if sel == nil {
		return
	}
	for _, cte := range sel.CTEs {
		detail := cte.Name
		if cte.Recursive {
			detail += " recursive"
		}
		addExplainStep(rows, "CTE", detail)
		explainSelect(env, rows, cte.Select, "cte ")
	}
	if sel.From.Table != "" || sel.From.Subquery != nil || sel.From.TableFunc != nil {
		explainFrom(env, rows, "SCAN", sel.From, prefix)
	}
	for _, join := range sel.Joins {
		explainFrom(env, rows, joinOperation(join.Type), join.Right, prefix)
		if join.On != nil {
			addExplainStep(rows, "JOIN FILTER", exprKind(join.On))
		}
	}
	if sel.Where != nil {
		addExplainStep(rows, "FILTER", exprKind(sel.Where))
	}
	if len(sel.GroupBy) > 0 {
		addExplainStep(rows, "GROUP", fmt.Sprintf("%d expression(s)", len(sel.GroupBy)))
	}
	if sel.Having != nil {
		addExplainStep(rows, "HAVING", exprKind(sel.Having))
	}
	if sel.Distinct {
		addExplainStep(rows, "DISTINCT", "all projected columns")
	}
	if len(sel.DistinctOn) > 0 {
		addExplainStep(rows, "DISTINCT ON", fmt.Sprintf("%d expression(s)", len(sel.DistinctOn)))
	}
	if len(sel.OrderBy) > 0 {
		addExplainStep(rows, "SORT", fmt.Sprintf("%d column(s)", len(sel.OrderBy)))
	}
	if sel.Limit != nil {
		addExplainStep(rows, "LIMIT", fmt.Sprintf("%d", *sel.Limit))
	}
	if sel.Offset != nil {
		addExplainStep(rows, "OFFSET", fmt.Sprintf("%d", *sel.Offset))
	}
	if sel.Union != nil {
		explainUnion(env, rows, sel.Union)
	}
	if len(sel.Projs) > 0 {
		addExplainStep(rows, "PROJECT", fmt.Sprintf("%d column(s)", len(sel.Projs)))
	}
}

func explainFrom(env ExecEnv, rows *[]Row, op string, from FromItem, prefix string) {
	if from.Subquery != nil {
		detail := strings.TrimSpace(prefix + "subquery")
		if from.Alias != "" {
			detail += " as " + from.Alias
		}
		addExplainStep(rows, op, detail)
		explainSelect(env, rows, from.Subquery, "derived ")
		return
	}
	if from.TableFunc != nil {
		detail := strings.TrimSpace(prefix + "function " + from.TableFunc.Name)
		if from.Alias != "" {
			detail += " as " + from.Alias
		}
		addExplainStep(rows, op, detail)
		return
	}
	detail := strings.TrimSpace(prefix + from.Table)
	if from.Alias != "" {
		detail += " as " + from.Alias
	}
	schema, name := splitObjectName(from.Table)
	if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
		if mv.CacheTableName != "" {
			detail += " using cache " + mv.CacheTableName
		}
	}
	addExplainStep(rows, op, detail)
}

func explainUnion(env ExecEnv, rows *[]Row, union *UnionClause) {
	for u := union; u != nil; u = u.Next {
		addExplainStep(rows, unionOperation(u.Type), "right input")
		explainSelect(env, rows, u.Right, "set ")
	}
}

func addExplainStep(rows *[]Row, op, detail string) {
	*rows = append(*rows, Row{
		"operation": op,
		"detail":    detail,
	})
}

func statementName(stmt Statement) string {
	switch stmt.(type) {
	case *Select:
		return "SELECT"
	case *Insert:
		return "INSERT"
	case *Update:
		return "UPDATE"
	case *Delete:
		return "DELETE"
	case *CreateTable:
		return "CREATE TABLE"
	case *CreateView:
		return "CREATE VIEW"
	case *CreateMaterializedView:
		return "CREATE MATERIALIZED VIEW"
	default:
		return fmt.Sprintf("%T", stmt)
	}
}

func joinOperation(t JoinType) string {
	switch t {
	case JoinLeft:
		return "LEFT JOIN"
	case JoinRight:
		return "RIGHT JOIN"
	default:
		return "JOIN"
	}
}

func unionOperation(t UnionType) string {
	switch t {
	case UnionAll:
		return "UNION ALL"
	case Except:
		return "EXCEPT"
	case Intersect:
		return "INTERSECT"
	default:
		return "UNION"
	}
}

func exprKind(e Expr) string {
	switch ex := e.(type) {
	case *Binary:
		return ex.Op
	case *Unary:
		return ex.Op
	case *FuncCall:
		return ex.Name
	case *SubqueryExpr:
		return "SUBQUERY"
	case *CaseExpr:
		return "CASE"
	case *IsNull:
		if ex.Negate {
			return "IS NOT NULL"
		}
		return "IS NULL"
	case *InExpr:
		if ex.Negate {
			return "NOT IN"
		}
		return "IN LIST"
	case *ExistsExpr:
		return "EXISTS"
	case *LikeExpr:
		if ex.Negate {
			return "NOT LIKE"
		}
		if ex.CaseInsensitive {
			return "ILIKE"
		}
		if ex.GlobStyle {
			return "GLOB"
		}
		return "LIKE"
	case *RegexpExpr:
		if ex.Negate {
			return "NOT REGEXP"
		}
		if ex.SimilarTo {
			return "SIMILAR TO"
		}
		return "REGEXP"
	case *BetweenExpr:
		if ex.Negate {
			return "NOT BETWEEN"
		}
		return "BETWEEN"
	default:
		return fmt.Sprintf("%T", e)
	}
}
