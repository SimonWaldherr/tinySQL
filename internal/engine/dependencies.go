package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func selectDependencies(cat *storage.CatalogManager, objectSchema, objectName, objectType string, sel *Select) []storage.CatalogDependency {
	seen := make(map[string]storage.CatalogDependency)
	collectSelectDependencies(cat, sel, map[string]bool{}, seen)

	out := make([]storage.CatalogDependency, 0, len(seen))
	for _, dep := range seen {
		dep.Schema = objectSchema
		dep.ObjectName = objectName
		dep.ObjectType = objectType
		out = append(out, dep)
	}
	return out
}

func collectSelectDependencies(cat *storage.CatalogManager, sel *Select, outerCTEs map[string]bool, seen map[string]storage.CatalogDependency) {
	if sel == nil {
		return
	}
	ctes := cloneCTEMap(outerCTEs)
	for _, cte := range sel.CTEs {
		ctes[strings.ToLower(cte.Name)] = true
	}
	for _, cte := range sel.CTEs {
		collectSelectDependencies(cat, cte.Select, outerCTEs, seen)
	}

	collectFromDependency(cat, sel.From, ctes, seen)
	for _, join := range sel.Joins {
		collectFromDependency(cat, join.Right, ctes, seen)
		collectExprDependencies(cat, join.On, ctes, seen)
	}
	for _, proj := range sel.Projs {
		collectExprDependencies(cat, proj.Expr, ctes, seen)
	}
	collectExprDependencies(cat, sel.Where, ctes, seen)
	for _, expr := range sel.GroupBy {
		collectExprDependencies(cat, expr, ctes, seen)
	}
	collectExprDependencies(cat, sel.Having, ctes, seen)
	if sel.Union != nil {
		collectUnionDependencies(cat, sel.Union, ctes, seen)
	}
}

func collectUnionDependencies(cat *storage.CatalogManager, union *UnionClause, ctes map[string]bool, seen map[string]storage.CatalogDependency) {
	for current := union; current != nil; current = current.Next {
		collectSelectDependencies(cat, current.Right, ctes, seen)
	}
}

func collectFromDependency(cat *storage.CatalogManager, from FromItem, ctes map[string]bool, seen map[string]storage.CatalogDependency) {
	if from.Subquery != nil {
		collectSelectDependencies(cat, from.Subquery, ctes, seen)
		return
	}
	if from.Table == "" || from.TableFunc != nil {
		return
	}
	addDependency(cat, from.Table, ctes, seen)
}

func collectExprDependencies(cat *storage.CatalogManager, expr Expr, ctes map[string]bool, seen map[string]storage.CatalogDependency) {
	switch ex := expr.(type) {
	case nil:
		return
	case *Unary:
		collectExprDependencies(cat, ex.Expr, ctes, seen)
	case *Binary:
		collectExprDependencies(cat, ex.Left, ctes, seen)
		collectExprDependencies(cat, ex.Right, ctes, seen)
	case *IsNull:
		collectExprDependencies(cat, ex.Expr, ctes, seen)
	case *FuncCall:
		for _, arg := range ex.Args {
			collectExprDependencies(cat, arg, ctes, seen)
		}
	case *InExpr:
		collectExprDependencies(cat, ex.Expr, ctes, seen)
		for _, value := range ex.Values {
			collectExprDependencies(cat, value, ctes, seen)
		}
	case *LikeExpr:
		collectExprDependencies(cat, ex.Expr, ctes, seen)
		collectExprDependencies(cat, ex.Pattern, ctes, seen)
		collectExprDependencies(cat, ex.Escape, ctes, seen)
	case *RegexpExpr:
		collectExprDependencies(cat, ex.Expr, ctes, seen)
		collectExprDependencies(cat, ex.Pattern, ctes, seen)
	case *ExistsExpr:
		collectSelectDependencies(cat, ex.Select, ctes, seen)
	case *SubqueryExpr:
		collectSelectDependencies(cat, ex.Select, ctes, seen)
	case *CaseExpr:
		collectExprDependencies(cat, ex.Operand, ctes, seen)
		for _, when := range ex.Whens {
			collectExprDependencies(cat, when.When, ctes, seen)
			collectExprDependencies(cat, when.Then, ctes, seen)
		}
		collectExprDependencies(cat, ex.Else, ctes, seen)
	}
}

func addDependency(cat *storage.CatalogManager, table string, ctes map[string]bool, seen map[string]storage.CatalogDependency) {
	schema, name := splitObjectName(table)
	lowerName := strings.ToLower(name)
	if ctes[lowerName] || strings.HasPrefix(lowerName, "__mv_") {
		return
	}
	if schema == "sys" || schema == "catalog" {
		return
	}
	depType := dependencyObjectType(cat, schema, name)
	key := strings.ToLower(schema + "." + name)
	seen[key] = storage.CatalogDependency{
		DependsOnSchema: schema,
		DependsOnName:   name,
		DependsOnType:   depType,
		DependencyType:  "NORMAL",
	}
}

func splitObjectName(name string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(name), ".", 2)
	if len(parts) == 2 {
		return strings.ToLower(parts[0]), parts[1]
	}
	return "main", name
}

func dependencyObjectType(cat *storage.CatalogManager, schema, name string) string {
	if cat == nil {
		return "UNKNOWN"
	}
	if _, ok := cat.GetView(schema, name); ok {
		return "VIEW"
	}
	if _, ok := cat.GetMaterializedView(schema, name); ok {
		return "MATERIALIZED_VIEW"
	}
	return "TABLE"
}

func cloneCTEMap(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
