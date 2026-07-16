package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSelectDependenciesTraversesNestedQueryForms(t *testing.T) {
	selectFrom := func(table string) *Select {
		return &Select{From: FromItem{Table: table}}
	}
	query := &Select{
		CTEs: []CTE{{Name: "recent", Select: selectFrom("cte_source")}},
		From: FromItem{Subquery: selectFrom("derived_source")},
		Joins: []JoinClause{{
			Right: FromItem{Table: "joined_source"},
			On:    &ExistsExpr{Select: selectFrom("join_condition_source")},
		}},
		Projs: []SelectItem{{Expr: &FuncCall{Args: []Expr{
			&InExpr{Expr: newVarRef("id"), Values: []Expr{&SubqueryExpr{Select: selectFrom("in_source")}}},
			&LikeExpr{Expr: newVarRef("name"), Pattern: &Literal{Val: "%x%"}, Escape: &Literal{Val: "\\"}},
			&RegexpExpr{Expr: newVarRef("name"), Pattern: &Literal{Val: "x"}},
			&BetweenExpr{Expr: newVarRef("id"), Lo: &Literal{Val: 1}, Hi: &Literal{Val: 9}},
		}}}},
		Where: &CaseExpr{Whens: []CaseWhen{{
			When: &ExistsExpr{Select: selectFrom("case_when_source")},
			Then: &SubqueryExpr{Select: selectFrom("case_then_source")},
		}}, Else: &Unary{Expr: &IsNull{Expr: newVarRef("deleted_at")}}},
		GroupBy: []Expr{&SubqueryExpr{Select: selectFrom("group_source")}},
		Having:  &ExistsExpr{Select: selectFrom("having_source")},
		Union: &UnionClause{
			Right: selectFrom("union_first_source"),
			Next:  &UnionClause{Right: selectFrom("union_second_source")},
		},
	}

	deps := selectDependencies(nil, "main", "report", "VIEW", query)
	got := make(map[string]storage.CatalogDependency, len(deps))
	for _, dep := range deps {
		got[dep.DependsOnName] = dep
	}
	wantNames := []string{
		"cte_source", "derived_source", "joined_source", "join_condition_source",
		"in_source", "case_when_source", "case_then_source", "group_source",
		"having_source", "union_first_source", "union_second_source",
	}
	if len(got) != len(wantNames) {
		t.Fatalf("dependencies = %#v, want %d entries", deps, len(wantNames))
	}
	for _, name := range wantNames {
		dep, ok := got[name]
		if !ok {
			t.Errorf("missing dependency %q in %#v", name, deps)
			continue
		}
		if dep.Schema != "main" || dep.ObjectName != "report" || dep.ObjectType != "VIEW" || dep.DependsOnType != "UNKNOWN" {
			t.Errorf("dependency %q = %#v", name, dep)
		}
	}
}

func TestAddDependencySkipsCTEsInternalAndCatalogTables(t *testing.T) {
	seen := make(map[string]storage.CatalogDependency)
	ctes := map[string]bool{"local_cte": true}
	for _, table := range []string{"local_cte", "__mv_cache", "sys.tables", "catalog.views", "real_table"} {
		addDependency(nil, table, ctes, seen)
	}
	if len(seen) != 1 {
		t.Fatalf("dependencies = %#v, want only real_table", seen)
	}
	if _, ok := seen["main.real_table"]; !ok {
		t.Fatalf("real_table dependency missing: %#v", seen)
	}
}

func TestDependencyObjectTypesAndIgnoredFromItems(t *testing.T) {
	cat := storage.NewCatalogManager()
	if err := cat.RegisterView("main", "saved_view", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := cat.RegisterMaterializedView(&storage.CatalogMaterializedView{Name: "cached_view"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		want string
	}{
		{"saved_view", "VIEW"},
		{"cached_view", "MATERIALIZED_VIEW"},
		{"plain_table", "TABLE"},
	} {
		if got := dependencyObjectType(cat, "main", tc.name); got != tc.want {
			t.Errorf("dependencyObjectType(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}

	seen := make(map[string]storage.CatalogDependency)
	collectFromDependency(cat, FromItem{}, nil, seen)
	collectFromDependency(cat, FromItem{Table: "FILES", TableFunc: &TableFuncCall{}}, nil, seen)
	if len(seen) != 0 {
		t.Fatalf("ignored FROM items created dependencies: %#v", seen)
	}
}
