package engine

import (
	"fmt"
	"sort"
	"strings"
)

// emptyProjectionColumns preserves the schema of an empty general SELECT.
// In particular, a view or CTE must not lose its columns when its last row
// disappears. Match the general projection's sorted, unqualified star names.
func emptyProjectionColumns(env ExecEnv, s *Select) ([]string, error) {
	return selectProjectionColumns(env, s, false)
}

func selectProjectionColumns(env ExecEnv, s *Select, schemaOnly bool) ([]string, error) {
	var cols []string
	seen := make(map[string]bool)
	for i, p := range s.Projs {
		if !p.Star {
			name := projName(p, i)
			if !seen[name] {
				seen[name] = true
				cols = append(cols, name)
			}
			continue
		}
		sources := []FromItem{s.From}
		for _, join := range s.Joins {
			sources = append(sources, join.Right)
		}
		var star []string
		for _, source := range sources {
			names, err := emptySourceColumns(env, source, schemaOnly)
			if err != nil {
				return nil, err
			}
			for _, name := range names {
				name = strings.ToLower(name)
				if dot := strings.LastIndex(name, "."); dot >= 0 {
					name = name[dot+1:]
				}
				if !seen[name] {
					seen[name] = true
					star = append(star, name)
				}
			}
		}
		sort.Strings(star)
		cols = append(cols, star...)
	}
	return cols, nil
}

func emptySourceColumns(env ExecEnv, source FromItem, schemaOnly bool) ([]string, error) {
	if source.TableFunc != nil {
		return env.tableFuncColumns[source.TableFunc], nil
	}
	if source.Subquery != nil {
		if cols, ok := env.applyColumns[source.Subquery]; ok {
			return cols, nil
		}
		if schemaOnly {
			return selectProjectionColumns(env, source.Subquery, true)
		}
		rs, err := executeSelect(env, source.Subquery)
		if err != nil {
			return nil, err
		}
		return rs.Cols, nil
	}
	if rs, ok := env.ctes[strings.ToLower(source.Table)]; ok {
		return rs.Cols, nil
	}
	if source.Table != "" {
		if table, err := env.db.Get(env.tenant, source.Table); err == nil {
			cols := make([]string, len(table.Cols))
			for i, col := range table.Cols {
				cols[i] = col.Name
			}
			return cols, nil
		}
		if schemaOnly {
			return emptyViewColumns(env, source.Table)
		}
		if rs, found, err := resolveViewResult(env, source); found || err != nil {
			if err != nil {
				return nil, err
			}
			return rs.Cols, nil
		}
	}
	// Sources with dynamic schemas keep their existing empty-result behavior.
	return nil, nil
}

// Infer the columns of an unevaluated APPLY source without running its view
// or refreshing a materialized view on an empty left input.
func emptyViewColumns(env ExecEnv, table string) ([]string, error) {
	schema, name := splitObjectName(table)
	var sql string
	if view, ok := env.db.Catalog().GetView(schema, name); ok {
		sql = view.SQLText
	} else if view, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
		sql = view.SQLText
	} else {
		return nil, nil
	}
	if env.viewDepth >= 16 {
		return nil, fmt.Errorf("view expansion exceeded depth limit")
	}
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("view %q parse failed: %w", name, err)
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return nil, fmt.Errorf("view %q query is not a SELECT", name)
	}
	env.viewDepth++
	env.ctes = nil
	return selectProjectionColumns(env, sel, true)
}
