package engine

import (
	"sort"
	"strings"
)

// emptyProjectionColumns preserves the schema of an empty general SELECT.
// In particular, a view or CTE must not lose its columns when its last row
// disappears. Match the general projection's sorted, unqualified star names.
func emptyProjectionColumns(env ExecEnv, s *Select) ([]string, error) {
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
			names, err := emptySourceColumns(env, source)
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

func emptySourceColumns(env ExecEnv, source FromItem) ([]string, error) {
	if source.Subquery != nil {
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
