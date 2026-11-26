package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func canProjectVecSearch(env ExecEnv, s *Select, fn TableFunction) bool {
	switch fn.(type) {
	case *VecSearchTableFunc, *VecTopKTableFunc:
	default:
		return false
	}
	if env.applyOuter != nil || len(s.Joins) != 0 || s.Where != nil || s.Pivot != nil ||
		s.Having != nil || len(s.GroupBy) != 0 || len(s.DistinctOn) != 0 || s.Union != nil {
		return false
	}
	for _, item := range s.Projs {
		if _, ok := item.Expr.(*VarRef); !ok || item.Star {
			return false
		}
	}
	return len(s.Projs) != 0
}

// resolveProjectedVecSearch serves explicit column lists, so even empty
// results have a known output schema. Reuse the same hits if name resolution
// needs the complete rows, for example to suggest a misspelled column.
func resolveProjectedVecSearch(env ExecEnv, s *Select, fn TableFunction) ([]Row, error) {
	args, err := vecParseArgs(env, s.From.TableFunc.Args, nil)
	if err != nil {
		return nil, err
	}
	allowConcurrent := env.vectorReadRoot == s && len(s.CTEs) == 0
	if allowConcurrent {
		for _, arg := range s.From.TableFunc.Args {
			if _, ok := arg.(*Literal); !ok {
				allowConcurrent = false
				break
			}
		}
	}
	table, hits, err := findVecSearchCandidates(env.ctx, env, args, allowConcurrent)
	if err != nil {
		return nil, err
	}
	if rows := projectVecSource(table, hits, args.metric, s); rows != nil {
		return rows, nil
	}
	return aliasRetrievalRows(fn, materializeVecCandidates(table, hits, args.metric), s.From.Alias), nil
}

type vecSourceColumn struct {
	key   string
	index int
}

const (
	vecSourceDistance = -1 - iota
	vecSourceSimilarity
	vecSourceRank
)

func projectVecSource(table *storage.Table, hits []vecScoredRow, metric string, s *Select) []Row {
	// A stored dotted column can collide with a name supplied by source
	// qualification. Use the general resolver for those schemas.
	for _, col := range table.Cols {
		if strings.Contains(col.Name, ".") {
			return nil
		}
	}
	var qualifier string
	if s.From.Alias != "" {
		qualifier = strings.ToLower(s.From.Alias) + "."
	}
	var scratch [8]vecSourceColumn
	cols := scratch[:0]
	bind := func(key string) bool {
		for _, col := range cols {
			if col.key == key {
				return true
			}
		}
		name := strings.TrimPrefix(key, qualifier)
		var index int
		switch name {
		case "_vec_distance":
			index = vecSourceDistance
		case "_vec_similarity":
			index = vecSourceSimilarity
		case "_vec_rank":
			index = vecSourceRank
		default:
			var err error
			index, err = table.ColIndex(name)
			if err != nil || index >= len(table.Cols) || strings.ToLower(table.Cols[index].Name) != name {
				return false
			}
		}
		cols = append(cols, vecSourceColumn{key: key, index: index})
		return true
	}
	for _, item := range s.Projs {
		ref := item.Expr.(*VarRef)
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		if !bind(key) {
			return nil
		}
	}
	// Output names take precedence over ORDER BY keys from the source.
orderKeys:
	for _, item := range s.OrderBy {
		key := strings.ToLower(item.Col)
		for i, projection := range s.Projs {
			if strings.ToLower(projName(projection, i)) == key {
				continue orderKeys
			}
		}
		if !bind(key) {
			return nil
		}
	}

	rows := make([]Row, 0, len(hits))
	for _, hit := range hits {
		if hit.rowIdx < 0 || hit.rowIdx >= len(table.Rows) {
			continue
		}
		source := table.Rows[hit.rowIdx]
		row := make(Row, len(cols))
		for _, col := range cols {
			switch col.index {
			case vecSourceDistance:
				row[col.key] = hit.distance
			case vecSourceSimilarity:
				row[col.key] = vecSimilarityFromDistance(metric, hit.distance)
			case vecSourceRank:
				row[col.key] = len(rows) + 1
			default:
				if col.index >= len(source) {
					// Keep the normal missing-column error and suggestions.
					return nil
				}
				row[col.key] = source[col.index]
			}
		}
		rows = append(rows, row)
	}
	return rows
}
