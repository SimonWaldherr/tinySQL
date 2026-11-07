package engine

import "strings"

type cteSelectProjection struct {
	source string
	name   string
	key    string
}

// Select from immutable materialized rows before allocating projection maps.
// Only direct columns reach this path: expressions, windows and DISTINCT keep
// their existing evaluation order in the general executor.
func executeOrderedCTESelect(env ExecEnv, s *Select, source *ResultSet, projections []cteSelectProjection, columns []string, resolve func(string) (string, bool), filter rowWhereTriFilter) (*ResultSet, bool, error) {
	// Output aliases win over source names. Assignment in projection order also
	// preserves the last value when output names coincide case-insensitively.
	aliases := make(map[string]string, len(projections))
	for _, p := range projections {
		aliases[p.key] = p.source
	}
	order := make([]OrderItem, len(s.OrderBy))
	orderKeys := make([]string, len(order))
	for i, item := range s.OrderBy {
		key := strings.ToLower(item.Col)
		orderKeys[i] = key
		column, projected := aliases[key]
		if !projected {
			var valid bool
			column, valid = resolve(key)
			if !valid {
				return nil, false, nil
			}
		}
		order[i] = item
		order[i].Col = column
	}

	rows := source.Rows
	if filter != nil {
		rows = make([]Row, 0, len(source.Rows))
		for i, row := range source.Rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, true, err
				}
			}
			tri, err := filter(row)
			if err != nil {
				// Preserve diagnostics derived from the general qualified maps.
				return nil, false, nil
			}
			if tri == tvTrue {
				rows = append(rows, row)
			}
		}
	}
	// The general executor evaluates WHERE completely, then projects every
	// match, even those later discarded by LIMIT/OFFSET. Keep missing-column
	// errors observable and let its qualified maps supply the same diagnostics.
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		for _, p := range projections {
			if _, exists := row[p.source]; !exists {
				return nil, false, nil
			}
		}
	}
	if len(rows) > 0 {
		for _, item := range order {
			if _, exists := rows[0][item.Col]; !exists {
				return nil, false, nil
			}
		}
	}
	// The unlimited sorter reorders its argument in place. A filtered slice
	// already owns its headers; otherwise copy them to protect sibling CTE uses.
	if filter == nil && s.Limit == nil && s.Offset == nil {
		rows = append([]Row(nil), rows...)
	}
	rows = applySortOrderWithLimit(order, rows, s.Limit, s.Offset)
	rows = applyOffsetLimit(s, rows)
	out := make([]Row, len(rows))
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		projected := make(Row, len(projections)+len(order))
		for _, p := range projections {
			projected[p.key] = row[p.source]
		}
		// Hidden ordering keys are part of the existing Row map contract, but
		// remain absent from ResultSet.Cols and positional client results.
		for j, key := range orderKeys {
			if _, exists := projected[key]; !exists {
				if value, exists := row[order[j].Col]; exists {
					projected[key] = value
				}
			}
		}
		out[i] = projected
	}
	return &ResultSet{Cols: columns, Rows: out}, true, nil
}
