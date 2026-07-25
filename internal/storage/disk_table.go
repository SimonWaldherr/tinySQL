// Converting one table to and from its on-disk form, including the row ranges
// and row sets the write-ahead log writes as deltas.
package storage

import (
	"encoding/json"
	"sort"
)

func tableToDisk(tn string, t *Table) diskTable {
	return tableToDiskRange(tn, t, 0, len(t.Rows))
}

// tableToDiskRange serializes the table schema and rows in [from, to).
// Used by the WAL to write only newly appended rows.
func tableToDiskRange(tn string, t *Table, from, to int) diskTable {
	if from < 0 {
		from = 0
	}
	if to > len(t.Rows) {
		to = len(t.Rows)
	}
	dt := diskTable{
		Tenant:  tn,
		Name:    t.Name,
		IsTemp:  t.IsTemp,
		Version: t.Version,
		Cols:    make([]diskColumn, len(t.Cols)),
		Rows:    make([][]any, to-from),
		Indexes: cloneSecondaryIndexes(t.Indexes),
		Stats:   cloneTableStats(t.Stats),
	}
	for i, c := range t.Cols {
		dt.Cols[i] = diskColumn(c)
	}
	for i := from; i < to; i++ {
		r := t.Rows[i]
		row := make([]any, len(r))
		for j, v := range r {
			if v == nil {
				row[j] = nil
				continue
			}
			if t.Cols[j].Type == JsonType {
				switch vv := v.(type) {
				case string:
					row[j] = vv
				default:
					b, _ := JSONMarshal(v)
					row[j] = string(b)
				}
			} else {
				row[j] = v
			}
		}
		dt.Rows[i-from] = row
	}
	return dt
}

// tableToDiskRows serializes the schema plus only the rows at the given
// indices, and returns those indices in the same order as the serialized rows
// so a replay can put each one back where it belongs. Indices are sorted and
// deduplicated, and any that no longer address a row are dropped.
func tableToDiskRows(tn string, t *Table, indexes []int) (diskTable, []int) {
	wanted := append([]int(nil), indexes...)
	sort.Ints(wanted)
	kept := wanted[:0]
	prev := -1
	for _, idx := range wanted {
		if idx == prev || idx < 0 || idx >= len(t.Rows) {
			continue
		}
		kept = append(kept, idx)
		prev = idx
	}

	dt := diskTable{
		Tenant:  tn,
		Name:    t.Name,
		IsTemp:  t.IsTemp,
		Version: t.Version,
		Cols:    make([]diskColumn, len(t.Cols)),
		Rows:    make([][]any, 0, len(kept)),
		Indexes: cloneSecondaryIndexes(t.Indexes),
		Stats:   cloneTableStats(t.Stats),
	}
	for i, c := range t.Cols {
		dt.Cols[i] = diskColumn(c)
	}
	for _, idx := range kept {
		dt.Rows = append(dt.Rows, diskRowFromTable(t, t.Rows[idx]))
	}
	return dt, kept
}

// diskRowFromTable converts one row to its on-disk representation, applying the
// same JSON normalization tableToDiskRange does.
func diskRowFromTable(t *Table, r []any) []any {
	row := make([]any, len(r))
	for j, v := range r {
		if v == nil {
			row[j] = nil
			continue
		}
		if j < len(t.Cols) && t.Cols[j].Type == JsonType {
			switch vv := v.(type) {
			case string:
				row[j] = vv
			default:
				b, _ := JSONMarshal(v)
				row[j] = string(b)
			}
		} else {
			row[j] = v
		}
	}
	return row
}

// normalizeVectorValue coerces a decoded vector cell back into []float64.
// GOB round-trips []float64 exactly; JSON round-trips it as []any (each
// element a float64, or a json.Number if a decoder used UseNumber()).
func normalizeVectorValue(v any) any {
	switch vv := v.(type) {
	case []float64:
		return vv
	case []any:
		out := make([]float64, len(vv))
		for i, e := range vv {
			switch n := e.(type) {
			case float64:
				out[i] = n
			case json.Number:
				f, _ := n.Float64()
				out[i] = f
			case int:
				out[i] = float64(n)
			case int64:
				out[i] = float64(n)
			}
		}
		return out
	default:
		return v
	}
}

func diskToTable(dt diskTable) *Table {
	cols := make([]Column, len(dt.Cols))
	for i, c := range dt.Cols {
		cols[i] = Column(c)
	}
	t := NewTable(dt.Name, cols, dt.IsTemp)
	t.Version = dt.Version
	t.Indexes = cloneSecondaryIndexes(dt.Indexes)
	t.Stats = cloneTableStats(dt.Stats)
	t.Rows = make([][]any, len(dt.Rows))
	for ri, r := range dt.Rows {
		row := make([]any, len(r))
		for ci, v := range r {
			if ci >= len(cols) {
				break // Skip extra columns beyond schema
			}
			if v == nil {
				row[ci] = nil
				continue
			}
			switch cols[ci].Type {
			case JsonType:
				var anyv any
				switch val := v.(type) {
				case string:
					if json.Unmarshal([]byte(val), &anyv) == nil {
						row[ci] = anyv
					} else {
						row[ci] = val
					}
				default:
					row[ci] = val
				}
			case VectorType:
				// GOB preserves []float64 exactly; JSON-based backends decode
				// a JSON number array into []any (each element boxed as
				// float64). Normalize both to []float64 so vector functions
				// (which type-switch on []float64) work regardless of the
				// backend that produced this row.
				row[ci] = normalizeVectorValue(v)
			default:
				row[ci] = v
			}
		}
		t.Rows[ri] = row
	}
	return t
}
