// Row plumbing: turning a stored table into row maps, merging rows for joins,
// building group and DISTINCT keys, and comparing rows.
package engine

import (
	"sort"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func rowsFromTable(t *storage.Table, alias string) ([]Row, []string) {
	numCols := len(t.Cols)
	// Pre-compute lowercase qualified and unqualified column keys.
	qualKeys := make([]string, numCols)
	unqualKeys := make([]string, numCols)
	for i, c := range t.Cols {
		qualKeys[i] = strings.ToLower(alias + "." + c.Name)
		unqualKeys[i] = strings.ToLower(c.Name)
	}

	cols := make([]string, numCols)
	copy(cols, qualKeys)

	// Pre-compute which unqualified names are unique (no duplicates), and
	// whether any duplicate exists at all. This is the only thing that can
	// make inserting a column's unqualified key unsafe to do unconditionally
	// (a qualified key like "t.name" can't collide with an unqualified key
	// for a different, uniquely-named column). Real schemas never have
	// duplicate column names, so computing this once per query — instead of
	// re-checking "does this key already exist" on every single row below —
	// turns the common case into one map assignment per column instead of a
	// map lookup plus a conditional assignment.
	unqualSeen := make(map[string]bool, numCols)
	firstOccurrence := make([]bool, numCols)
	hasDup := false
	for i, k := range unqualKeys {
		if unqualSeen[k] {
			hasDup = true
			continue
		}
		unqualSeen[k] = true
		firstOccurrence[i] = true
	}
	// Total keys per row: qualified + unique unqualified.
	keysPerRow := numCols + len(unqualSeen)

	out := make([]Row, len(t.Rows))
	if !hasDup {
		for ri, r := range t.Rows {
			row := make(Row, keysPerRow)
			for i := range t.Cols {
				v := r[i]
				row[qualKeys[i]] = v
				row[unqualKeys[i]] = v
			}
			out[ri] = row
		}
		return out, cols
	}

	// Slow path: at least one duplicate unqualified column name exists, so
	// the first occurrence (in column order) must win — mirrors the
	// pre-optimization behavior exactly.
	for ri, r := range t.Rows {
		row := make(Row, keysPerRow)
		for i := range t.Cols {
			row[qualKeys[i]] = r[i]
		}
		for i := range t.Cols {
			if firstOccurrence[i] {
				row[unqualKeys[i]] = r[i]
			}
		}
		out[ri] = row
	}
	return out, cols
}

func keysOfRow(r Row) []string {
	var ks []string
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func aliasOr(f FromItem) string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Table
}

func mergeRows(l, r Row) Row {
	m := make(Row, len(l)+len(r))
	for k, v := range l {
		m[k] = v
	}
	for k, v := range r {
		m[k] = v
	}
	return m
}

func cloneRow(r Row) Row {
	m := make(Row, len(r))
	for k, v := range r {
		m[k] = v
	}
	return m
}

// addRightNulls fills in NULLs for the right side of an unmatched (or
// possibly-matched, see below) row's columns.
//
// Both the qualified ("alias.col") and unqualified ("col") keys are guarded
// by an existence check. The unqualified guard was already here; the
// qualified one was missing, which mattered for exactly one caller:
// processLeftJoin's >500-row branch calls this on every row the hash join
// produced, including rows that matched and already carry the right table's
// real values under their qualified keys — an unconditional write there
// silently overwrote a correct match with NULL. The other callers
// (processLeftJoin's small-table fallback, processFullOuterJoin) only ever
// call this on rows confirmed unmatched, where the qualified key was never
// set, so the guard is a no-op for them — same observable behavior, just
// expressed uniformly instead of relying on each call site to only invoke
// this function when it's already safe to do so unconditionally.
func addRightNulls(m Row, alias string, t *storage.Table) {
	for _, c := range t.Cols {
		if _, ex := m[strings.ToLower(alias+"."+c.Name)]; !ex {
			putVal(m, alias+"."+c.Name, nil)
		}
		if _, ex := m[strings.ToLower(c.Name)]; !ex {
			putVal(m, c.Name, nil)
		}
	}
}

// addLeftNulls fills in NULLs for the left side of an unmatched right-outer
// row (processRightJoin, processFullOuterJoin's right-only pass), guarded by
// an existence check for the same reason addRightNulls needs one: leftKeys
// is a sample of key names taken from an arbitrary left row (to learn what
// the left side's columns are called, not because it corresponds to this
// particular right row), and it always includes the left table's unqualified
// column names. When the right table shares an unqualified column name with
// the left table (the common case that makes the join's bare column names
// ambiguous in the first place), m — the unmatched right row — already
// carries its own real value under that same unqualified key. Nulling it
// unconditionally replaced that real value with NULL for every row with no
// left match, deterministically (not depending on map iteration order, since
// this loop iterates the fixed leftKeys slice rather than a row map) but
// still wrongly.
func addLeftNulls(m Row, leftKeys []string) {
	for _, k := range leftKeys {
		if _, ex := m[k]; !ex {
			m[k] = nil
		}
	}
}

func fmtKeyPart(v any) string {
	return string(writeFmtKeyPart(make([]byte, 0, 32), v))
}

func comparableKeyPart(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case int:
		return x
	case int64:
		return x
	case float64:
		return x
	case bool:
		return x
	case string:
		return x
	default:
		return fmtKeyPart(v)
	}
}

// writeFmtKeyPart appends a typed, self-delimiting key part to buf and
// returns the extended slice, following the standard Go "AppendX" pattern
// (like strconv.AppendInt) instead of writing through a *strings.Builder.
// Self-delimiting text and JSON payloads are essential for composite keys: a
// user string may contain the separator used by GROUP BY, DISTINCT, or
// set-operation callers.
//
// Callers building a composite key across many rows (GROUP BY, PIVOT,
// DISTINCT) should reuse one []byte across rows via `buf = buf[:0]` before
// each row, not `var buf strings.Builder; buf.Reset()` — Builder.Reset nils
// its internal buffer, so every row's first write starts from zero capacity
// and must reallocate from scratch; slicing to buf[:0] keeps the backing
// array and lets append reuse it once row-to-row key sizes stabilize. Pair
// that with a zero-allocation map lookup via `m[string(buf)]` (the Go
// compiler special-cases a map index/comprehension whose key is a
// string-conversion of a []byte to skip the conversion's allocation when the
// result is only read, not stored) and materialize a real, independently-
// owned string via `string(buf)` only on the rarer "first time seeing this
// group" path — turning per-row key-string allocation into per-distinct-
// group allocation.
func writeFmtKeyPart(buf []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(buf, "N;"...)
	case int:
		buf = append(buf, 'I')
		buf = strconv.AppendInt(buf, int64(x), 10)
		return append(buf, ';')
	case int64:
		// 'L' ("long"), distinct from int's 'I' prefix: int and int64 must
		// remain distinct keys even when their numeric values are equal (see
		// writeSingleGroupKey's doc comment, which used to be the only place
		// this case was handled). Without this case here, every multi-column
		// GROUP BY/PIVOT/DISTINCT key builder (executeSimpleMultiGroupAggregate,
		// processAggregateQuery, processPivot, distinctRows, set-op dedup — every
		// caller of this function except the single-column GROUP BY fast path)
		// fell through to the default branch below and paid a full JSON marshal
		// per row for an int64-typed grouping column, instead of this 2-line
		// integer append.
		buf = append(buf, 'L')
		buf = strconv.AppendInt(buf, x, 10)
		return append(buf, ';')
	case float64:
		buf = append(buf, 'F')
		buf = strconv.AppendFloat(buf, x, 'g', -1, 64)
		return append(buf, ';')
	case bool:
		if x {
			return append(buf, "B1;"...)
		}
		return append(buf, "B0;"...)
	case string:
		buf = append(buf, 'S')
		buf = strconv.AppendInt(buf, int64(len(x)), 10)
		buf = append(buf, ':')
		return append(buf, x...)
	default:
		byt, _ := storage.JSONMarshal(x)
		buf = append(buf, 'J')
		buf = strconv.AppendInt(buf, int64(len(byt)), 10)
		buf = append(buf, ':')
		return append(buf, byt...)
	}
}

// writeSingleGroupKey returns a stable, typed key for the one-column GROUP BY
// fast path. Its type distinctions match comparableKeyPart, which this path
// used before it switched from map[any] to map[string]. In particular, int
// and int64 remain distinct keys even when their numeric values are equal —
// now guaranteed directly by writeFmtKeyPart, which this delegates to
// entirely.
func writeSingleGroupKey(buf []byte, v any) []byte {
	return writeFmtKeyPart(buf, v)
}

func distinctRows(rows []Row, cols []string) []Row {
	seen := make(map[string]bool, len(rows)/2)
	out := make([]Row, 0, len(rows))
	// Pre-lowercase column names once.
	lcCols := make([]string, len(cols))
	for i, c := range cols {
		lcCols[i] = strings.ToLower(c)
	}
	// buf is reused across rows via buf[:0] — see the writeFmtKeyPart doc
	// comment. seen[string(buf)] is a zero-allocation lookup; a real string
	// is only materialized for a row that is actually distinct so far.
	buf := make([]byte, 0, 64)
	for _, r := range rows {
		buf = buf[:0]
		for i, c := range lcCols {
			if i > 0 {
				buf = append(buf, '|')
			}
			buf = writeFmtKeyPart(buf, r[c])
		}
		if seen[string(buf)] {
			continue
		}
		seen[string(buf)] = true
		out = append(out, r)
	}
	return out
}
