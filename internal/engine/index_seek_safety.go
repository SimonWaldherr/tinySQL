package engine

// Making a numeric secondary-index seek decision cheap.
//
// SQL compares 1 and 1.0 as equal, while the durable index encoding keeps their
// representations distinct (integers carry one type tag, float64 another). A
// seek on a numeric column is therefore only sound when no row that compares
// equal to the literal is stored under a different tag — otherwise the seek would
// miss it.
//
// numericSecondaryIndexSeekSafe established that by scanning every row, once per
// indexed column, on every query. For a composite index that is several full
// table scans just to decide whether a seek is permitted, which made an indexed
// point lookup *slower* than the scan it was meant to replace: a tile-server
// lookup over 65,536 tiles measured 4.6 ms against SQLite's 13 µs, and grew
// linearly with the tileset.
//
// The observation that fixes it: whether a column holds any float64 at all is a
// property of the column, not of the literal being sought. Cached per
// (table, table.Version) — the same invalidation every other cache in the engine
// uses — it is computed once per table version instead of once per query, and an
// integer literal against a float-free column is then decided without touching a
// row.
//
// Only that case is fast-pathed. A float literal still takes the original scan,
// because float equality has edge cases the column-level summary cannot settle:
// -0.0 and 0.0 compare equal but encode differently, so "the column contains
// floats" is not enough to prove a float seek sound.

import (
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// numericColumnProfile records, per column, which numeric encodings appear.
//
// Both are tracked because the two questions asked of it differ. A point seek
// needs "does this column hold any float64" (an integer literal is then
// unambiguous). A range seek needs the stronger "is the column uniformly one
// kind", since integers and floats carry different type tags and sort as
// separate blocks, so a mixed column has no order to walk.
type numericColumnProfile struct {
	table    *storage.Table
	version  int
	hasFloat []bool
	hasInt   []bool
}

// numericProfileMaxEntries bounds the cache. Each entry pins its *storage.Table,
// so as with the vector and FTS caches an unbounded map would retain one table
// per orphaned key.
const numericProfileMaxEntries = 256

var (
	numericProfileMu    sync.RWMutex
	numericProfileCache = make(map[string]numericColumnProfile)
)

// numericColumnProfileFor returns the cached column profile, rebuilding it when
// the table has changed.
func numericColumnProfileFor(table *storage.Table) numericColumnProfile {
	key := table.Name

	numericProfileMu.RLock()
	entry, ok := numericProfileCache[key]
	numericProfileMu.RUnlock()
	if ok && entry.table == table && entry.version == table.Version {
		return entry
	}
	entry = buildNumericColumnProfile(table)
	numericProfileMu.Lock()
	if _, exists := numericProfileCache[key]; !exists {
		evictOverCap(numericProfileCache, numericProfileMaxEntries)
	}
	numericProfileCache[key] = entry
	numericProfileMu.Unlock()
	return entry
}

// numericColumnHasFloat reports whether any row stores a float64 in colPos.
func numericColumnHasFloat(table *storage.Table, colPos int) bool {
	entry := numericColumnProfileFor(table)
	if colPos < 0 || colPos >= len(entry.hasFloat) {
		// Unknown column: report "may hold floats" so the caller takes the exact
		// per-value path rather than trusting an absent summary.
		return true
	}
	return entry.hasFloat[colPos]
}

// numericColumnIsAllFloat reports whether colPos holds float64 values and no
// integers, i.e. whether a float-keyed range walk over it is ordered. A column
// with neither kind (empty, or all NULL/text) is not all-float.
func numericColumnIsAllFloat(table *storage.Table, colPos int) bool {
	entry := numericColumnProfileFor(table)
	if colPos < 0 || colPos >= len(entry.hasFloat) || colPos >= len(entry.hasInt) {
		return false
	}
	return entry.hasFloat[colPos] && !entry.hasInt[colPos]
}

// buildNumericColumnProfile scans the table once, recording which numeric
// encodings each column holds.
func buildNumericColumnProfile(table *storage.Table) numericColumnProfile {
	hasFloat := make([]bool, len(table.Cols))
	hasInt := make([]bool, len(table.Cols))
	settled := 0 // columns where both kinds are known present: nothing left to learn
	for _, row := range table.Rows {
		for i := 0; i < len(row) && i < len(hasFloat); i++ {
			if hasFloat[i] && hasInt[i] {
				continue
			}
			switch row[i].(type) {
			case float64:
				if !hasFloat[i] {
					hasFloat[i] = true
					if hasInt[i] {
						settled++
					}
				}
			case int, int64:
				if !hasInt[i] {
					hasInt[i] = true
					if hasFloat[i] {
						settled++
					}
				}
			}
		}
		if settled == len(hasFloat) {
			break
		}
	}
	return numericColumnProfile{table: table, version: table.Version, hasFloat: hasFloat, hasInt: hasInt}
}

// purgeNumericProfilesFor drops cached column profiles for one table, called from
// DROP TABLE. Purging is always safe: a profile rebuilds on next use.
func purgeNumericProfilesFor(table string) {
	numericProfileMu.Lock()
	delete(numericProfileCache, table)
	numericProfileMu.Unlock()
}

// isIntegerSQLValue reports whether v encodes under the index's integer tag,
// which int and int64 share (see storage.appendCanonicalIndexValue).
func isIntegerSQLValue(v any) bool {
	switch v.(type) {
	case int, int64:
		return true
	default:
		return false
	}
}
