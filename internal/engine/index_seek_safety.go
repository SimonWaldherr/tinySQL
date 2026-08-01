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

// numericColumnProfile records, per column, whether any row holds a float64.
type numericColumnProfile struct {
	table    *storage.Table
	version  int
	hasFloat []bool
}

// numericProfileMaxEntries bounds the cache. Each entry pins its *storage.Table,
// so as with the vector and FTS caches an unbounded map would retain one table
// per orphaned key.
const numericProfileMaxEntries = 256

var (
	numericProfileMu    sync.RWMutex
	numericProfileCache = make(map[string]numericColumnProfile)
)

// numericColumnHasFloat reports whether any row stores a float64 in colPos.
func numericColumnHasFloat(table *storage.Table, colPos int) bool {
	key := table.Name

	numericProfileMu.RLock()
	entry, ok := numericProfileCache[key]
	numericProfileMu.RUnlock()
	if !ok || entry.table != table || entry.version != table.Version {
		entry = buildNumericColumnProfile(table)
		numericProfileMu.Lock()
		if _, exists := numericProfileCache[key]; !exists {
			evictOverCap(numericProfileCache, numericProfileMaxEntries)
		}
		numericProfileCache[key] = entry
		numericProfileMu.Unlock()
	}
	if colPos < 0 || colPos >= len(entry.hasFloat) {
		// Unknown column: report "may hold floats" so the caller takes the exact
		// per-value path rather than trusting an absent summary.
		return true
	}
	return entry.hasFloat[colPos]
}

// buildNumericColumnProfile scans the table once, recording every column that
// holds at least one float64.
func buildNumericColumnProfile(table *storage.Table) numericColumnProfile {
	hasFloat := make([]bool, len(table.Cols))
	remaining := len(table.Cols)
	for _, row := range table.Rows {
		for i := 0; i < len(row) && i < len(hasFloat); i++ {
			if hasFloat[i] {
				continue
			}
			if _, isFloat := row[i].(float64); isFloat {
				hasFloat[i] = true
				remaining--
			}
		}
		// Every column has already been shown to hold a float; nothing more to
		// learn from the remaining rows.
		if remaining == 0 {
			break
		}
	}
	return numericColumnProfile{table: table, version: table.Version, hasFloat: hasFloat}
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
