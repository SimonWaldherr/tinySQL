package main

import (
	"strconv"
	"strings"
	"time"
)

// IncrementalRow is one row read from the source during a sync pass,
// shaped for the apply stages (INSERT/UPDATE execution, stages 4/5) to
// consume without any further decoding:
//
//   - Key is the canonical row-key string, computeRowKey(KeyValues). Treat
//     it as an opaque identifier (map lookups, equality checks against
//     TableSyncState.Keys/RowHashes) — never parse it yourself; see
//     decodeRowKey below if you ever must.
//   - KeyValues holds the row's key-column values in their original Go
//     types (not canonicalized strings), in the same order as the keyCols
//     slice that produced Key. Apply stages build WHERE clauses for
//     UPDATE/DELETE from KeyValues directly — this is what lets them avoid
//     decoding Key at all for any row that is present in the current pull.
//   - Columns holds every column value for the row (key columns included),
//     in a stable, caller-defined order, ready for INSERT/UPDATE binding.
//   - Watermark is the raw watermark-column value for this row (e.g.
//     time.Time, an integer type, a float type, or a string), or nil if
//     the table has no watermark column configured. Only meaningful when
//     hasWatermark is true in the call to planIncrementalSync.
type IncrementalRow struct {
	Key       string
	KeyValues []any
	Columns   []any
	Watermark any
}

// decodeRowKey reverses computeRowKey (sync_state.go), splitting a
// canonical row-key string back into its per-column canonical parts, in
// the same order the original key columns were joined in.
//
// This exists for exactly one situation: applying a delete. A key in
// toDeleteKeys names a row that has vanished from the source, so there is
// no IncrementalRow for it in the current pass to read KeyValues from —
// the only data available is the key string itself (round-tripped through
// TableSyncState.Keys from a prior run). decodeRowKey lets the delete-apply
// stage recover per-column values (as canonicalized strings, per
// canonicalKeyPart) to build a WHERE clause in that case.
//
// For any row that IS present in the current pass (inserts/updates),
// prefer IncrementalRow.KeyValues instead — it carries the original typed
// values and needs no decoding.
//
// The round trip relies on keyPartSeparator (U+001F, the ASCII unit
// separator) never appearing inside an individual canonicalized value,
// which holds for all but pathological input data.
func decodeRowKey(key string) []string {
	return strings.Split(key, keyPartSeparator)
}

// planIncrementalSync is the pure decision core of an incremental sync
// pass for one (source, target, table) pipeline. It performs no I/O and
// touches no database — it only compares the previous persisted state
// against what the caller already fetched from the source, and reports
// what to upsert, what to delete, and the state to persist next.
//
// currentKeys is the full current key-set scan from the source: the
// caller always fetches this regardless of whether a watermark is in use,
// because it is the only way to detect deletes (a row that no longer
// appears in the source's key set). Each entry must be a canonical key
// string produced by computeRowKey, in the same form as prev.Keys and
// each IncrementalRow.Key.
//
// changedRows' meaning depends on hasWatermark:
//   - hasWatermark == true: changedRows is already pre-filtered by the
//     caller to rows whose watermark column is >= the last persisted
//     watermark. Every row in it becomes an upsert; no further filtering
//     happens here (re-upserting a row tied to the watermark boundary is
//     harmless and expected).
//   - hasWatermark == false: changedRows is the FULL current row set
//     (i.e. it corresponds 1:1 with currentKeys). Each row's content hash
//     (rowContentHash over Columns) is compared against
//     prev.RowHashes[row.Key]; only new or changed rows become upserts.
//
// toDeleteKeys is always derived the same way regardless of hasWatermark:
// keys present in prev.Keys but absent from currentKeys. It is returned
// as canonical key strings, not decomposed — see decodeRowKey and
// IncrementalRow.KeyValues for how a caller turns a key back into
// per-column values.
//
// next is the TableSyncState to persist for the following run. Its
// UpdatedAt field is intentionally left zero: this function has no wall
// clock dependence (so it stays trivially testable) and the caller is
// expected to stamp UpdatedAt = time.Now() (or similar) before saving.
func planIncrementalSync(prev *TableSyncState, currentKeys []string, changedRows []IncrementalRow, hasWatermark bool) (toUpsert []IncrementalRow, toDeleteKeys []string, next *TableSyncState) {
	if prev == nil {
		prev = &TableSyncState{}
	}

	currentSet := make(map[string]struct{}, len(currentKeys))
	for _, k := range currentKeys {
		currentSet[k] = struct{}{}
	}
	for _, k := range prev.Keys {
		if _, ok := currentSet[k]; !ok {
			toDeleteKeys = append(toDeleteKeys, k)
		}
	}

	next = &TableSyncState{
		Keys: append([]string(nil), currentKeys...),
	}

	if hasWatermark {
		toUpsert = changedRows

		best := prev.Watermark
		for _, row := range changedRows {
			if row.Watermark == nil {
				continue
			}
			wv, err := newWatermarkValue(row.Watermark)
			if err != nil {
				continue
			}
			if best == nil || compareWatermarkValues(wv, *best) > 0 {
				best = &wv
			}
		}
		next.Watermark = best

		return toUpsert, toDeleteKeys, next
	}

	nextHashes := make(map[string]string, len(changedRows))
	for _, row := range changedRows {
		hash := rowContentHash(row.Columns)
		nextHashes[row.Key] = hash

		prevHash, existed := prev.RowHashes[row.Key]
		if !existed || prevHash != hash {
			toUpsert = append(toUpsert, row)
		}
	}
	next.RowHashes = nextHashes

	return toUpsert, toDeleteKeys, next
}

// compareWatermarkValues compares two WatermarkValues, returning -1, 0, or
// 1 as a < b, a == b, or a > b. Comparison is type-aware (numeric values
// compare numerically, times compare chronologically) whenever both sides
// share a Kind and parse cleanly; it falls back to a plain string compare
// of Text otherwise (differing Kinds, or a malformed Text that fails to
// parse — which should not happen for values built by newWatermarkValue,
// but a pure comparison function must not panic or error on it).
func compareWatermarkValues(a, b WatermarkValue) int {
	if a.Kind != b.Kind {
		return strings.Compare(a.Text, b.Text)
	}

	switch a.Kind {
	case "time":
		ta, errA := time.Parse(time.RFC3339Nano, a.Text)
		tb, errB := time.Parse(time.RFC3339Nano, b.Text)
		if errA != nil || errB != nil {
			return strings.Compare(a.Text, b.Text)
		}
		switch {
		case ta.Before(tb):
			return -1
		case ta.After(tb):
			return 1
		default:
			return 0
		}
	case "int":
		na, errA := strconv.ParseInt(a.Text, 10, 64)
		nb, errB := strconv.ParseInt(b.Text, 10, 64)
		if errA != nil || errB != nil {
			return strings.Compare(a.Text, b.Text)
		}
		switch {
		case na < nb:
			return -1
		case na > nb:
			return 1
		default:
			return 0
		}
	case "float":
		fa, errA := strconv.ParseFloat(a.Text, 64)
		fb, errB := strconv.ParseFloat(b.Text, 64)
		if errA != nil || errB != nil {
			return strings.Compare(a.Text, b.Text)
		}
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	default: // "string" and anything unrecognized
		return strings.Compare(a.Text, b.Text)
	}
}
