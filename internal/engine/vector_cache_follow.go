// Following a table through DELETEs: how the flat vector column cache applies the
// removals the table's change log reports instead of being rebuilt from scratch
// (see Table.ChangesSince).
package engine

import (
	"slices"
	"sort"
	"sync/atomic"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	// vecColumnGarbagePercent is how much packed data can belong to removed
	// rows before the next follow needs to reclaim it. Until then, a followed
	// removal shares the packed array and drops entries in the per-row arrays.
	vecColumnGarbagePercent = 10
	// vecColumnMaxSegments is the slack allowed beyond the segments needed to
	// hold the vectors. Small append batches and removals can leave fragments
	// that the binary-counter merge cannot join.
	vecColumnMaxSegments = 24
)

// vecColumnFollowCount counts the caches built by followVecColumnCache, for tests
// and diagnostics.
var vecColumnFollowCount atomic.Int64

// followVecColumnCache brings cached, which covered the first cached.rows rows of
// the table at its structVersion, up to the table's current rows, given the
// removals and UPDATEs of the vector column that Table.ChangesSince reported. The
// result keeps every surviving row at its position in the table (so no consumer
// changes), shares the packed vector arrays of cached instead of copying them, and
// is a new, immutable entry: cached, which a concurrent search may still hold, is
// never written to.
//
// ok is false whenever following is not possible or not worthwhile (the entry is
// not what the change log describes, or a single packed segment needs rebuilding);
// the caller then builds a cache from scratch. A panic is treated the same way.
func followVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, includeNorms bool, changes storage.TableChanges) (entry vecSearchColumnCacheEntry, ok bool) {
	defer func() {
		if recover() != nil {
			entry, ok = vecSearchColumnCacheEntry{}, false
		}
	}()
	dead := changes.Dead
	oldRows := cached.rows
	if len(dead) == 0 || changes.Survivors != oldRows-len(dead) || changes.Survivors > len(table.Rows) ||
		dead[0] < 0 || dead[len(dead)-1] >= oldRows ||
		!cached.adjacentSegments(0, oldRows) || len(cached.overrides) != len(cached.overrideRows) {
		return vecSearchColumnCacheEntry{}, false
	}
	for i := 1; i < len(dead); i++ {
		if dead[i-1] >= dead[i] {
			return vecSearchColumnCacheEntry{}, false
		}
	}

	// Count retained backing arrays only. A fully removed segment will drop
	// its whole array, including garbage already in it.
	total, garbage := 0, 0
	j := 0
	for _, segment := range cached.segments {
		from := j
		for end := segment.start + len(segment.vectors); j < len(dead) && dead[j] < end; j++ {
		}
		if j-from == len(segment.vectors) {
			continue
		}
		total += len(segment.data)
		garbage += segment.garbage
		for _, row := range dead[from:j] {
			garbage += len(segment.vectors[row-segment.start])
		}
	}
	compact := vecColumnGarbageTooHigh(garbage, total)
	if compact && len(cached.segments) == 1 {
		return vecSearchColumnCacheEntry{}, false
	}

	normsReady := includeNorms || cached.normsReady
	upgrade := normsReady && !cached.normsReady
	segments := make([]vecColumnSegment, 0, len(cached.segments)+1)
	deadBefore, j := 0, 0
	for _, segment := range cached.segments {
		end := segment.start + len(segment.vectors)
		from := j
		for j < len(dead) && dead[j] < end {
			j++
		}
		gone := dead[from:j]
		start := segment.start - deadBefore
		deadBefore += len(gone)
		switch {
		case len(gone) == 0:
			segment.start = start // nothing to drop: share every array
		case len(gone) == len(segment.vectors):
			continue // the whole segment is gone, and its packed data with it
		default:
			segment = removeRowsFromVecColumnSegment(segment, gone, start, cached.normsReady)
		}
		// For a wide corpus reclaim only arrays that exceed the garbage
		// limit. Every other segment can still be shared with active readers.
		if compact && vecColumnGarbageTooHigh(segment.garbage, len(segment.data)) {
			segment = repackVecColumnSegments(cached.normsReady, segment)
		}
		if upgrade {
			segment.norms = vecSegmentNorms(segment)
		}
		segments = append(segments, segment)
	}
	if survivors := changes.Survivors; survivors < len(table.Rows) {
		segments = appendVecColumnSegments(segments, table.Rows[survivors:], colIdx, normsReady, survivors)
	}
	if len(segments) == 0 {
		return vecSearchColumnCacheEntry{}, false
	}
	segments = mergeVecColumnSegments(segments, normsReady)
	segments = limitVecColumnSegments(segments, normsReady)

	// Overrides follow their rows; an UPDATE of the vector column replaces the
	// override of the row it touched, as a refresh does.
	overrides := make(map[int]vecColumnOverride, len(cached.overrides)+len(changes.Updated))
	for _, row := range cached.overrideRows {
		at := sort.SearchInts(dead, row)
		if at < len(dead) && dead[at] == row {
			continue
		}
		value := cached.overrides[row]
		if upgrade && value.valid {
			value.norm = vectorL2Norm(value.vector)
		}
		overrides[row-at] = value
	}
	overrideLimit := vecColumnOverrideLimit(len(table.Rows))
	for _, orig := range changes.Updated {
		row := changes.NewPosition(orig)
		if row < 0 || row >= changes.Survivors || len(overrides) > overrideLimit {
			// Too many rows to override: a rebuild is the better deal, and
			// copying their vectors first would only produce garbage.
			return vecSearchColumnCacheEntry{}, false
		}
		overrides[row] = vecColumnOverrideOf(table.Rows[row], colIdx, normsReady)
	}
	entry = vecSearchColumnCacheEntry{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		rows:          len(table.Rows),
		shape:         new(vecColumnShapeCache),
		segments:      segments,
		overrides:     overrides,
		normsReady:    normsReady,
	}
	if vecColumnOverridesNeedCompaction(entry) {
		return vecSearchColumnCacheEntry{}, false
	}
	if len(overrides) != 0 {
		entry.overrideRows = make([]int, 0, len(overrides))
		for row := range overrides {
			entry.overrideRows = append(entry.overrideRows, row)
		}
		slices.Sort(entry.overrideRows)
	}
	if !entry.adjacentSegments(0, entry.rows) {
		return vecSearchColumnCacheEntry{}, false
	}
	vecColumnFollowCount.Add(1)
	return entry, true
}

func vecColumnGarbageTooHigh(garbage, total int) bool {
	return int64(garbage)*100 > int64(total)*vecColumnGarbagePercent
}

// removeRowsFromVecColumnSegment returns segment without the rows at the ascending
// positions gone (table positions, all inside the segment), starting at newStart.
// The vectors keep pointing into the packed array of segment, which the result
// shares; the floats of the rows dropped are recorded as garbage. Only the
// per-row arrays (24 bytes of slice header, 8 of norm and 1 of validity per row)
// are copied, in runs between the dropped rows.
func removeRowsFromVecColumnSegment(segment vecColumnSegment, gone []int, newStart int, hasNorms bool) vecColumnSegment {
	rows := len(segment.vectors) - len(gone)
	out := vecColumnSegment{
		start:   newStart,
		data:    segment.data,
		vectors: make([][]float64, rows),
		valid:   make([]bool, rows),
		garbage: segment.garbage,
	}
	if hasNorms && len(segment.norms) == len(segment.vectors) {
		out.norms = make([]float64, rows)
	}
	w, from := 0, 0
	run := func(to int) {
		n := copy(out.vectors[w:], segment.vectors[from:to])
		copy(out.valid[w:], segment.valid[from:to])
		if out.norms != nil {
			copy(out.norms[w:], segment.norms[from:to])
		}
		w += n
	}
	for _, position := range gone {
		local := position - segment.start
		out.garbage += len(segment.vectors[local])
		run(local)
		from = local + 1
	}
	run(len(segment.vectors))
	return out
}

// vecSegmentNorms computes the norm of every valid vector of segment.
func vecSegmentNorms(segment vecColumnSegment) []float64 {
	norms := make([]float64, len(segment.vectors))
	for row, vector := range segment.vectors {
		if segment.valid[row] {
			norms[row] = vectorL2Norm(vector)
		}
	}
	return norms
}

// vecColumnOverrideOf is the override of a row whose vector column was updated.
func vecColumnOverrideOf(row []any, colIdx int, withNorm bool) vecColumnOverride {
	var value vecColumnOverride
	if colIdx < len(row) && row[colIdx] != nil {
		if vector, ok := vecRowValue(row[colIdx]); ok {
			value.vector = append([]float64(nil), vector...)
			value.valid = true
			if withNorm {
				value.norm = vectorL2Norm(value.vector)
			}
		}
	}
	return value
}

// limitVecColumnSegments merges small neighbours that fit within the packing
// budget. Let a large column keep a proportional number of segments and a
// small tail before forcing a merge of unequally sized neighbours.
func limitVecColumnSegments(segments []vecColumnSegment, normsReady bool) []vecColumnSegment {
	if len(segments) <= vecColumnMaxSegments {
		return segments
	}
	rows, floats := 0, 0
	for _, segment := range segments {
		rows += len(segment.vectors)
		floats += len(segment.data) - segment.garbage
	}
	limit := vecColumnMaxSegments + rows/vecColumnSegmentMaxRows + floats/vecColumnSegmentMaxFloats
	for len(segments) > limit {
		best, bestRows := 0, -1
		for i := 0; i+1 < len(segments); i++ {
			if !vecColumnSegmentsFit(segments[i], segments[i+1]) {
				continue
			}
			if rows := len(segments[i].vectors) + len(segments[i+1].vectors); bestRows < 0 || rows < bestRows {
				best, bestRows = i, rows
			}
		}
		if bestRows < 0 {
			break
		}
		segments[best] = repackVecColumnSegments(normsReady, segments[best], segments[best+1])
		segments = slices.Delete(segments, best+1, best+2)
	}
	return segments
}
