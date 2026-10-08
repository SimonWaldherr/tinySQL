package storage

// Ordered range seeks on a materialized secondary index.
//
// Point and prefix seeks answer `col = ?`. A range predicate — `col > ?`,
// `col BETWEEN ? AND ?` — had no index path at all, so the most ordinary spatial
// query there is,
//
//	WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
//
// scanned the whole table. For a POI or road-segment table that is the difference
// between a usable and an unusable map query.
//
// A range seek is possible because index entries are a byte-sorted array and the
// numeric key encoding is order-preserving: appendCanonicalIndexValue writes
// integers as big-endian with the sign bit flipped, and float64 with its bits
// flipped for negatives, so comparing encoded bytes compares the numbers.
//
// It is restricted to numeric components, and that restriction is not
// conservatism — it is required:
//
//   - Text and BLOB components are framed as tag + 4-byte big-endian length +
//     payload, so byte order compares the *length* first: "z" sorts before "aa".
//     Byte order is therefore not string order, and a range walk would return the
//     wrong rows.
//   - Integers and floats carry different type tags (0x02 and 0x03), so a column
//     holding both sorts every integer before every float regardless of value.
//     The caller must establish that a column's numeric kind is uniform before
//     asking for a range; the engine does this via its cached column profile.

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// ErrIndexRangeUnsupported reports that this index cannot answer the requested
// range, so the caller must fall back to a scan. It is returned rather than a
// partial result: silently dropping an entry the walk cannot compare would return
// wrong rows, which is worse than being slow.
var ErrIndexRangeUnsupported = errors.New("secondary index cannot serve this range")

// IndexRangeBound is one end of a range. Absent means unbounded on that side.
type IndexRangeBound struct {
	Value     any
	Inclusive bool
	Absent    bool
}

// numericComponentLen is the encoded width of an integer or float component:
// one tag byte, a 4-byte length, and an 8-byte payload. Fixed width is what lets
// a walk compare exactly the range component of a longer composite key.
const numericComponentLen = 1 + 4 + 8

// LookupSecondaryIndexRange returns the row IDs whose index key matches every
// value in prefix and whose next component falls within lo..hi.
//
// prefix supplies equality-matched leading columns and may be empty. The range
// applies to the component at position len(prefix), which must be a numeric
// column; trailing index columns are ignored, so the result is a superset the
// caller still filters. Row IDs come back in table order, matching the
// observable order of a scan with no ORDER BY.
func (t *Table) LookupSecondaryIndexRange(idx *SecondaryIndex, prefix []any, lo, hi IndexRangeBound) ([]int, error) {
	return t.LookupSecondaryIndexRangeNext(idx, prefix, lo, hi, nil)
}

// IndexNextBounds are bounds on the index column that follows the range column,
// applied to each entry as the walk passes it. They only drop rows the caller's
// residual filter would drop anyway; an entry whose next component cannot be
// compared exactly (another type tag, NULL, a shorter key) is always kept.
//
// This is what makes a two-dimensional box predicate on a (lat, lon) index cost
// about the rows inside the box rather than every row in the latitude band.
type IndexNextBounds struct {
	Lo, Hi IndexRangeBound
}

// LookupSecondaryIndexRangeNext is LookupSecondaryIndexRange with an optional
// pre-filter on the column after the range column. next may be nil. The result
// is a superset of the matching rows, exactly as for the one-column form.
func (t *Table) LookupSecondaryIndexRangeNext(idx *SecondaryIndex, prefix []any, lo, hi IndexRangeBound, next *IndexNextBounds) ([]int, error) {
	if idx == nil {
		return nil, nil
	}
	if len(prefix) >= len(idx.Columns) {
		return nil, fmt.Errorf("range seek needs a column after the %d-column prefix, index has %d",
			len(prefix), len(idx.Columns))
	}
	if lo.Absent && hi.Absent {
		return nil, fmt.Errorf("range seek needs at least one bound")
	}

	var prefixScratch [128]byte
	prefixKey := canonicalIndexKeyInto(prefixScratch[:0], prefix)

	// Encode the bounds as single components so they can be compared against the
	// range component of each entry's key.
	var loEnc, hiEnc []byte
	var loScratch, hiScratch [numericComponentLen]byte
	if !lo.Absent {
		loEnc = appendCanonicalIndexValue(loScratch[:0], lo.Value)
		if len(loEnc) != numericComponentLen {
			return nil, fmt.Errorf("%w: lower bound %T is not numeric", ErrIndexRangeUnsupported, lo.Value)
		}
	}
	if !hi.Absent {
		hiEnc = appendCanonicalIndexValue(hiScratch[:0], hi.Value)
		if len(hiEnc) != numericComponentLen {
			return nil, fmt.Errorf("%w: upper bound %T is not numeric", ErrIndexRangeUnsupported, hi.Value)
		}
	}

	// Bounds on the following column become a pre-filter only if they encode as
	// fixed-width numeric components of one kind; otherwise it is simply off.
	var nextFilter *nextComponentFilter
	if next != nil && len(prefix)+1 < len(idx.Columns) {
		nextFilter = newNextComponentFilter(*next)
	}

	// Start at the first entry that could satisfy the prefix and lower bound.
	// Seeking to prefix||loEnc is correct even though entries may carry further
	// components: any key sharing the prefix whose range component is below the
	// bound sorts before it.
	var seekScratch [128 + numericComponentLen]byte
	seek := append(append(seekScratch[:0], prefixKey...), loEnc...)

	// A small starting capacity cuts the append-driven reallocation chain
	// (0->1->2->4->...) that would otherwise run on every range seek: most
	// seeks match at least a handful of rows, and even a seek that matches
	// hundreds still reallocates only a few times instead of eight-plus.
	out := make([]int, 0, 16)
	var rangeErr error
	i := 0
	idx.visitRange(seek, func(key []byte, rowIDs []int) bool {
		if !bytes.HasPrefix(key, prefixKey) {
			return false // left the equality prefix; entries are sorted, so nothing follows
		}
		off := len(prefixKey)
		if off+numericComponentLen > len(key) {
			// The component is not a fixed-width numeric one, so the column is
			// not uniformly numeric after all and byte order does not track value
			// order. Abandon the seek instead of returning rows chosen by the
			// wrong comparison.
			rangeErr = fmt.Errorf("%w: entry %d has a non-numeric range component", ErrIndexRangeUnsupported, i)
			return false
		}
		component := key[off : off+numericComponentLen]
		if component[0] != loHiTag(loEnc, hiEnc) {
			// A differently tagged component (an integer where the bounds are
			// floats, say) sorts as a block rather than by value.
			rangeErr = fmt.Errorf("%w: entry %d component tag %#x does not match the bounds",
				ErrIndexRangeUnsupported, i, component[0])
			return false
		}

		if !lo.Absent {
			cmp := bytes.Compare(component, loEnc)
			// Reachable only for an exclusive bound: the seek above already
			// skipped everything strictly below loEnc.
			if cmp < 0 || (cmp == 0 && !lo.Inclusive) {
				i++
				return true
			}
		}
		if !hi.Absent {
			cmp := bytes.Compare(component, hiEnc)
			if cmp > 0 || (cmp == 0 && !hi.Inclusive) {
				return false // past the bound, and the rest sorts higher still
			}
		}
		if nextFilter != nil && nextFilter.excludes(key[off+numericComponentLen:]) {
			i++
			return true
		}
		out = append(out, rowIDs...)
		i++
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	sort.Ints(out)
	return out, nil
}

// nextComponentFilter compares the component after the range column with its
// bounds, byte for byte, which orders exactly as the numbers do (see the file
// comment).
type nextComponentFilter struct {
	loEnc, hiEnc       [numericComponentLen]byte
	hasLo, hasHi       bool
	loInclusive, hiInc bool
	tag                byte
}

func newNextComponentFilter(b IndexNextBounds) *nextComponentFilter {
	f := &nextComponentFilter{loInclusive: b.Lo.Inclusive, hiInc: b.Hi.Inclusive}
	if !b.Lo.Absent {
		enc := appendCanonicalIndexValue(f.loEnc[:0], b.Lo.Value)
		if len(enc) != numericComponentLen {
			return nil
		}
		f.hasLo, f.tag = true, f.loEnc[0]
	}
	if !b.Hi.Absent {
		enc := appendCanonicalIndexValue(f.hiEnc[:0], b.Hi.Value)
		if len(enc) != numericComponentLen {
			return nil
		}
		if f.hasLo && f.hiEnc[0] != f.tag {
			return nil // bounds of different kinds: nothing exact to compare
		}
		f.hasHi, f.tag = true, f.hiEnc[0]
	}
	if !f.hasLo && !f.hasHi {
		return nil
	}
	return f
}

// excludes reports whether the entry whose remaining key starts with rest
// provably lies outside the bounds.
func (f *nextComponentFilter) excludes(rest []byte) bool {
	if len(rest) < numericComponentLen || rest[0] != f.tag {
		return false
	}
	comp := rest[:numericComponentLen]
	if f.hasLo {
		if cmp := bytes.Compare(comp, f.loEnc[:]); cmp < 0 || (cmp == 0 && !f.loInclusive) {
			return true
		}
	}
	if f.hasHi {
		if cmp := bytes.Compare(comp, f.hiEnc[:]); cmp > 0 || (cmp == 0 && !f.hiInc) {
			return true
		}
	}
	return false
}

// loHiTag returns the type tag the bounds encode with. Both bounds are checked
// to be numeric before this is called, and the caller is required to have made
// them the same kind, so either one answers.
func loHiTag(loEnc, hiEnc []byte) byte {
	if len(loEnc) > 0 {
		return loEnc[0]
	}
	return hiEnc[0]
}

// IndexRangeComponentEncodable reports whether v encodes as a fixed-width
// numeric component, i.e. whether it can serve as a range bound. Text and BLOB
// values cannot: their framing puts a length ahead of the payload, so byte order
// is not value order.
func IndexRangeComponentEncodable(v any) bool {
	var scratch [numericComponentLen]byte
	return len(appendCanonicalIndexValue(scratch[:0], v)) == numericComponentLen
}
