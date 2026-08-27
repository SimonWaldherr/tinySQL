package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// pagedSimpleSelectSource is a query-private, bounded cursor description for
// a read-only ModePagedIndex table. indexName empty selects the table B+Tree;
// otherwise startKey..endKey is an inclusive secondary-index interval.
//
// It deliberately stores encoded keys rather than decoded candidate rows. The
// latter was the old PagedIndexRows path and forced all matching BLOBs into
// memory before ResultStream could produce even one row.
type pagedSimpleSelectSource struct {
	tenant    string
	table     string
	indexName string
	startKey  []byte
	endKey    []byte
}

// buildStreamingSimpleSelectPlan is the direct-stream counterpart of
// buildSimpleSelectPlan. Read-only paged artifacts use schema metadata plus a
// cursor description, avoiding LoadTable/PagedIndexRows before the first row.
// Mutable paged databases deliberately retain the regular locked path: their
// pager roots can change between cursor batches, so pretending that a long
// stream were an immutable snapshot would weaken query consistency.
func buildStreamingSimpleSelectPlan(env ExecEnv, s *Select) (*simpleSelectPlan, bool, error) {
	if !simpleSelectEligible(s) {
		return nil, false, nil
	}

	metadata, paged, err := env.db.PagedIndexMetadata(env.tenant, s.From.Table)
	if err != nil {
		return nil, true, err
	}
	if !paged || !env.db.IsReadOnly() {
		return buildSimpleSelectPlan(env, s)
	}

	template, ok, err := loadSimpleSelectPlanTemplate(metadata, s, true)
	if !ok || err != nil {
		return nil, ok, err
	}
	plan := *template
	plan.table = metadata
	resetSimplePlanAccess(&plan, 0)
	source := &pagedSimpleSelectSource{tenant: env.tenant, table: s.From.Table}

	// A complete composite equality predicate maps to one contiguous physical
	// index key. The 0xff suffix includes every row-ID payload behind a
	// non-unique key while remaining before the next complete component (whose
	// type tag is at most 0x7f in CanonicalIndexKey).
	if idx, values, predicates, residual := selectSecondaryIndex(metadata, plan.colIndex, s.Where); idx != nil && len(values) == len(idx.Columns) {
		start := storage.CanonicalIndexKey(values)
		source.indexName = idx.Name
		source.startKey = start
		source.endKey = append(append([]byte(nil), start...), 0xff)
		plan.scanType = "PAGED INDEX POINT SEEK"
		plan.indexName = idx.Name
		plan.indexPredicates = predicates
		plan.residualFilter = residual
		plan.coveringIndex = projectionsCoveredByIndex(plan.projs, idx, metadata)
	} else if rangePlan, haveRange := selectPagedStreamRangeIndex(metadata, plan.colIndex, s.Where); haveRange {
		prefix := append([]any(nil), rangePlan.prefix...)
		prefixKey := storage.CanonicalIndexKey(prefix)
		if rangePlan.lo.Absent {
			source.startKey = prefixKey
		} else {
			source.startKey = storage.CanonicalIndexKey(append(prefix, rangePlan.lo.Value))
		}
		if rangePlan.hi.Absent {
			source.endKey = append(append([]byte(nil), prefixKey...), 0xff)
		} else {
			end := storage.CanonicalIndexKey(append(prefix, rangePlan.hi.Value))
			source.endKey = append(end, 0xff)
		}
		source.indexName = rangePlan.index.Name
		plan.scanType = "PAGED INDEX RANGE SCAN"
		plan.indexName = rangePlan.index.Name
		plan.indexPredicates = rangePlan.predicates
		// Bounds deliberately include exclusive endpoints; raw WHERE evaluation
		// removes them and preserves SQL semantics without fragile byte-key
		// successor arithmetic.
		plan.residualFilter = true
		plan.coveringIndex = projectionsCoveredByIndex(plan.projs, rangePlan.index, metadata)
	} else {
		plan.scanType = "PAGED TABLE SCAN"
	}
	plan.pagedSource = source
	return &plan, true, nil
}

// selectPagedStreamRangeIndex is the metadata-only counterpart of
// selectRangeIndex. A paged serving artifact intentionally has no table rows
// resident, so it cannot build numericColumnProfile. Restrict it to declared
// integer/real columns whose storage coercion fixes one canonical index tag;
// ambiguous NUMERIC/FLOAT64 columns retain the safe paged table scan.
func selectPagedStreamRangeIndex(table *storage.Table, colIndex map[string]int, where Expr) (*rangeIndexPlan, bool) {
	if table == nil || where == nil || len(table.Indexes) == 0 {
		return nil, false
	}
	ranges := make(map[int]*rangeTerm)
	collectRangeTerms(where, colIndex, ranges)
	if len(ranges) == 0 {
		return nil, false
	}
	equalities := make(map[int]any)
	collectEqualityTerms(where, colIndex, equalities)

	var best *rangeIndexPlan
	bestScore := -1
	for _, name := range sortedIndexNames(table) {
		index := table.Indexes[name]
		if index == nil || len(index.Columns) == 0 {
			continue
		}
		prefix := make([]any, 0, len(index.Columns))
		predicates := make([]string, 0, len(index.Columns))
		for _, column := range index.Columns {
			pos, err := table.ColIndex(column)
			if err != nil {
				break
			}
			if value, ok := equalities[pos]; ok {
				if isNumericSQLValue(value) && !numericPagedIndexSeekSafe(table.Cols[pos], value) {
					break
				}
				prefix = append(prefix, value)
				predicates = append(predicates, column+" = ?")
				continue
			}
			term, ok := ranges[pos]
			if !ok || !term.bounded() {
				break
			}
			lo, hi, ok := pagedStreamRangeBounds(table.Cols[pos], *term)
			if !ok {
				break
			}
			candidate := &rangeIndexPlan{
				index:      index,
				prefix:     append([]any(nil), prefix...),
				lo:         lo,
				hi:         hi,
				predicates: append(append([]string(nil), predicates...), rangePredicateText(column, lo, hi)),
				rangeCol:   column,
			}
			score := len(prefix) * 2
			if !lo.Absent && !hi.Absent {
				score++
			}
			if score > bestScore || (score == bestScore && best != nil && strings.Compare(strings.ToLower(index.Name), strings.ToLower(best.index.Name)) < 0) {
				best, bestScore = candidate, score
			}
			break // a range consumes the next index component
		}
	}
	return best, best != nil
}

func pagedStreamRangeBounds(column storage.Column, term rangeTerm) (storage.IndexRangeBound, storage.IndexRangeBound, bool) {
	wantFloat, ok := pagedStreamRangeKind(column)
	if !ok {
		return term.lo, term.hi, false
	}
	convert := func(bound storage.IndexRangeBound) (storage.IndexRangeBound, bool) {
		if bound.Absent {
			return bound, true
		}
		if !isNumericSQLValue(bound.Value) {
			return bound, false
		}
		value, ok := coerceRangeBound(bound.Value, wantFloat)
		if !ok || !storage.IndexRangeComponentEncodable(value) {
			return bound, false
		}
		bound.Value = value
		return bound, true
	}
	lo, ok := convert(term.lo)
	if !ok {
		return lo, term.hi, false
	}
	hi, ok := convert(term.hi)
	if !ok {
		return lo, hi, false
	}
	lo, hi = normalizeSignedZeroBounds(lo, hi)
	return lo, hi, true
}

func pagedStreamRangeKind(column storage.Column) (wantFloat bool, ok bool) {
	switch column.Affinity {
	case storage.AffinityInteger:
		return false, true
	case storage.AffinityReal:
		return true, true
	case storage.AffinityNumeric:
		return false, false
	}
	switch column.Type {
	case storage.IntType, storage.Int8Type, storage.Int16Type, storage.Int32Type, storage.Int64Type:
		return false, true
	case storage.Float32Type, storage.Float64Type, storage.FloatType:
		return true, true
	default:
		return false, false
	}
}
