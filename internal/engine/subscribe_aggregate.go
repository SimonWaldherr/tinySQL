package engine

import (
	"context"
	"reflect"
	"sort"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// State belongs to one subscription and is accessed under the content read lock.
// A row snapshot contains the values needed to rebuild its group in source order.
type subscriptionAggregate struct {
	table                      *storage.Table
	columns                    []storage.Column
	plan                       *simpleAggregatePlan
	source                     *simpleSelectPlan
	groups                     map[string]*subscriptionAggregateGroup
	identities                 map[*any]*subscriptionAggregateRow
	generation                 uint64
	rows                       []*subscriptionAggregateRow
	version, structural, count int
}
type subscriptionAggregateRow struct {
	source  []any // retains the backing array used to identify a surviving row
	values  []any
	group   string
	index   int
	matched bool
	seen    uint64
}
type subscriptionAggregateGroup struct {
	state   *simpleAggregateState
	members map[*subscriptionAggregateRow]struct{}
}

// Only direct projections and deterministic, row-local filters are eligible.
// Index candidates are in table order; fully covered constraint seeks fall back.
func (s *querySubscriptionState) tryInitializeAggregate(ctx context.Context) error {
	s.aggregateChecked = true
	q := s.query
	if !subscriptionPredicate(q.Where) || len(q.OrderBy) != 0 || q.Limit != nil || q.Offset != nil {
		return nil
	}
	env := ExecEnv{ctx: ctx, db: s.db, tenant: s.tenant}
	plan, ok, err := buildSimpleAggregatePlan(env, q)
	if err != nil || !ok || plan.resultEvals != nil || len(plan.groupExprs) != 0 {
		return nil
	}
	table, err := s.db.Get(s.tenant, q.From.Table)
	if err != nil || table != plan.table {
		return nil
	}
	for _, proj := range plan.projs {
		if proj.filter != nil {
			return nil
		}
		switch proj.kind {
		case aggGroupCol:
		case aggCount, aggSum, aggAvg, aggMin, aggMax:
			if proj.arg != nil && proj.argColumn == 0 {
				return nil
			}
		default:
			return nil
		}
	}
	var source *simpleSelectPlan
	if subscriptionIndexOrderIndependent(plan) {
		source = &simpleSelectPlan{table: table, colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where)}
	} else {
		source, err = buildSimpleAggregateSourcePlan(plan)
		if err != nil || source.filterFullyCovered {
			return nil
		}
	}
	a := &subscriptionAggregate{table: table, columns: append([]storage.Column(nil), table.Cols...), plan: plan, source: source, groups: make(map[string]*subscriptionAggregateGroup), identities: make(map[*any]*subscriptionAggregateRow, len(table.Rows)), rows: make([]*subscriptionAggregateRow, 0, len(table.Rows)+len(table.Rows)/4), version: table.Version, structural: table.StructVersion(), count: len(table.Rows)}
	for i, raw := range table.Rows {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return err
			}
		}
		entry, ok, err := a.makeRow(raw, i)
		if err != nil || !ok {
			return nil
		}
		id := &raw[0]
		if a.identities[id] != nil {
			return nil
		}
		a.identities[id] = entry
		if entry.matched {
			entry.values = append([]any(nil), entry.values...)
		}
		a.rows = append(a.rows, entry)
		if entry.matched {
			group := a.ensureGroup(entry)
			group.members[entry] = struct{}{}
			if err := accumulateSimpleAggregateState(env, source, entry.values, group.state, plan.projs); err != nil {
				return nil
			}
		}
	}
	if len(plan.groupCols) == 0 {
		a.ensureWholeGroup()
	}
	result, err := a.result(ctx)
	if err != nil {
		return nil
	}
	if !subscriptionAggregateSameResult(result, s.snapshot) {
		return nil
	}
	s.aggregate = a
	return nil
}

func (a *subscriptionAggregate) makeRow(raw []any, index int) (*subscriptionAggregateRow, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	row := &subscriptionAggregateRow{source: raw, index: index}
	match := true
	var err error
	if a.source.where != nil {
		if a.source.filter != nil {
			match, err = a.source.filter(raw)
		} else {
			match, err = evalRawWhere(a.source, raw)
		}
	}
	if err != nil {
		return nil, false, err
	}
	if !match {
		return row, true, nil
	}
	if !a.isCandidate(index) {
		return nil, false, nil
	}
	if !a.accepts(raw) {
		return nil, false, nil
	}
	row.matched = true
	row.values = raw
	row.group = a.groupKey(raw)
	return row, true, nil
}

func (a *subscriptionAggregate) groupKey(raw []any) string {
	var key []byte
	for i, col := range a.plan.groupCols {
		if i > 0 {
			key = append(key, '\x1f')
		}
		key = writeFmtKeyPart(key, raw[col])
	}
	return string(key)
}

func (a *subscriptionAggregate) ensureGroup(row *subscriptionAggregateRow) *subscriptionAggregateGroup {
	if g := a.groups[row.group]; g != nil {
		return g
	}
	values := make([]any, len(a.plan.groupCols))
	for i, col := range a.plan.groupCols {
		values[i] = row.values[col]
	}
	g := &subscriptionAggregateGroup{state: newSimpleAggregateState(values, len(a.plan.projs)), members: make(map[*subscriptionAggregateRow]struct{})}
	a.groups[row.group] = g
	return g
}
func (a *subscriptionAggregate) ensureWholeGroup() {
	if a.groups[""] == nil {
		a.groups[""] = &subscriptionAggregateGroup{state: newSimpleAggregateState(nil, len(a.plan.projs)), members: make(map[*subscriptionAggregateRow]struct{})}
	}
}
func (a *subscriptionAggregate) result(ctx context.Context) (*ResultSet, error) {
	states := make([]*simpleAggregateState, 0, len(a.groups))
	for _, g := range a.groups {
		states = append(states, g.state)
	}
	return finalizeSimpleAggregateResultSet(ExecEnv{ctx: ctx}, a.plan, states)
}
func subscriptionAggregateSameResult(result *ResultSet, snapshot []Row) bool {
	if len(result.Rows) != len(snapshot) {
		return false
	}
	keys := setOperationColumnKeys(result.Cols)
	buckets := make(map[string][]Row, len(snapshot))
	var buf []byte
	for _, row := range snapshot {
		buf = appendSetOperationSignature(buf[:0], row, keys)
		key := string(buf)
		buckets[key] = append(buckets[key], row)
	}
	for _, row := range result.Rows {
		buf = appendSetOperationSignature(buf[:0], row, keys)
		key := string(buf)
		candidates := buckets[key]
		found := -1
		for i, candidate := range candidates {
			if reflect.DeepEqual(row, candidate) {
				found = i
				break
			}
		}
		if found < 0 {
			return false
		}
		candidates[found] = candidates[len(candidates)-1]
		buckets[key] = candidates[:len(candidates)-1]
	}
	return true
}

func (a *subscriptionAggregate) accepts(raw []any) bool {
	for _, col := range a.plan.groupCols {
		if col < 0 || col >= len(raw) || !subscriptionAggregateScalar(raw[col]) {
			return false
		}
	}
	for _, proj := range a.plan.projs {
		if proj.argColumn == 0 {
			continue
		}
		col := proj.argColumn - 1
		if col >= len(raw) {
			return false
		}
		value := raw[col]
		switch proj.kind {
		case aggSum, aggAvg:
			switch value.(type) {
			case nil, int, int64, float64:
			default:
				return false
			}
		case aggMin, aggMax:
			if !subscriptionAggregateScalar(value) {
				return false
			}
		}
	}
	return true
}
func subscriptionAggregateScalar(v any) bool {
	switch v.(type) {
	case nil, int, int64, string, bool:
		return true
	}
	return false
}

// unchangedRow checks only the predicate and values that affect this result.
func (a *subscriptionAggregate) unchangedRow(old *subscriptionAggregateRow, raw []any, index int) (bool, error) {
	matched := true
	var err error
	if a.source.where != nil {
		if a.source.filter != nil {
			matched, err = a.source.filter(raw)
		} else {
			matched, err = evalRawWhere(a.source, raw)
		}
	}
	if err != nil {
		return false, err
	}
	if matched != old.matched {
		return false, nil
	}
	if !matched {
		return true, nil
	}
	if !a.isCandidate(index) {
		return false, nil
	}
	if !a.accepts(raw) {
		return false, nil
	}
	for _, col := range a.plan.groupCols {
		if old.values[col] != raw[col] {
			return false, nil
		}
	}
	for _, proj := range a.plan.projs {
		if proj.argColumn == 0 {
			continue
		}
		col := proj.argColumn - 1
		if proj.kind == aggCount {
			if (old.values[col] == nil) != (raw[col] == nil) {
				return false, nil
			}
		} else if old.values[col] != raw[col] {
			return false, nil
		}
	}
	return true, nil
}

func (s *querySubscriptionState) refreshAggregate(ctx context.Context) (*QueryChange, error) {
	a := s.aggregate
	table, err := s.db.Get(s.tenant, s.query.From.Table)
	if err != nil {
		s.aggregate = nil
		return s.refreshFull(ctx)
	}
	if table == a.table && table.Version == a.version {
		return nil, nil
	}
	fallback := func() (*QueryChange, error) {
		s.aggregate = nil
		change, err := s.refreshFull(ctx)
		if err == nil {
			err = s.tryInitializeAggregate(ctx)
		}
		return change, err
	}
	if table != a.table || !reflect.DeepEqual(a.columns, table.Cols) {
		return fallback()
	}
	// Fully covered seeks bypass WHERE evaluation in the executor.
	if a.plan.where != nil && !subscriptionIndexOrderIndependent(a.plan) {
		source, err := buildSimpleAggregateSourcePlan(a.plan)
		if err != nil || source.filterFullyCovered {
			return fallback()
		}
		a.source.rowIDs = source.rowIDs
	}
	env := ExecEnv{ctx: ctx, db: s.db, tenant: s.tenant}
	affected := make(map[string]struct{})
	changed := false
	remove := func(row *subscriptionAggregateRow) {
		if row.matched {
			delete(a.groups[row.group].members, row)
			affected[row.group] = struct{}{}
		}
	}
	add := func(row *subscriptionAggregateRow) {
		if row.matched {
			g := a.ensureGroup(row)
			g.members[row] = struct{}{}
			affected[row.group] = struct{}{}
		}
	}
	appendRow := func(row *subscriptionAggregateRow) error {
		if !row.matched {
			return nil
		}
		group := a.ensureGroup(row)
		group.members[row] = struct{}{}
		changed = true
		if _, dirty := affected[row.group]; !dirty {
			return accumulateSimpleAggregateState(env, a.source, row.values, group.state, a.plan.projs)
		}
		return nil
	}
	updated, updatesOnly := table.UpdatedRowsSince(a.structural)
	scanned := 0
	if updatesOnly && len(table.Rows) >= a.count {
		for _, i := range updated {
			if i < 0 || i >= len(table.Rows) {
				return fallback()
			}
			if i >= a.count {
				continue
			}
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
			old := a.rows[i]
			raw := table.Rows[i]
			if len(raw) == 0 {
				return fallback()
			}
			unchanged, err := a.unchangedRow(old, raw, i)
			if err != nil {
				return fallback()
			}
			if unchanged {
				oldID, newID := &old.source[0], &raw[0]
				if other := a.identities[newID]; other != nil && other != old {
					return fallback()
				}
				delete(a.identities, oldID)
				a.identities[newID] = old
				old.source = raw
				scanned++
				continue
			}
			next, ok, err := a.makeRow(raw, i)
			scanned++
			if err != nil || !ok {
				return fallback()
			}
			if next.matched {
				next.values = append([]any(nil), next.values...)
			}
			oldID, newID := &old.source[0], &next.source[0]
			if other := a.identities[newID]; other != nil && other != old {
				return fallback()
			}
			delete(a.identities, oldID)
			a.identities[newID] = next
			remove(old)
			a.rows[i] = next
			add(next)
		}
		for i := a.count; i < len(table.Rows); i++ {
			if i&63 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			next, ok, err := a.makeRow(table.Rows[i], i)
			scanned++
			if err != nil || !ok {
				return fallback()
			}
			if next.matched {
				next.values = append([]any(nil), next.values...)
			}
			id := &next.source[0]
			if a.identities[id] != nil {
				return fallback()
			}
			a.identities[id] = next
			a.rows = append(a.rows, next)
			if err := appendRow(next); err != nil {
				return nil, err
			}
		}
	} else {
		// DELETE compacts row positions and clears update history. Match surviving
		// backing arrays, reevaluate the predicate, and recompute affected groups.
		// Ambiguous identities and reordering fall back to a complete refresh.
		a.generation++
		if a.generation == 0 {
			return fallback()
		}
		nextRows := make([]*subscriptionAggregateRow, 0, len(table.Rows))
		lastOld := -1
		for i, raw := range table.Rows {
			if i&63 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			if len(raw) == 0 {
				return fallback()
			}
			scanned++
			id := &raw[0]
			old := a.identities[id]
			if old != nil {
				if old.seen == a.generation || old.index <= lastOld {
					return fallback()
				}
				lastOld = old.index
				old.seen = a.generation
				same, err := a.unchangedRow(old, raw, i)
				if err != nil {
					return fallback()
				}
				if same {
					old.index = i
					old.source = raw
					nextRows = append(nextRows, old)
					continue
				}
			}
			next, ok, err := a.makeRow(raw, i)
			if err != nil || !ok {
				return fallback()
			}
			if next.matched {
				next.values = append([]any(nil), next.values...)
			}
			if old != nil {
				remove(old)
			}
			next.seen = a.generation
			a.identities[id] = next
			add(next)
			nextRows = append(nextRows, next)
		}
		for _, old := range a.rows {
			if old.seen != a.generation {
				remove(old)
				id := &old.source[0]
				if a.identities[id] == old {
					delete(a.identities, id)
				}
			}
		}
		a.rows = nextRows
	}
	for key := range affected {
		group := a.groups[key]
		if len(group.members) == 0 && len(a.plan.groupCols) != 0 {
			delete(a.groups, key)
			continue
		}
		members := make([]*subscriptionAggregateRow, 0, len(group.members))
		for row := range group.members {
			members = append(members, row)
		}
		sort.Slice(members, func(i, j int) bool { return members[i].index < members[j].index })
		values := []any(nil)
		if len(members) > 0 {
			values = make([]any, len(a.plan.groupCols))
			for i, col := range a.plan.groupCols {
				values[i] = members[0].values[col]
			}
		}
		group.state = newSimpleAggregateState(values, len(a.plan.projs))
		for i, row := range members {
			if i&63 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			scanned++
			if err := accumulateSimpleAggregateState(env, a.source, row.values, group.state, a.plan.projs); err != nil {
				return nil, err
			}
		}
	}
	if len(a.plan.groupCols) == 0 {
		a.ensureWholeGroup()
	}
	a.count = len(table.Rows)
	a.version = table.Version
	a.structural = table.StructVersion()
	if !changed && len(affected) == 0 {
		return nil, nil
	}
	result, err := a.result(ctx)
	if err != nil {
		return nil, err
	}
	return s.applySubscriptionResult(ctx, result, scanned)
}

// COUNT over direct columns is independent of the source scan order.
// Other aggregates validate indexed access before incrementally maintaining state.
func subscriptionIndexOrderIndependent(plan *simpleAggregatePlan) bool {
	for _, proj := range plan.projs {
		if proj.kind != aggGroupCol && proj.kind != aggCount {
			return false
		}
	}
	return true
}

// Candidate lists are sorted in table order by the index planner.
func (a *subscriptionAggregate) isCandidate(row int) bool {
	if a.source.rowIDs == nil {
		return true
	}
	i := sort.SearchInts(a.source.rowIDs, row)
	return i < len(a.source.rowIDs) && a.source.rowIDs[i] == row
}
