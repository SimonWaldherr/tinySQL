package engine

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"
)

// refreshFull runs under the caller's content read lock. Calling Execute here
// would acquire the same lock again and deadlock when a writer is waiting.
func (s *querySubscriptionState) refreshFull(ctx context.Context) (*QueryChange, error) {
	s.fullRefreshes.Add(1)
	rs, err := executeSelect(ExecEnv{ctx: ctx, db: s.db, tenant: s.tenant, now: time.Now(), subqueryCache: newSubqueryResultCache()}, s.query)
	if err != nil {
		return nil, err
	}
	return s.applySubscriptionResult(ctx, rs, -1)
}

// applySubscriptionResult compares visible rows as multisets and retains owned copies.
func (s *querySubscriptionState) applySubscriptionResult(ctx context.Context, rs *ResultSet, scanned int) (*QueryChange, error) {
	if s.options.MaxResultRows > 0 && len(rs.Rows) > s.options.MaxResultRows {
		return nil, fmt.Errorf("subscription result exceeds row limit %d", s.options.MaxResultRows)
	}
	initial := !s.fullInitialized
	if !initial && !slices.Equal(s.cols, rs.Cols) {
		return nil, fmt.Errorf("subscription result schema changed")
	}
	// A common live-metric result is one row. If its visible values did not
	// change, avoid building a multiset index and cloning the same snapshot.
	if !initial && len(s.snapshot) == 1 && len(rs.Rows) == 1 {
		same := true
		for key, old := range s.snapshot[0] {
			if !reflect.DeepEqual(old, rs.Rows[0][key]) {
				same = false
				break
			}
		}
		if same {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		}
	}
	change := &QueryChange{Initial: initial, Cols: slices.Clone(rs.Cols), ScannedRows: scanned}
	keys := setOperationColumnKeys(rs.Cols)
	// Signatures narrow comparisons; DeepEqual resolves collisions and preserves
	// Go value types. Index lists retain duplicate rows with multiset semantics.
	buckets := make(map[string][]int, len(s.snapshot))
	var buf []byte
	for i, row := range s.snapshot {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], row, keys)
		key := string(buf)
		buckets[key] = append(buckets[key], i)
	}
	matched := make([]bool, len(s.snapshot))
	next := make([]Row, 0, len(rs.Rows))
	// Reuse one borrowed projection for comparisons; retained and emitted rows
	// are separately cloned below. Every visible key is overwritten each time.
	row := make(Row, len(keys))
	for i, raw := range rs.Rows {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		// Executor rows can carry hidden ORDER BY fields. Only the visible result
		// columns participate in subscription output and equality.
		for _, key := range keys {
			row[key] = raw[key]
		}
		buf = appendSetOperationSignature(buf[:0], row, keys)
		key := string(buf)
		candidates := buckets[key]
		found := -1
		for j := len(candidates) - 1; j >= 0; j-- {
			index := candidates[j]
			if reflect.DeepEqual(s.snapshot[index], row) {
				found = index
				candidates[j] = candidates[len(candidates)-1]
				buckets[key] = candidates[:len(candidates)-1]
				break
			}
		}
		if found >= 0 {
			matched[found] = true
			next = append(next, s.snapshot[found])
			continue
		}
		owned, err := cloneSubscriptionRow(row)
		if err != nil {
			return nil, err
		}
		out, err := cloneSubscriptionRow(owned)
		if err != nil {
			return nil, err
		}
		next = append(next, owned)
		change.Added = append(change.Added, out)
	}
	for i, row := range s.snapshot {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if !matched[i] {
			out, err := cloneSubscriptionRow(row)
			if err != nil {
				return nil, err
			}
			change.Removed = append(change.Removed, out)
		}
	}
	s.snapshot = next
	s.cols = slices.Clone(rs.Cols)
	s.fullInitialized = true
	if !initial && len(change.Added) == 0 && len(change.Removed) == 0 {
		return nil, nil
	}
	return change, nil
}
