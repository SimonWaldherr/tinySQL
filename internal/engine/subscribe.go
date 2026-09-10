package engine

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// QueryChange is a multiset delta: remove one occurrence for every Removed row,
// then add each Added row. Initial replaces the entire result. There is no row
// ordering contract. Slow readers may observe several commits as one delta.
type QueryChange struct {
	Initial bool
	Cols    []string
	Added   []Row
	Removed []Row
	// ScannedRows counts candidates evaluated for this change, useful for
	// observing incremental versus fallback execution.
	ScannedRows int
}

// QuerySubscription owns one worker and a one-element output queue. Close or
// cancel the context when done. Errors terminate Changes and are available in Err.
type QuerySubscription struct {
	Changes <-chan QueryChange
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	err     error
}

func (s *QuerySubscription) Close()     { s.cancel(); <-s.done }
func (s *QuerySubscription) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// SubscribeSQL supports deterministic single-table SELECTs with direct columns
// or *, and simple WHERE expressions. ORDER BY, paging, joins and aggregates
// are rejected rather than silently providing different semantics.
func SubscribeSQL(ctx context.Context, db *storage.DB, tenant, query string) (*QuerySubscription, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}
	stmt, err := NewParser(query).ParseStatement()
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*Select)
	if !ok || !simpleSelectEligible(sel) || sel.Distinct || len(sel.OrderBy) > 0 || sel.Limit != nil || sel.Offset != nil || !subscriptionPredicate(sel.Where) {
		return nil, fmt.Errorf("subscription requires a single-table SELECT without ordering, paging, aggregation or joins")
	}
	for _, p := range sel.Projs {
		if _, ok := p.Expr.(*VarRef); !p.Star && !ok {
			return nil, fmt.Errorf("subscription projections must be columns or *")
		}
	}
	wake, unregister, err := db.WatchChanges()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	state := &querySubscriptionState{db: db, tenant: tenant, query: sel, rows: make(map[int]Row)}
	initial, err := state.refresh(ctx)
	if err != nil {
		unregister()
		cancel()
		return nil, err
	}
	output := make(chan QueryChange, 1)
	output <- *initial
	sub := &QuerySubscription{Changes: output, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(sub.done)
		defer close(output)
		defer unregister()
		defer cancel()
		dirty := false
		for {
			if !dirty {
				select {
				case <-ctx.Done():
					return
				case _, open := <-wake:
					if !open {
						return
					}
				}
			}
			dirty = false
			change, err := state.refresh(ctx)
			if err != nil {
				sub.mu.Lock()
				sub.err = err
				sub.mu.Unlock()
				return
			}
			if change == nil {
				continue
			}
		send:
			for {
				select {
				case output <- *change:
					break send
				case <-ctx.Done():
					return
				case _, open := <-wake:
					if !open {
						return
					}
					dirty = true
				}
			}
		}
	}()
	return sub, nil
}

func subscriptionPredicate(e Expr) bool {
	switch ex := e.(type) {
	case nil, *Literal, *VarRef:
		return true
	case *Binary:
		return subscriptionPredicate(ex.Left) && subscriptionPredicate(ex.Right)
	case *Unary:
		return subscriptionPredicate(ex.Expr)
	case *IsNull:
		return subscriptionPredicate(ex.Expr)
	default:
		return false
	}
}

type querySubscriptionState struct {
	db                         *storage.DB
	tenant                     string
	query                      *Select
	table                      *storage.Table
	version, structural, count int
	cols                       []string
	rows                       map[int]Row
}

func (s *querySubscriptionState) refresh(ctx context.Context) (result *QueryChange, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("internal error refreshing subscription: %v", recovered)
		}
	}()
	s.db.LockContentForRead()
	defer s.db.UnlockContentForRead()
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := checkPermission(ctx, s.db, s.query); err != nil {
		return nil, err
	}
	table, err := s.db.Get(s.tenant, s.query.From.Table)
	if err != nil {
		return nil, err
	}
	initial := s.table == nil
	if !initial && table == s.table && table.Version == s.version {
		return nil, nil
	}
	template, ok, err := loadSimpleSelectPlanTemplate(table, s.query, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("SELECT is not supported by subscriptions")
	}
	if !initial && !slices.Equal(s.cols, template.outputCols) {
		return nil, fmt.Errorf("subscription result schema changed")
	}
	change := &QueryChange{Initial: initial, Cols: append([]string(nil), template.outputCols...)}
	updated, incremental := table.UpdatedRowsSince(s.structural)
	incremental = !initial && table == s.table && len(table.Rows) >= s.count && incremental
	var candidates []int
	if incremental {
		for _, i := range updated {
			if i < s.count {
				candidates = append(candidates, i)
			}
		}
		for i := s.count; i < len(table.Rows); i++ {
			candidates = append(candidates, i)
		}
		sort.Ints(candidates)
	}
	next := s.rows
	if !incremental {
		next = make(map[int]Row)
	}
	candidateCount := len(candidates)
	if !incremental {
		candidateCount = len(table.Rows)
	}
	for candidate := 0; candidate < candidateCount; candidate++ {
		i := candidate
		if incremental {
			i = candidates[candidate]
		}
		if change.ScannedRows&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		change.ScannedRows++
		match, err := evalRawWhere(template, table.Rows[i])
		if err != nil {
			return nil, err
		}
		var current Row
		if match {
			row, err := projectRawRow(template, table.Rows[i])
			if err != nil {
				return nil, err
			}
			current = row
		}
		previous := s.rows[i]
		if reflect.DeepEqual(previous, current) {
			// Keep the owned snapshot when the projection has not changed.
			current = previous
		} else {
			if previous != nil {
				out, err := cloneSubscriptionRow(previous)
				if err != nil {
					return nil, err
				}
				change.Removed = append(change.Removed, out)
			}
			if current != nil {
				// The projected values still belong to storage. Take a snapshot
				// only for changed rows, then isolate the reader's copy as well.
				current, err = cloneSubscriptionRow(current)
				if err != nil {
					return nil, err
				}
				out, err := cloneSubscriptionRow(current)
				if err != nil {
					return nil, err
				}
				change.Added = append(change.Added, out)
			}
		}
		if current != nil {
			next[i] = current
		} else {
			delete(next, i)
		}
	}
	if !incremental {
		// Rows beyond the new physical end can disappear after DELETE.
		for i := len(table.Rows); i < s.count; i++ {
			if old := s.rows[i]; old != nil {
				out, err := cloneSubscriptionRow(old)
				if err != nil {
					return nil, err
				}
				change.Removed = append(change.Removed, out)
			}
		}
	}
	s.rows = next
	s.table = table
	s.version = table.Version
	s.structural = table.StructVersion()
	s.count = len(table.Rows)
	s.cols = append(s.cols[:0], template.outputCols...)
	if !initial && len(change.Added) == 0 && len(change.Removed) == 0 {
		return nil, nil
	}
	return change, nil
}

func cloneSubscriptionRow(row Row) (Row, error) {
	out := make(Row, len(row))
	for key, v := range row {
		value, err := cloneSubscriptionValue(v)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}
func cloneSubscriptionValue(v any) (any, error) {
	switch x := v.(type) {
	case []byte:
		if x == nil {
			return []byte(nil), nil
		}
		return append([]byte{}, x...), nil
	case []float64:
		if x == nil {
			return []float64(nil), nil
		}
		return append([]float64{}, x...), nil
	case []any:
		if x == nil {
			return []any(nil), nil
		}
		out := make([]any, len(x))
		for i, v := range x {
			copy, err := cloneSubscriptionValue(v)
			if err != nil {
				return nil, err
			}
			out[i] = copy
		}
		return out, nil
	case map[string]any:
		if x == nil {
			return map[string]any(nil), nil
		}
		out := make(map[string]any, len(x))
		for k, v := range x {
			copy, err := cloneSubscriptionValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = copy
		}
		return out, nil
	case time.Time:
		return x, nil
	case big.Int:
		return *new(big.Int).Set(&x), nil
	case big.Rat:
		return *new(big.Rat).Set(&x), nil
	case *big.Int:
		if x == nil {
			return (*big.Int)(nil), nil
		}
		return new(big.Int).Set(x), nil
	case *big.Rat:
		if x == nil {
			return (*big.Rat)(nil), nil
		}
		return new(big.Rat).Set(x), nil
	}
	if v != nil {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Array:
			kind := reflect.TypeOf(v).Elem().Kind()
			if kind != reflect.String && (kind < reflect.Bool || kind > reflect.Complex128) {
				return nil, fmt.Errorf("unsupported mutable subscription value %T", v)
			}
		case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Func, reflect.Chan, reflect.Struct:
			return nil, fmt.Errorf("unsupported mutable subscription value %T", v)
		}
	}
	return v, nil
}
