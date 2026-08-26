package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const resultStreamBuffer = 64

var errResultStreamClosed = errors.New("result stream closed")

// ResultStream incrementally exposes query rows. Next blocks until another row
// is available, the query finishes, or its context is cancelled. Row is valid
// until the next call to Next. Call Close when abandoning a stream early so
// the producer can release its database read lock promptly.
//
// Simple single-table SELECTs without ORDER BY, GROUP BY, DISTINCT, joins or
// set operations stream directly from the scan. Shapes that need the complete
// input before their first result retain their existing semantics and begin
// yielding after materialization.
type ResultStream struct {
	Cols []string

	ctx    context.Context
	cancel context.CancelCauseFunc
	rows   chan Row
	done   chan struct{}

	mu          sync.RWMutex
	current     Row
	producerErr error
}

// Columns returns the result columns in display order.
func (s *ResultStream) Columns() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.Cols...)
}

// Next advances to the next result row, blocking while the producer continues
// scanning. It returns false at EOF or on error; inspect Err to distinguish the
// two. Next and Row are intended to be called by one consumer goroutine.
func (s *ResultStream) Next() bool {
	if s == nil {
		return false
	}
	row, ok := <-s.rows
	if !ok {
		<-s.done
		return false
	}
	s.mu.Lock()
	s.current = row
	s.mu.Unlock()
	return true
}

// Row returns the row selected by the most recent successful Next call.
func (s *ResultStream) Row() Row {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	row := s.current
	s.mu.RUnlock()
	return row
}

// Err reports the terminal producer or context error. Closing a stream
// explicitly is not an error.
func (s *ResultStream) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	err := s.producerErr
	s.mu.RUnlock()
	if errors.Is(err, errResultStreamClosed) {
		return nil
	}
	return err
}

// Close stops production and waits until any held database read lock has been
// released. It is safe to call repeatedly.
func (s *ResultStream) Close() error {
	if s == nil {
		return nil
	}
	s.cancel(errResultStreamClosed)
	<-s.done
	return s.Err()
}

func (s *ResultStream) finish(err error) {
	s.mu.Lock()
	s.producerErr = err
	s.mu.Unlock()
	close(s.rows)
	close(s.done)
}

type resultStreamHeader struct {
	cols []string
	err  error
}

// ExecuteStream starts a statement and returns as soon as its result columns
// are known. See ResultStream for which SELECT shapes produce rows before the
// full query has completed.
func ExecuteStream(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultStream, error) {
	if db == nil {
		return nil, fmt.Errorf("cannot stream with a nil database")
	}
	if stmt == nil {
		return nil, fmt.Errorf("cannot stream a nil statement")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx, cancel := context.WithCancelCause(ctx)
	stream := &ResultStream{
		ctx:    streamCtx,
		cancel: cancel,
		rows:   make(chan Row, resultStreamBuffer),
		done:   make(chan struct{}),
	}
	header := make(chan resultStreamHeader, 1)
	go produceResultStream(stream, header, db, tenant, stmt)

	select {
	case h := <-header:
		if h.err != nil {
			cancel(h.err)
			<-stream.done
			return nil, h.err
		}
		stream.Cols = append([]string(nil), h.cols...)
		return stream, nil
	case <-streamCtx.Done():
		cancel(context.Cause(streamCtx))
		<-stream.done
		return nil, context.Cause(streamCtx)
	}
}

func produceResultStream(stream *ResultStream, header chan<- resultStreamHeader, db *storage.DB, tenant string, stmt Statement) {
	var (
		err        error
		headerSent bool
		locked     bool
		audit      bool
	)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("internal error executing streamed statement: %v", recovered)
		}
		if locked {
			db.UnlockContentForRead()
		}
		if audit {
			recordAudit(stream.ctx, db, tenant, stmt, err)
		}
		if !headerSent {
			header <- resultStreamHeader{err: err}
		}
		stream.finish(err)
	}()

	if selectStmt, ok := stmt.(*Select); ok && streamableSimpleSelect(selectStmt) {
		if err = checkPermission(stream.ctx, db, stmt); err != nil {
			audit = true
			return
		}
		db.LockContentForRead()
		locked = true
		env := ExecEnv{
			ctx:           stream.ctx,
			tenant:        tenant,
			db:            db,
			now:           time.Now(),
			subqueryCache: newSubqueryResultCache(),
		}
		plan, handled, planErr := buildSimpleSelectPlan(env, selectStmt)
		if planErr != nil {
			err = planErr
			audit = true
			return
		}
		if handled && len(plan.orderBy) == 0 {
			audit = true
			if !sendResultStreamHeader(stream.ctx, header, resultStreamHeader{cols: plan.outputCols}) {
				err = context.Cause(stream.ctx)
				return
			}
			headerSent = true
			err = streamSimpleSelectPlan(stream, plan)
			return
		}
		db.UnlockContentForRead()
		locked = false
	}

	// Blocking/global query shapes preserve their existing implementation and
	// stream the materialized result afterward. This keeps ORDER BY, aggregates,
	// DISTINCT, joins, CTEs and DML semantics unchanged.
	var rs *ResultSet
	rs, err = Execute(stream.ctx, db, tenant, stmt)
	if err != nil {
		return
	}
	if rs == nil {
		rs = &ResultSet{}
	}
	if !sendResultStreamHeader(stream.ctx, header, resultStreamHeader{cols: rs.Cols}) {
		err = context.Cause(stream.ctx)
		return
	}
	headerSent = true
	for _, row := range rs.Rows {
		if !sendResultStreamRow(stream.ctx, stream.rows, row) {
			err = context.Cause(stream.ctx)
			return
		}
	}
}

func streamableSimpleSelect(s *Select) bool {
	return s != nil && len(s.OrderBy) == 0 && simpleSelectEligible(s)
}

func streamSimpleSelectPlan(stream *ResultStream, plan *simpleSelectPlan) error {
	rows := simplePlanRows(plan)
	rowCount := len(rows)
	if plan.rowIDs != nil {
		rowCount = len(plan.rowIDs)
	}
	offset := 0
	if plan.offset != nil && *plan.offset > 0 {
		offset = *plan.offset
	}
	limit := -1
	if plan.limit != nil {
		limit = *plan.limit
	}
	if limit == 0 {
		return nil
	}

	matched, emitted := 0, 0
	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(rows) {
			return fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		if i&63 == 0 {
			if err := checkCtx(stream.ctx); err != nil {
				return context.Cause(stream.ctx)
			}
		}
		raw := rows[rowID]
		match := plan.filterFullyCovered
		if !match {
			var err error
			match, err = evalRawWhere(plan, raw)
			if err != nil {
				return err
			}
		}
		if !match {
			continue
		}
		if matched < offset {
			matched++
			continue
		}
		out, err := projectRawRow(plan, raw)
		if err != nil {
			return err
		}
		if !sendResultStreamRow(stream.ctx, stream.rows, out) {
			return context.Cause(stream.ctx)
		}
		emitted++
		if limit >= 0 && emitted >= limit {
			return nil
		}
	}
	return nil
}

func sendResultStreamHeader(ctx context.Context, dst chan<- resultStreamHeader, h resultStreamHeader) bool {
	select {
	case dst <- h:
		return true
	case <-ctx.Done():
		return false
	}
}

func sendResultStreamRow(ctx context.Context, dst chan<- Row, row Row) bool {
	select {
	case dst <- row:
		return true
	case <-ctx.Done():
		return false
	}
}
