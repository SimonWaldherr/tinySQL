package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// DefaultResultStreamBuffer is the number of produced rows that can wait for
// a consumer when callers use ExecuteStream. It keeps the convenient API
// responsive without letting an abandoned or slow consumer retain an
// unbounded result in memory.
const DefaultResultStreamBuffer = 64

var errResultStreamClosed = errors.New("result stream closed")

// StreamOptions controls the producer/consumer boundary of a ResultStream.
//
// Buffer is the maximum number of produced rows waiting for a consumer. Zero
// is intentionally valid and provides strict backpressure: a producer waits
// for every call to Next. ExecuteStream uses DefaultResultStreamBuffer; use
// ExecuteStreamWithOptions to select zero or another capacity explicitly.
type StreamOptions struct {
	Buffer int
}

// StreamStats is a point-in-time, concurrency-safe snapshot of a query
// stream. RowsScanned is populated for direct simple-scan streams (while a
// stream is running it may trail by up to 63 candidates to avoid adding an
// atomic operation to every hot-loop iteration); a materialized query cannot
// in general expose the executor's intermediate candidate count and reports
// zero for that field. RowsProduced is the number of result rows accepted by
// the stream's producer.
type StreamStats struct {
	StartedAt      time.Time
	FirstRowAt     time.Time
	CompletedAt    time.Time
	RowsScanned    uint64
	RowsProduced   uint64
	BufferCapacity int
	Materialized   bool
	Complete       bool
}

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

	errMu       sync.RWMutex
	current     Row
	producerErr error

	startedAt      time.Time
	firstRowAt     atomic.Int64
	completedAt    atomic.Int64
	rowsScanned    atomic.Uint64
	rowsProduced   atomic.Uint64
	materialized   atomic.Bool
	bufferCapacity int
	complete       atomic.Bool
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
	s.current = row
	return true
}

// Row returns the row selected by the most recent successful Next call.
func (s *ResultStream) Row() Row {
	if s == nil {
		return nil
	}
	return s.current
}

// Err reports the terminal producer or context error. Closing a stream
// explicitly is not an error.
func (s *ResultStream) Err() error {
	if s == nil {
		return nil
	}
	s.errMu.RLock()
	err := s.producerErr
	s.errMu.RUnlock()
	if errors.Is(err, errResultStreamClosed) {
		return nil
	}
	return err
}

// Done is closed after the producer has stopped and no longer accesses the
// statement or database. Consumers can use it to release resources that only
// need to outlive production rather than buffered-row consumption.
func (s *ResultStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

// Stats returns a snapshot of stream progress. It is safe to call while Next
// is blocked or while another goroutine is producing rows.
func (s *ResultStream) Stats() StreamStats {
	if s == nil {
		return StreamStats{}
	}
	stats := StreamStats{
		StartedAt:      s.startedAt,
		RowsScanned:    s.rowsScanned.Load(),
		RowsProduced:   s.rowsProduced.Load(),
		BufferCapacity: s.bufferCapacity,
		Materialized:   s.materialized.Load(),
		Complete:       s.complete.Load(),
	}
	if ns := s.firstRowAt.Load(); ns != 0 {
		stats.FirstRowAt = time.Unix(0, ns)
	}
	if ns := s.completedAt.Load(); ns != 0 {
		stats.CompletedAt = time.Unix(0, ns)
	}
	return stats
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
	s.errMu.Lock()
	s.producerErr = err
	s.errMu.Unlock()
	s.completedAt.Store(time.Now().UnixNano())
	s.complete.Store(true)
	close(s.rows)
	close(s.done)
}

func (s *ResultStream) noteProduced() {
	// There is exactly one producer. Use the increment result to identify the
	// first row instead of reading the clock and attempting a CAS for every row.
	if s.rowsProduced.Add(1) == 1 {
		s.firstRowAt.Store(time.Now().UnixNano())
	}
}

type resultStreamHeader struct {
	cols []string
	err  error
}

// ExecuteStream starts a statement and returns as soon as its result columns
// are known. See ResultStream for which SELECT shapes produce rows before the
// full query has completed.
func ExecuteStream(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultStream, error) {
	return ExecuteStreamWithOptions(ctx, db, tenant, stmt, StreamOptions{Buffer: DefaultResultStreamBuffer})
}

// ExecuteStreamWithOptions starts a statement with explicit producer/consumer
// backpressure settings. It otherwise has the same semantics as
// ExecuteStream.
func ExecuteStreamWithOptions(ctx context.Context, db *storage.DB, tenant string, stmt Statement, opts StreamOptions) (*ResultStream, error) {
	if db == nil {
		return nil, fmt.Errorf("cannot stream with a nil database")
	}
	if stmt == nil {
		return nil, fmt.Errorf("cannot stream a nil statement")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Buffer < 0 {
		return nil, fmt.Errorf("stream buffer must not be negative: %d", opts.Buffer)
	}
	streamCtx, cancel := context.WithCancelCause(ctx)
	stream := &ResultStream{
		ctx:            streamCtx,
		cancel:         cancel,
		rows:           make(chan Row, opts.Buffer),
		done:           make(chan struct{}),
		startedAt:      time.Now(),
		bufferCapacity: opts.Buffer,
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
		releasePin func()
	)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("internal error executing streamed statement: %v", recovered)
		}
		if locked {
			db.UnlockContentForRead()
		}
		if releasePin != nil {
			releasePin()
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
		plan, handled, planErr := buildStreamingSimpleSelectPlan(env, selectStmt)
		if planErr != nil {
			err = planErr
			audit = true
			return
		}
		if handled && len(plan.orderBy) == 0 {
			// A normal in-memory/direct-backend table is pinned by identity.
			// Future writes copy that one table before mutation, letting the
			// slow consumer outlive contentMu's global read lock without racing
			// row-slice changes. Paged read-only sources use a cursor and do not
			// need a table pin (see streamPagedSimpleSelectPlan).
			if plan.pagedSource != nil {
				db.UnlockContentForRead()
				locked = false
			} else if release, snapshotted := db.PinTableForStream(plan.table); snapshotted {
				releasePin = release
				db.UnlockContentForRead()
				locked = false
			}
			audit = true
			if !sendResultStreamHeader(stream.ctx, header, resultStreamHeader{cols: plan.outputCols}) {
				err = context.Cause(stream.ctx)
				return
			}
			headerSent = true
			err = streamSimpleSelectPlan(stream, plan, db)
			return
		}
		db.UnlockContentForRead()
		locked = false
	}

	// Blocking/global query shapes preserve their existing implementation and
	// stream the materialized result afterward. This keeps ORDER BY, aggregates,
	// DISTINCT, joins, CTEs and DML semantics unchanged.
	stream.materialized.Store(true)
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
		if !sendResultStreamRow(stream.ctx, stream, row) {
			err = context.Cause(stream.ctx)
			return
		}
	}
}

func streamableSimpleSelect(s *Select) bool {
	return s != nil && len(s.OrderBy) == 0 && simpleSelectEligible(s)
}

func streamSimpleSelectPlan(stream *ResultStream, plan *simpleSelectPlan, db *storage.DB) error {
	if plan.pagedSource != nil {
		return streamPagedSimpleSelectPlan(stream, plan, db)
	}
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
	var scanned uint64
	defer func() {
		// Publish the unbatched tail so completed stream statistics are exact.
		if remainder := scanned & 63; remainder != 0 {
			stream.rowsScanned.Add(remainder)
		}
	}()
	for i := 0; i < rowCount; i++ {
		scanned++
		// Hot scans only touch shared state every 64 candidates. Stats observed
		// during a query are therefore at most 63 rows behind, while completed
		// stats remain exact without putting an atomic operation in the inner
		// loop for every candidate.
		if scanned&63 == 0 {
			stream.rowsScanned.Add(64)
		}
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
		if !sendResultStreamRow(stream.ctx, stream, out) {
			return context.Cause(stream.ctx)
		}
		emitted++
		if limit >= 0 && emitted >= limit {
			return nil
		}
	}
	return nil
}

// pagedResultStreamBatchRows bounds decoded source rows while a paged stream
// is waiting on its consumer. The pager lock is released before projection or
// channel sends, so this controls only transient decode memory rather than a
// second producer/consumer queue.
const pagedResultStreamBatchRows = 32

func streamPagedSimpleSelectPlan(stream *ResultStream, plan *simpleSelectPlan, db *storage.DB) error {
	if db == nil || plan == nil || plan.pagedSource == nil {
		return fmt.Errorf("paged stream source is unavailable")
	}
	source := plan.pagedSource
	var (
		cursor *storage.PagedRowCursor
		ok     bool
		err    error
	)
	if source.indexName == "" {
		cursor, ok, err = db.OpenPagedTableCursor(source.tenant, source.table)
	} else {
		cursor, ok, err = db.OpenPagedIndexRangeCursor(source.tenant, source.table, source.indexName, source.startKey, source.endKey)
	}
	if err != nil {
		return err
	}
	if !ok || cursor == nil {
		return fmt.Errorf("paged stream source is unavailable")
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
	var scanned uint64
	defer func() {
		if remainder := scanned & 63; remainder != 0 {
			stream.rowsScanned.Add(remainder)
		}
	}()
	for !cursor.Done() {
		if err := checkCtx(stream.ctx); err != nil {
			return context.Cause(stream.ctx)
		}
		batch, err := cursor.NextBatch(pagedResultStreamBatchRows)
		if err != nil {
			return err
		}
		for _, raw := range batch {
			scanned++
			if scanned&63 == 0 {
				stream.rowsScanned.Add(64)
				if err := checkCtx(stream.ctx); err != nil {
					return context.Cause(stream.ctx)
				}
			}
			match := plan.filterFullyCovered
			if !match {
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
			if !sendResultStreamRow(stream.ctx, stream, out) {
				return context.Cause(stream.ctx)
			}
			emitted++
			if limit >= 0 && emitted >= limit {
				return nil
			}
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

func sendResultStreamRow(ctx context.Context, stream *ResultStream, row Row) bool {
	select {
	case stream.rows <- row:
		stream.noteProduced()
		return true
	case <-ctx.Done():
		return false
	}
}
