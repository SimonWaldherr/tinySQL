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
	BufferOccupancy int
	BlockedSends    uint64
	SendWait        time.Duration
	StartedAt       time.Time
	FirstRowAt      time.Time
	CompletedAt     time.Time
	RowsScanned     uint64
	RowsProduced    uint64
	BufferCapacity  int
	Materialized    bool
	Complete        bool
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
	blockedSends   atomic.Uint64
	sendWait       atomic.Int64
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
		s.current = nil
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

// ReleaseRow returns row -- which must be the value most recently returned by
// Row, and which the caller must not use again after this call -- to the
// producer's row pool for reuse. It is always safe to call (a no-op if
// pooling isn't in effect for this stream's plan shape; see rawRowPool's doc
// comment in exec_raw_eval.go), but only useful for a caller that, like
// database/sql's driver, is finished with a row's values before it calls
// Next again.
func (s *ResultStream) ReleaseRow(row Row) {
	if s == nil {
		return
	}
	releasePooledRow(row)
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
		StartedAt:       s.startedAt,
		BufferOccupancy: len(s.rows),
		BlockedSends:    s.blockedSends.Load(),
		SendWait:        time.Duration(s.sendWait.Load()),
		RowsScanned:     s.rowsScanned.Load(),
		RowsProduced:    s.rowsProduced.Load(),
		BufferCapacity:  s.bufferCapacity,
		Materialized:    s.materialized.Load(),
		Complete:        s.complete.Load(),
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
	if sel, ok := stmt.(*Select); ok && streamableSimpleSelect(sel) {
		if stream, err := streamSmallSelect(ctx, db, tenant, sel, opts.Buffer); stream != nil || err != nil {
			return stream, err
		}
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

// The synchronous first phase of a streamed SELECT. A request that touches a
// few rows -- a point lookup by key, a viewport query, a small LIMIT -- is
// answered before ExecuteStream returns; only when the scan outgrows these
// budgets does a producer goroutine take over, from exactly where the first
// phase stopped.
const (
	// syncStreamMaxRows is the most rows the first phase produces.
	syncStreamMaxRows = 64
	// syncStreamMaxCandidates is the most candidate rows it examines.
	syncStreamMaxCandidates = 2048
)

// closedResultStreamDone is the Done channel of every stream whose production
// finished before it was returned.
var closedResultStreamDone = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// simpleScanState is the position of a simple-plan scan, so that it can be
// handed from the synchronous first phase to the producer goroutine.
type simpleScanState struct {
	next    int    // next candidate position to examine
	matched int    // matches seen, for OFFSET
	emitted int    // rows produced, for LIMIT
	scanned uint64 // candidates examined
}

// streamSmallSelect starts a streamable SELECT synchronously.
//
// A point lookup is the request a tile server or an API makes thousands of times
// a second; for it the goroutine start, the channel hand-offs and the scheduler
// wake-ups cost more than the lookup itself. The first phase evaluates rows here,
// under the read lock the producer would hold, into the stream's buffer. If the
// whole result fits within the budgets, the returned stream is already complete:
// same API, but Next never blocks and no goroutine exists. Otherwise the
// producer goroutine continues the scan from the first phase's position, so the
// work is neither repeated nor reordered.
//
// It returns (nil, nil) when the statement does not qualify -- a caller that
// asked for a small buffer (it asked for backpressure), ORDER BY, a paged source
// -- and ExecuteStreamWithOptions then streams as before. Planning errors are
// reported here exactly as the producer reports them.
func streamSmallSelect(ctx context.Context, db *storage.DB, tenant string, sel *Select, buffer int) (*ResultStream, error) {
	// Rows are buffered before any consumer exists, so they must fit the
	// requested buffer for the result to be indistinguishable from the
	// producer's.
	maxRows := min(syncStreamMaxRows, buffer)
	if maxRows <= 0 {
		return nil, nil
	}
	if err := checkPermission(ctx, db, sel); err != nil {
		recordAudit(ctx, db, tenant, sel, err)
		return nil, err
	}
	db.LockContentForRead()
	locked := true
	defer func() {
		if locked {
			db.UnlockContentForRead()
		}
	}()
	env := ExecEnv{
		ctx:           ctx,
		tenant:        tenant,
		db:            db,
		now:           time.Now(),
		subqueryCache: newSubqueryResultCache(),
	}
	plan, handled, err := buildStreamingSimpleSelectPlan(env, sel)
	if err != nil {
		recordAudit(ctx, db, tenant, sel, err)
		return nil, err
	}
	if !handled || len(plan.orderBy) != 0 {
		return nil, nil
	}
	if plan.pagedSource != nil {
		if !plan.pagedSingleRow {
			return nil, nil
		}
		return streamSmallPagedSelect(ctx, db, tenant, sel, plan, buffer)
	}
	if err := checkCtx(ctx); err != nil {
		err = context.Cause(ctx)
		recordAudit(ctx, db, tenant, sel, err)
		return nil, err
	}

	rows := simplePlanRows(plan)
	candidates := len(rows)
	if plan.rowIDs != nil {
		candidates = len(plan.rowIDs)
	}
	offset := 0
	if plan.offset != nil && *plan.offset > 0 {
		offset = *plan.offset
	}
	limit := -1
	if plan.limit != nil {
		limit = *plan.limit
	}

	var out [syncStreamMaxRows]Row
	produced := 0
	var st simpleScanState
	finished := limit == 0
	// An evaluation error after earlier rows is delivered the way the producer
	// delivers it: the rows first, the error through Err.
	var evalErr error
	collect := func() {
		// The producer goroutine turns a panic while evaluating a row into an
		// error; keep that behavior here.
		defer func() {
			if recovered := recover(); recovered != nil {
				evalErr = fmt.Errorf("internal error executing streamed statement: %v", recovered)
			}
		}()
		budget := min(candidates, syncStreamMaxCandidates)
		for st.next < budget {
			i := st.next
			rowID := i
			if plan.rowIDs != nil {
				rowID = plan.rowIDs[i]
			}
			if rowID < 0 || rowID >= len(rows) {
				evalErr = fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
				return
			}
			st.next++
			st.scanned++
			raw := rows[rowID]
			match := plan.filterFullyCovered
			if !match {
				var err error
				if match, err = evalRawWhere(plan, raw); err != nil {
					evalErr = err
					return
				}
			}
			if !match {
				continue
			}
			if st.matched < offset {
				st.matched++
				continue
			}
			row, err := projectRawRowPooled(plan, raw)
			if err != nil {
				evalErr = err
				return
			}
			out[produced] = row
			produced++
			st.emitted++
			if limit >= 0 && st.emitted >= limit {
				finished = true
				return
			}
			if produced >= maxRows {
				return
			}
		}
	}
	if !finished {
		collect()
	}
	if evalErr != nil || finished || st.next >= candidates {
		recordAudit(ctx, db, tenant, sel, evalErr)
		return completedResultStream(ctx, plan.outputCols, out[:produced], st.scanned, evalErr, buffer), nil
	}

	// The scan outgrew the first phase: a producer continues it. Hand over what
	// the producer's own startup would have established -- the table pin that
	// lets it outlive the read lock, or the lock itself.
	streamCtx, cancel := context.WithCancelCause(ctx)
	ch := make(chan Row, buffer)
	for _, row := range out[:produced] {
		ch <- row
	}
	stream := &ResultStream{
		Cols:           append([]string(nil), plan.outputCols...),
		ctx:            streamCtx,
		cancel:         cancel,
		rows:           ch,
		done:           make(chan struct{}),
		startedAt:      time.Now(),
		bufferCapacity: buffer,
	}
	stream.rowsProduced.Store(uint64(produced))
	if produced > 0 {
		stream.firstRowAt.Store(time.Now().UnixNano())
	}
	// The scan loop publishes candidates in groups of 64 and adds the remainder
	// when it ends; start from the groups the first phase already completed.
	stream.rowsScanned.Store(st.scanned &^ 63)
	var releasePin func()
	if release, pinned := db.PinTableForStream(plan.table); pinned {
		releasePin = release
		db.UnlockContentForRead()
		locked = false
	}
	go produceStreamTail(stream, plan, st, db, tenant, sel, releasePin, locked)
	locked = false // ownership of the read lock, if still held, moved to the producer
	return stream, nil
}

// produceStreamTail continues a simple-plan scan from st on its own goroutine.
// It releases the read lock or table pin it was handed, audits the statement and
// finishes the stream, as produceResultStream does for a stream it started.
func produceStreamTail(stream *ResultStream, plan *simpleSelectPlan, st simpleScanState, db *storage.DB, tenant string, stmt Statement, releasePin func(), locked bool) {
	var err error
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
		recordAudit(stream.ctx, db, tenant, stmt, err)
		stream.finish(err)
	}()
	err = streamSimpleSelectPlanFrom(stream, plan, db, st)
}

// streamSmallPagedSelect is streamSmallSelect for a paged (read-only on-disk)
// point seek on a unique index, which yields at most one row. The cursor is
// drained here, under the read lock, so no producer goroutine is needed to
// serve a tile or a record by key.
func streamSmallPagedSelect(ctx context.Context, db *storage.DB, tenant string, sel *Select, plan *simpleSelectPlan, buffer int) (*ResultStream, error) {
	source := plan.pagedSource
	var out []Row
	var scanned uint64
	var evalErr error
	run := func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				evalErr = fmt.Errorf("internal error executing streamed statement: %v", recovered)
			}
		}()
		cursor, ok, err := db.OpenPagedIndexRangeCursor(source.tenant, source.table, source.indexName, source.startKey, source.endKey)
		if err == nil && (!ok || cursor == nil) {
			err = fmt.Errorf("paged stream source is unavailable")
		}
		if err != nil {
			evalErr = err
			return
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
			return
		}
		matched := 0
		for !cursor.Done() {
			if err := checkCtx(ctx); err != nil {
				evalErr = context.Cause(ctx)
				return
			}
			batch, err := cursor.NextBatch(pagedResultStreamBatchRows)
			if err != nil {
				evalErr = err
				return
			}
			for _, raw := range batch {
				scanned++
				match := plan.filterFullyCovered
				if !match {
					if match, err = evalRawWhere(plan, raw); err != nil {
						evalErr = err
						return
					}
				}
				if !match {
					continue
				}
				if matched < offset {
					matched++
					continue
				}
				row, err := projectRawRowPooled(plan, raw)
				if err != nil {
					evalErr = err
					return
				}
				out = append(out, row)
				if limit >= 0 && len(out) >= limit {
					return
				}
			}
		}
	}
	run()
	if evalErr != nil && len(out) == 0 {
		// Nothing was produced: report like the producer does for a source that
		// cannot be opened or read.
		recordAudit(ctx, db, tenant, sel, evalErr)
		if errors.Is(evalErr, context.Canceled) || errors.Is(evalErr, context.DeadlineExceeded) {
			return nil, evalErr
		}
		return failedResultStream(ctx, plan.outputCols, evalErr, buffer), nil
	}
	recordAudit(ctx, db, tenant, sel, evalErr)
	return completedResultStream(ctx, plan.outputCols, out, scanned, evalErr, buffer), nil
}

// completedResultStream wraps already-produced rows in a finished ResultStream.
func completedResultStream(ctx context.Context, cols []string, rows []Row, scanned uint64, err error, buffer int) *ResultStream {
	ch := make(chan Row, len(rows))
	for _, row := range rows {
		ch <- row
	}
	close(ch)
	now := time.Now()
	stream := &ResultStream{
		Cols:           append([]string(nil), cols...),
		ctx:            ctx,
		cancel:         func(error) {},
		rows:           ch,
		done:           closedResultStreamDone,
		producerErr:    err,
		startedAt:      now,
		bufferCapacity: buffer,
	}
	stream.rowsScanned.Store(scanned)
	stream.rowsProduced.Store(uint64(len(rows)))
	if len(rows) > 0 {
		stream.firstRowAt.Store(now.UnixNano())
	}
	stream.completedAt.Store(now.UnixNano())
	stream.complete.Store(true)
	return stream
}

func failedResultStream(ctx context.Context, cols []string, err error, buffer int) *ResultStream {
	return completedResultStream(ctx, cols, nil, 0, err, buffer)
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

// streamableSimpleSelect additionally excludes DISTINCT, which
// simpleSelectEligible permits for executeSimpleSelectFastPath's sake. That
// path dedupes on projected values; streamSimpleSelectPlan emits every match as
// it is scanned and has nowhere to hold the seen-set, so a streamed DISTINCT
// would return duplicates. It takes the materialized route instead.
func streamableSimpleSelect(s *Select) bool {
	return s != nil && !s.Distinct && len(s.OrderBy) == 0 && simpleSelectEligible(s)
}

// MayStreamIncrementally reports whether stmt has a shape for which
// ExecuteStream may produce rows directly from the source scan. It is a
// deliberately conservative syntactic test: a true result can still fall
// back to materialization when the runtime source is a view or another source
// the simple plan cannot handle, while false guarantees that ExecuteStream
// would only add a goroutine and channel around an already materialized
// ResultSet.
//
// The database/sql adapter uses this distinction to keep genuine large-result
// streaming while returning blocking shapes such as ORDER BY, GROUP BY,
// DISTINCT, joins, CTEs and EXPLAIN synchronously.
func MayStreamIncrementally(stmt Statement) bool {
	s, ok := stmt.(*Select)
	return ok && streamableSimpleSelect(s)
}

func streamSimpleSelectPlan(stream *ResultStream, plan *simpleSelectPlan, db *storage.DB) error {
	if plan.pagedSource != nil {
		return streamPagedSimpleSelectPlan(stream, plan, db)
	}
	return streamSimpleSelectPlanFrom(stream, plan, db, simpleScanState{})
}

// streamSimpleSelectPlanFrom runs the scan from position st, which is the zero
// state for a scan that has not started.
func streamSimpleSelectPlanFrom(stream *ResultStream, plan *simpleSelectPlan, db *storage.DB, st simpleScanState) error {
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

	matched, emitted := st.matched, st.emitted
	scanned := st.scanned
	defer func() {
		// Publish the unbatched tail so completed stream statistics are exact.
		if remainder := scanned & 63; remainder != 0 {
			stream.rowsScanned.Add(remainder)
		}
	}()
	for i := st.next; i < rowCount; i++ {
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
		out, err := projectRawRowPooled(plan, raw)
		if err != nil {
			return err
		}
		if !sendResultStreamRow(stream.ctx, stream, out) {
			releasePooledRow(out)
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
			out, err := projectRawRowPooled(plan, raw)
			if err != nil {
				return err
			}
			if !sendResultStreamRow(stream.ctx, stream, out) {
				releasePooledRow(out)
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
	// Most sends fit in the buffer. Avoid the blocking two-channel select on
	// that path, while checking cancellation before accepting another row.
	select {
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case stream.rows <- row:
		stream.noteProduced()
		return true
	default:
	}
	stream.blockedSends.Add(1)
	start := time.Now()
	defer func() { stream.sendWait.Add(time.Since(start).Nanoseconds()) }()
	select {
	case stream.rows <- row:
		stream.noteProduced()
		return true
	case <-ctx.Done():
		return false
	}
}
