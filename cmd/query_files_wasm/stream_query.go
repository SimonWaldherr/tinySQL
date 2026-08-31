//go:build js && wasm

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// The stream path is deliberately a bounded preview, not a second unbounded
// result cache. It gives the browser a practical escape hatch for wide table
// scans while preserving the existing materialized/paged path for scripts and
// full-result exports.
const (
	streamPreviewMaxRows  = 10_000
	streamPreviewMaxBytes = 16 * 1024 * 1024
	streamPumpRows        = 128
	streamPumpBudget      = 8 * time.Millisecond
)

type streamQueryOperation struct {
	requestID int
	sql       string
	startedAt time.Time
	request   runtimeRequest

	ctx    context.Context
	cancel context.CancelFunc

	stream    *tinysql.ResultStream
	columns   []string
	rows      []tinysql.Row
	resultB   int64
	truncated bool
	reason    string
	settled   bool

	resolve js.Value
	pump    js.Func
}

var activeStreamQueries = struct {
	sync.Mutex
	queries map[int]*streamQueryOperation
}{queries: make(map[int]*streamQueryOperation)}

// executeQueryStream starts one single-statement query as a Promise. The
// worker passes its request id as the first argument; it is intentionally not
// part of the public UI API. Work is consumed in small timer-scheduled chunks
// so the Worker can process a cancellation message between result batches.
func executeQueryStream(_ js.Value, args []js.Value) interface{} {
	request := telemetry.begin("query")
	if len(args) < 2 || args[0].Type() != js.TypeNumber {
		telemetry.finish(request, false, false)
		return jsErr("Usage: executeQueryStream(requestId, sqlQuery)")
	}

	requestID := args[0].Int()
	if requestID < 1 {
		telemetry.finish(request, false, false)
		return jsErr("stream request id must be a positive integer")
	}
	query, err := normalizeSQLInput(args[1].String())
	if err != nil {
		telemetry.finish(request, false, false)
		return jsErr(err.Error())
	}

	// Do not leave an old full result available for export while this bounded
	// preview is running (or after it is cancelled).
	lastResult = nil
	lastResultPager.reset()

	return streamPromise(func(resolve js.Value) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
		op := &streamQueryOperation{
			requestID: requestID,
			sql:       query,
			startedAt: time.Now(),
			request:   request,
			ctx:       ctx,
			cancel:    cancel,
			resolve:   resolve,
		}
		op.pump = js.FuncOf(op.run)
		activeStreamQueries.Lock()
		activeStreamQueries.queries[requestID] = op
		activeStreamQueries.Unlock()
		op.emit("queued", nil)
		op.schedule()
	})
}

// cancelQueryStream is called directly by wasm-worker.js instead of entering
// its serialized RPC queue. Context cancellation wakes ResultStream producers
// and lets the scheduled consumer resolve promptly with a cancelled result.
func cancelQueryStream(_ js.Value, args []js.Value) interface{} {
	if len(args) < 1 || args[0].Type() != js.TypeNumber {
		return jsErr("Usage: cancelQueryStream(requestId)")
	}
	requestID := args[0].Int()
	activeStreamQueries.Lock()
	op := activeStreamQueries.queries[requestID]
	activeStreamQueries.Unlock()
	if op == nil || op.settled {
		return map[string]interface{}{"success": true, "cancelled": false}
	}
	op.cancel()
	op.emit("cancelling", nil)
	return map[string]interface{}{"success": true, "cancelled": true}
}

func streamPromise(start func(resolve js.Value)) js.Value {
	executor := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) > 0 {
			start(args[0])
		}
		return nil
	})
	promise := js.Global().Get("Promise").New(executor)
	executor.Release()
	return promise
}

func (op *streamQueryOperation) schedule() {
	js.Global().Call("setTimeout", op.pump.Value, 0)
}

func (op *streamQueryOperation) run(_ js.Value, _ []js.Value) interface{} {
	if op.settled {
		return nil
	}
	if err := op.ctx.Err(); err != nil {
		op.finishError(err)
		return nil
	}
	if op.stream == nil {
		if err := op.start(); err != nil {
			op.finishError(err)
			return nil
		}
	}

	deadline := time.Now().Add(streamPumpBudget)
	batch := make([]tinysql.Row, 0, streamPumpRows)
	for rowCount := 0; rowCount < streamPumpRows; rowCount++ {
		if err := op.ctx.Err(); err != nil {
			_ = op.stream.Close()
			op.finishError(err)
			return nil
		}
		if !op.stream.Next() {
			op.emitChunk(batch)
			if err := op.stream.Err(); err != nil {
				op.finishError(err)
			} else {
				op.finishSuccess()
			}
			return nil
		}

		row := op.stream.Row()
		rowBytes, err := streamRowBytes(op.columns, row)
		if err != nil {
			_ = op.stream.Close()
			op.finishError(fmt.Errorf("encode streamed row: %w", err))
			return nil
		}
		// Consume one extra row at the row cap. That distinguishes a result
		// with exactly the cap from one that really was truncated.
		if len(op.rows) >= streamPreviewMaxRows {
			op.emitChunk(batch)
			op.finishTruncated("row limit")
			return nil
		}
		if op.resultB+rowBytes > streamPreviewMaxBytes {
			op.emitChunk(batch)
			op.finishTruncated("byte limit")
			return nil
		}
		op.rows = append(op.rows, row)
		batch = append(batch, row)
		op.resultB += rowBytes

		if time.Now().After(deadline) {
			break
		}
	}
	op.emitChunk(batch)
	op.emit("progress", nil)
	op.schedule()
	return nil
}

func (op *streamQueryOperation) start() error {
	compiled, err := queryCache.Compile(op.sql)
	if err != nil {
		return fmt.Errorf("Parse error: %w", err)
	}
	stream, err := tinysql.ExecuteCompiledStreamWithOptions(
		op.ctx,
		db,
		tenant,
		compiled,
		tinysql.StreamOptions{Buffer: 0},
	)
	if err != nil {
		if errors.Is(op.ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("Query timeout after %s", defaultQueryTimeout)
		}
		return fmt.Errorf("Execute error: %w", err)
	}
	op.stream = stream
	op.columns = stream.Columns()
	op.emit("started", map[string]interface{}{
		"columns": stringsToInterfaces(op.columns),
	})
	return nil
}

func streamRowBytes(columns []string, row tinysql.Row) (int64, error) {
	encoded, err := json.Marshal(resultRowToJS(columns, row))
	if err != nil {
		return 0, err
	}
	// Include one JSON separator/newline byte, matching the server's streaming
	// response limiter closely enough to make the browser budget predictable.
	return int64(len(encoded)) + 1, nil
}

func (op *streamQueryOperation) finishTruncated(reason string) {
	op.truncated = true
	op.reason = reason
	if err := op.stream.Close(); err != nil {
		op.finishError(err)
		return
	}
	op.finishSuccess()
}

func (op *streamQueryOperation) emitChunk(rows []tinysql.Row) {
	if len(rows) == 0 {
		return
	}
	op.emit("chunk", map[string]interface{}{
		"columns": stringsToInterfaces(op.columns),
		"rows":    resultRowsToJS(op.columns, rows),
	})
}

func (op *streamQueryOperation) finishSuccess() {
	if op.settled {
		return
	}
	lastQueryDurMs = durationMilliseconds(op.startedAt)
	stats := op.stream.Stats()
	// Keep a complete bounded result available for the existing WASM pager and
	// exporter. Once the preview hit a cap this is intentionally only a prefix,
	// so the UI marks it as preview-only and disables full-result export.
	lastResult = &tinysql.ResultSet{
		Cols: append([]string(nil), op.columns...),
		Rows: append([]tinysql.Row(nil), op.rows...),
	}
	lastResultPager.reset()
	page := lastResultPager.page(lastResult, 0, defaultResultPageSize, "", "", "asc")
	payload := map[string]interface{}{
		"success":          true,
		"columns":          stringsToInterfaces(op.columns),
		"rows":             resultRowsToJS(op.columns, page.Rows),
		"totalRows":        page.TotalRows,
		"filteredRows":     page.FilteredRows,
		"pageOffset":       page.Offset,
		"pageLimit":        page.Limit,
		"serverPaged":      true,
		"durationMs":       lastQueryDurMs,
		"streamed":         true,
		"previewOnly":      op.truncated,
		"truncated":        op.truncated,
		"truncationReason": op.reason,
		"resultLimitRows":  streamPreviewMaxRows,
		"resultLimitBytes": streamPreviewMaxBytes,
		"rowsScanned":      float64(stats.RowsScanned),
		"rowsProduced":     float64(stats.RowsProduced),
		"rowsRetained":     len(op.rows),
		"resultBytes":      op.resultB,
		"materialized":     stats.Materialized,
	}
	op.finish(payload, true, false)
}

func (op *streamQueryOperation) finishError(err error) {
	if op.settled {
		return
	}
	if op.stream != nil {
		_ = op.stream.Close()
	}
	timedOut := errors.Is(err, context.DeadlineExceeded) || errors.Is(op.ctx.Err(), context.DeadlineExceeded)
	message := err.Error()
	if timedOut {
		message = "Query timeout after " + defaultQueryTimeout.String()
	} else if errors.Is(err, context.Canceled) || errors.Is(op.ctx.Err(), context.Canceled) {
		message = "Query cancelled"
	}
	op.finish(map[string]interface{}{
		"success":  false,
		"error":    message,
		"streamed": true,
	}, false, timedOut)
}

func (op *streamQueryOperation) finish(payload map[string]interface{}, success, timedOut bool) {
	if op.settled {
		return
	}
	op.settled = true
	op.cancel()
	activeStreamQueries.Lock()
	if activeStreamQueries.queries[op.requestID] == op {
		delete(activeStreamQueries.queries, op.requestID)
	}
	activeStreamQueries.Unlock()
	telemetry.finish(op.request, success, timedOut)
	op.emit("complete", map[string]interface{}{
		"success":   success,
		"truncated": op.truncated,
	})
	op.resolve.Invoke(streamJSValue(payload))
	op.pump.Release()
}

func (op *streamQueryOperation) emit(phase string, extra map[string]interface{}) {
	reporter := js.Global().Get("__tinySQLReportQueryStream")
	if reporter.Type() != js.TypeFunction {
		return
	}
	payload := map[string]interface{}{
		"phase":        phase,
		"elapsedMs":    durationMilliseconds(op.startedAt),
		"rowsRetained": len(op.rows),
		"resultBytes":  op.resultB,
	}
	if op.stream != nil {
		stats := op.stream.Stats()
		payload["rowsScanned"] = float64(stats.RowsScanned)
		payload["rowsProduced"] = float64(stats.RowsProduced)
		payload["materialized"] = stats.Materialized
	}
	for key, value := range extra {
		payload[key] = value
	}
	reporter.Invoke(op.requestID, streamJSValue(payload))
}

func durationMilliseconds(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

func streamJSValue(value interface{}) js.Value {
	encoded, err := json.Marshal(value)
	if err != nil {
		return js.Global().Get("Object").New()
	}
	return js.Global().Get("JSON").Call("parse", string(encoded))
}
