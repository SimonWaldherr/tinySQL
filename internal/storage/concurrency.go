// Package storage - Advanced Concurrency Framework
//
// What: Leverages Go's concurrency primitives (channels, goroutines, context, sync)
//       for high-performance concurrent database operations.
// How: Worker pools, pipeline patterns, fan-out/fan-in, context cancellation,
//      concurrent iterators, and parallel query execution.
// Why: Maximize throughput and resource utilization while maintaining safety.

package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrencyConfig configures the concurrency system.
type ConcurrencyConfig struct {
	// Worker pool sizes
	ReadWorkers  int
	WriteWorkers int

	// Channel buffer sizes
	ReadQueueSize  int
	WriteQueueSize int

	// Timeouts
	WorkerTimeout time.Duration
	QueueTimeout  time.Duration

	// Batch settings
	BatchSize     int
	BatchInterval time.Duration
}

// DefaultConcurrencyConfig returns sensible defaults based on CPU count.
func DefaultConcurrencyConfig() ConcurrencyConfig {
	cpus := runtime.NumCPU()
	return ConcurrencyConfig{
		ReadWorkers:    cpus * 2,   // More readers than CPUs
		WriteWorkers:   cpus,       // One writer per CPU
		ReadQueueSize:  cpus * 100, // Large buffer for reads
		WriteQueueSize: cpus * 50,  // Moderate buffer for writes
		WorkerTimeout:  5 * time.Second,
		QueueTimeout:   1 * time.Second,
		BatchSize:      100,
		BatchInterval:  10 * time.Millisecond,
	}
}

// WorkRequest represents a unit of work to be processed.
type WorkRequest struct {
	ID      uint64
	Context context.Context
	Type    WorkType
	Data    interface{}
	Result  chan WorkResult
}

// WorkType defines the type of operation.
type WorkType uint8

const (
	WorkTypeRead WorkType = iota
	WorkTypeWrite
	WorkTypeDelete
	WorkTypeScan
	WorkTypeBatch
)

// WorkResult contains the result of a work request.
type WorkResult struct {
	ID    uint64
	Data  interface{}
	Error error
}

// ConcurrencyManager orchestrates concurrent operations.
type ConcurrencyManager struct {
	config ConcurrencyConfig

	// Worker pools
	readPool  *WorkerPool
	writePool *WorkerPool

	// Request queues (buffered channels)
	readQueue  chan WorkRequest
	writeQueue chan WorkRequest

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	stats ConcurrencyStats

	// Batch processor
	batcher *BatchProcessor
}

// ConcurrencyStats tracks concurrency metrics.
type ConcurrencyStats struct {
	TotalRequests   atomic.Uint64
	CompletedReads  atomic.Uint64
	CompletedWrites atomic.Uint64
	FailedRequests  atomic.Uint64
	QueuedReads     atomic.Int64
	QueuedWrites    atomic.Int64
	ActiveWorkers   atomic.Int64
}

// WorkerPool manages a pool of worker goroutines.
type WorkerPool struct {
	name       string
	size       int
	workQueue  chan WorkRequest
	handler    WorkHandler
	ctx        context.Context
	wg         *sync.WaitGroup
	semaphore  chan struct{}
	processing atomic.Int64
}

// WorkHandler processes work requests.
type WorkHandler func(ctx context.Context, req WorkRequest) WorkResult

// NewConcurrencyManager creates a new concurrency manager.
func NewConcurrencyManager(config ConcurrencyConfig) *ConcurrencyManager {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ConcurrencyManager{
		config:     config,
		readQueue:  make(chan WorkRequest, config.ReadQueueSize),
		writeQueue: make(chan WorkRequest, config.WriteQueueSize),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Create worker pools
	cm.readPool = NewWorkerPool("reader", config.ReadWorkers, cm.readQueue, cm.handleRead, ctx, &cm.wg)
	cm.writePool = NewWorkerPool("writer", config.WriteWorkers, cm.writeQueue, cm.handleWrite, ctx, &cm.wg)

	// Create batch processor
	cm.batcher = NewBatchProcessor(config.BatchSize, config.BatchInterval, cm.processBatch)

	// Start worker pools
	cm.readPool.Start()
	cm.writePool.Start()

	// Start batch processor
	cm.wg.Add(1)
	go cm.batcher.Run(ctx, &cm.wg)

	return cm
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(name string, size int, workQueue chan WorkRequest, handler WorkHandler, ctx context.Context, wg *sync.WaitGroup) *WorkerPool {
	return &WorkerPool{
		name:      name,
		size:      size,
		workQueue: workQueue,
		handler:   handler,
		ctx:       ctx,
		wg:        wg,
		semaphore: make(chan struct{}, size),
	}
}

// Start launches all worker goroutines.
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.size; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker is the main worker loop.
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return

		case req := <-wp.workQueue:
			// Acquire semaphore
			wp.semaphore <- struct{}{}
			wp.processing.Add(1)

			// Process with timeout
			result := wp.processWithTimeout(req)

			// Send result
			select {
			case req.Result <- result:
			case <-req.Context.Done():
			case <-wp.ctx.Done():
			}

			// Release semaphore
			<-wp.semaphore
			wp.processing.Add(-1)
		}
	}
}

// processWithTimeout processes a request with timeout.
func (wp *WorkerPool) processWithTimeout(req WorkRequest) WorkResult {
	// Create timeout context
	ctx, cancel := context.WithTimeout(req.Context, 5*time.Second)
	defer cancel()

	// Process in goroutine
	resultChan := make(chan WorkResult, 1)

	go func() {
		resultChan <- wp.handler(ctx, req)
	}()

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		return WorkResult{
			ID:    req.ID,
			Error: fmt.Errorf("worker timeout: %w", ctx.Err()),
		}
	}
}

// SubmitRead submits a read request (non-blocking).
func (cm *ConcurrencyManager) SubmitRead(ctx context.Context, data interface{}) <-chan WorkResult {
	return cm.submitRequest(ctx, WorkTypeRead, data, cm.readQueue, &cm.stats.QueuedReads)
}

// SubmitWrite submits a write request (non-blocking).
func (cm *ConcurrencyManager) SubmitWrite(ctx context.Context, data interface{}) <-chan WorkResult {
	return cm.submitRequest(ctx, WorkTypeWrite, data, cm.writeQueue, &cm.stats.QueuedWrites)
}

// submitRequest submits a work request to a queue.
func (cm *ConcurrencyManager) submitRequest(ctx context.Context, workType WorkType, data interface{}, queue chan WorkRequest, queueCounter *atomic.Int64) <-chan WorkResult {
	reqID := cm.stats.TotalRequests.Add(1)
	resultChan := make(chan WorkResult, 1)

	req := WorkRequest{
		ID:      reqID,
		Context: ctx,
		Type:    workType,
		Data:    data,
		Result:  resultChan,
	}

	queueCounter.Add(1)

	// Try to submit with timeout
	go func() {
		defer queueCounter.Add(-1)

		select {
		case queue <- req:
			// Submitted successfully
		case <-ctx.Done():
			// Context cancelled
			resultChan <- WorkResult{ID: reqID, Error: ctx.Err()}
		case <-time.After(cm.config.QueueTimeout):
			// Queue full timeout
			resultChan <- WorkResult{ID: reqID, Error: errors.New("queue full timeout")}
			cm.stats.FailedRequests.Add(1)
		}
	}()

	return resultChan
}

// handleRead processes read requests.
func (cm *ConcurrencyManager) handleRead(ctx context.Context, req WorkRequest) WorkResult {
	defer cm.stats.CompletedReads.Add(1)

	// Simulate read operation
	// In real implementation, this would interact with buffer pool, WAL, etc.
	select {
	case <-ctx.Done():
		return WorkResult{ID: req.ID, Error: ctx.Err()}
	default:
		// Process read
		return WorkResult{ID: req.ID, Data: req.Data, Error: nil}
	}
}

// handleWrite processes write requests.
func (cm *ConcurrencyManager) handleWrite(ctx context.Context, req WorkRequest) WorkResult {
	defer cm.stats.CompletedWrites.Add(1)

	// Simulate write operation
	select {
	case <-ctx.Done():
		return WorkResult{ID: req.ID, Error: ctx.Err()}
	default:
		// Process write
		return WorkResult{ID: req.ID, Data: req.Data, Error: nil}
	}
}

// Stats returns current concurrency statistics.
func (cm *ConcurrencyManager) Stats() *ConcurrencyStats {
	return &cm.stats
}

// Shutdown gracefully shuts down the concurrency manager.
func (cm *ConcurrencyManager) Shutdown(timeout time.Duration) error {
	// Cancel context
	cm.cancel()

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("shutdown timeout")
	}
}

// BatchProcessor batches operations for efficiency.
type BatchProcessor struct {
	maxSize  int
	interval time.Duration
	handler  BatchHandler
	queue    chan interface{}
	mu       sync.Mutex
	batch    []interface{}
}

// BatchHandler processes a batch of items.
type BatchHandler func(items []interface{}) error

// NewBatchProcessor creates a new batch processor.
func NewBatchProcessor(maxSize int, interval time.Duration, handler BatchHandler) *BatchProcessor {
	return &BatchProcessor{
		maxSize:  maxSize,
		interval: interval,
		handler:  handler,
		queue:    make(chan interface{}, maxSize*2),
		batch:    make([]interface{}, 0, maxSize),
	}
}

// Add adds an item to the batch queue.
func (bp *BatchProcessor) Add(item interface{}) error {
	select {
	case bp.queue <- item:
		return nil
	default:
		return errors.New("batch queue full")
	}
}

// Run starts the batch processor.
func (bp *BatchProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(bp.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch
			bp.flush()
			return

		case item := <-bp.queue:
			bp.mu.Lock()
			bp.batch = append(bp.batch, item)

			if len(bp.batch) >= bp.maxSize {
				bp.flushLocked()
			}
			bp.mu.Unlock()

		case <-ticker.C:
			bp.flush()
		}
	}
}

// flush flushes the current batch.
func (bp *BatchProcessor) flush() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.flushLocked()
}

// flushLocked flushes the batch (must hold lock).
func (bp *BatchProcessor) flushLocked() {
	if len(bp.batch) == 0 {
		return
	}

	// Process batch
	if err := bp.handler(bp.batch); err != nil {
		log.Printf("batch handler error: %v", err)
	}

	// Clear batch
	bp.batch = bp.batch[:0]
}

// processBatch is the default batch handler.
func (cm *ConcurrencyManager) processBatch(items []interface{}) error {
	// Process batch of items
	// In real implementation, this would batch writes to WAL, etc.
	return nil
}

// ParallelIterator provides concurrent iteration over data.
type ParallelIterator struct {
	items   []interface{}
	workers int
	ctx     context.Context
}

// NewParallelIterator creates a parallel iterator.
func NewParallelIterator(items []interface{}, workers int) *ParallelIterator {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	return &ParallelIterator{
		items:   items,
		workers: workers,
		ctx:     context.Background(),
	}
}

// WithContext sets the context for the iterator.
func (pi *ParallelIterator) WithContext(ctx context.Context) *ParallelIterator {
	pi.ctx = ctx
	return pi
}

// ForEach processes items in parallel.
func (pi *ParallelIterator) ForEach(fn func(item interface{}) error) error {
	if len(pi.items) == 0 {
		return nil
	}

	// Create work channels
	workChan := make(chan interface{}, len(pi.items))
	errorChan := make(chan error, pi.workers)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < pi.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range workChan {
				select {
				case <-pi.ctx.Done():
					errorChan <- pi.ctx.Err()
					return
				default:
					if err := fn(item); err != nil {
						errorChan <- err
						return
					}
				}
			}
		}()
	}

	// Feed work
	go func() {
		for _, item := range pi.items {
			select {
			case <-pi.ctx.Done():
				close(workChan)
				return
			case workChan <- item:
			}
		}
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()
	close(errorChan)

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// Map applies a function to all items in parallel and returns results.
func (pi *ParallelIterator) Map(fn func(item interface{}) (interface{}, error)) ([]interface{}, error) {
	if len(pi.items) == 0 {
		return nil, nil
	}

	type result struct {
		index int
		value interface{}
		err   error
	}

	results := make([]interface{}, len(pi.items))
	resultChan := make(chan result, len(pi.items))
	workChan := make(chan struct {
		index int
		item  interface{}
	}, len(pi.items))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < pi.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for work := range workChan {
				select {
				case <-pi.ctx.Done():
					resultChan <- result{index: work.index, err: pi.ctx.Err()}
					return
				default:
					value, err := fn(work.item)
					resultChan <- result{index: work.index, value: value, err: err}
				}
			}
		}()
	}

	// Feed work
	go func() {
		for i, item := range pi.items {
			select {
			case <-pi.ctx.Done():
				close(workChan)
				return
			case workChan <- struct {
				index int
				item  interface{}
			}{index: i, item: item}:
			}
		}
		close(workChan)
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for res := range resultChan {
		if res.err != nil {
			return nil, res.err
		}
		results[res.index] = res.value
	}

	return results, nil
}

// Pipeline implements a concurrent pipeline pattern.
type Pipeline struct {
	stages []PipelineStage
	ctx    context.Context
}

// PipelineStage represents a stage in the pipeline.
type PipelineStage func(ctx context.Context, input <-chan interface{}) <-chan interface{}

// NewPipeline creates a new pipeline.
func NewPipeline(ctx context.Context, stages ...PipelineStage) *Pipeline {
	return &Pipeline{
		stages: stages,
		ctx:    ctx,
	}
}

// Execute runs the pipeline.
func (p *Pipeline) Execute(input []interface{}) <-chan interface{} {
	// Create input channel
	inputChan := make(chan interface{}, len(input))

	// Feed input
	go func() {
		defer close(inputChan)
		for _, item := range input {
			select {
			case <-p.ctx.Done():
				return
			case inputChan <- item:
			}
		}
	}()

	// Chain stages
	var current <-chan interface{} = inputChan
	for _, stage := range p.stages {
		current = stage(p.ctx, current)
	}

	return current
}

// FanOut distributes work from one channel to multiple channels.
func FanOut(ctx context.Context, input <-chan interface{}, workers int) []<-chan interface{} {
	outputs := make([]<-chan interface{}, workers)

	for i := 0; i < workers; i++ {
		ch := make(chan interface{})
		outputs[i] = ch

		go func(out chan interface{}) {
			defer close(out)

			for item := range input {
				select {
				case <-ctx.Done():
					return
				case out <- item:
				}
			}
		}(ch)
	}

	return outputs
}

// FanIn combines multiple channels into one.
func FanIn(ctx context.Context, channels ...<-chan interface{}) <-chan interface{} {
	output := make(chan interface{})

	var wg sync.WaitGroup
	multiplex := func(ch <-chan interface{}) {
		defer wg.Done()

		for item := range ch {
			select {
			case <-ctx.Done():
				return
			case output <- item:
			}
		}
	}

	wg.Add(len(channels))
	for _, ch := range channels {
		go multiplex(ch)
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

// RateLimiter limits the rate of operations.
type RateLimiter struct {
	ticker   *time.Ticker
	tokens   chan struct{}
	capacity int
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(opsPerSecond int) *RateLimiter {
	rl := &RateLimiter{
		ticker:   time.NewTicker(time.Second / time.Duration(opsPerSecond)),
		tokens:   make(chan struct{}, opsPerSecond),
		capacity: opsPerSecond,
	}

	// Fill initial tokens
	for i := 0; i < opsPerSecond; i++ {
		rl.tokens <- struct{}{}
	}

	// Refill tokens
	go func() {
		for range rl.ticker.C {
			select {
			case rl.tokens <- struct{}{}:
			default:
			}
		}
	}()

	return rl
}

// Wait blocks until a token is available.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.tokens:
		return nil
	}
}

// Stop stops the rate limiter.
func (rl *RateLimiter) Stop() {
	rl.ticker.Stop()
}
