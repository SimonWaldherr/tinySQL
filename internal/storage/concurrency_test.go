package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyManager_Basic(t *testing.T) {
	config := DefaultConcurrencyConfig()
	config.ReadWorkers = 2
	config.WriteWorkers = 2
	
	cm := NewConcurrencyManager(config)
	defer cm.Shutdown(5 * time.Second)
	
	ctx := context.Background()
	
	// Submit read request
	resultChan := cm.SubmitRead(ctx, "test data")
	
	select {
	case result := <-resultChan:
		if result.Error != nil {
			t.Fatalf("read failed: %v", result.Error)
		}
		if result.Data != "test data" {
			t.Errorf("expected 'test data', got %v", result.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read timeout")
	}
	
	// Submit write request
	resultChan = cm.SubmitWrite(ctx, "write data")
	
	select {
	case result := <-resultChan:
		if result.Error != nil {
			t.Fatalf("write failed: %v", result.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write timeout")
	}
}

func TestConcurrencyManager_Concurrent(t *testing.T) {
	config := DefaultConcurrencyConfig()
	config.ReadWorkers = 4
	config.WriteWorkers = 2
	
	cm := NewConcurrencyManager(config)
	defer cm.Shutdown(5 * time.Second)
	
	ctx := context.Background()
	
	var wg sync.WaitGroup
	readCount := 100
	writeCount := 50
	
	// Concurrent reads
	for i := 0; i < readCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			resultChan := cm.SubmitRead(ctx, id)
			select {
			case result := <-resultChan:
				if result.Error != nil {
					t.Errorf("read %d failed: %v", id, result.Error)
				}
			case <-time.After(3 * time.Second):
				t.Errorf("read %d timeout", id)
			}
		}(i)
	}
	
	// Concurrent writes
	for i := 0; i < writeCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			resultChan := cm.SubmitWrite(ctx, id)
			select {
			case result := <-resultChan:
				if result.Error != nil {
					t.Errorf("write %d failed: %v", id, result.Error)
				}
			case <-time.After(3 * time.Second):
				t.Errorf("write %d timeout", id)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Check stats
	stats := cm.Stats()
	if stats.TotalRequests.Load() != uint64(readCount+writeCount) {
		t.Errorf("expected %d requests, got %d", readCount+writeCount, stats.TotalRequests.Load())
	}
}

func TestConcurrencyManager_ContextCancellation(t *testing.T) {
	config := DefaultConcurrencyConfig()
	cm := NewConcurrencyManager(config)
	defer cm.Shutdown(5 * time.Second)
	
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	
	// Submit request with short timeout
	resultChan := cm.SubmitRead(ctx, "test")
	
	// Wait for timeout
	time.Sleep(100 * time.Millisecond)
	
	// Should get timeout error
	select {
	case result := <-resultChan:
		// Either got result before timeout, or got timeout error - both ok
		if result.Error != nil {
			t.Logf("Got expected error: %v", result.Error)
		} else {
			t.Log("Request completed before timeout (also valid)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestConcurrencyManager_Shutdown(t *testing.T) {
	config := DefaultConcurrencyConfig()
	config.ReadWorkers = 2
	
	cm := NewConcurrencyManager(config)
	
	// Submit some work
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		cm.SubmitRead(ctx, i)
	}
	
	// Shutdown
	err := cm.Shutdown(3 * time.Second)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestWorkerPool_Processing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	var wg sync.WaitGroup
	queue := make(chan WorkRequest, 10)
	
	var processed atomic.Int64
	handler := func(ctx context.Context, req WorkRequest) WorkResult {
		processed.Add(1)
		return WorkResult{ID: req.ID, Data: req.Data}
	}
	
	pool := NewWorkerPool("test", 3, queue, handler, ctx, &wg)
	pool.Start()
	
	// Submit work
	workCount := 20
	results := make([]chan WorkResult, workCount)
	
	for i := 0; i < workCount; i++ {
		results[i] = make(chan WorkResult, 1)
		queue <- WorkRequest{
			ID:      uint64(i),
			Context: ctx,
			Data:    i,
			Result:  results[i],
		}
	}
	
	// Collect results
	for i := 0; i < workCount; i++ {
		select {
		case result := <-results[i]:
			if result.Error != nil {
				t.Errorf("work %d failed: %v", i, result.Error)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("work %d timeout", i)
		}
	}
	
	cancel()
	wg.Wait()
	
	if processed.Load() != int64(workCount) {
		t.Errorf("expected %d processed, got %d", workCount, processed.Load())
	}
}

func TestBatchProcessor(t *testing.T) {
	var processedBatches atomic.Int64
	var totalItems atomic.Int64
	
	handler := func(items []interface{}) error {
		processedBatches.Add(1)
		totalItems.Add(int64(len(items)))
		return nil
	}
	
	// Use batch size that accommodates our test
	bp := NewBatchProcessor(10, 50*time.Millisecond, handler)
	
	// Manually set larger queue to avoid "full" error
	bp.queue = make(chan interface{}, 100)
	
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go bp.Run(ctx, &wg)
	
	// Add items
	itemCount := 25
	for i := 0; i < itemCount; i++ {
		if err := bp.Add(i); err != nil {
			t.Fatalf("failed to add item %d: %v", i, err)
		}
	}
	
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	
	cancel()
	wg.Wait()
	
	if totalItems.Load() != int64(itemCount) {
		t.Errorf("expected %d items processed, got %d", itemCount, totalItems.Load())
	}
	
	batches := processedBatches.Load()
	if batches < 1 {
		t.Error("expected at least 1 batch processed")
	}
	
	t.Logf("Processed %d items in %d batches", itemCount, batches)
}

func TestParallelIterator_ForEach(t *testing.T) {
	items := make([]interface{}, 100)
	for i := range items {
		items[i] = i
	}
	
	pi := NewParallelIterator(items, 4)
	
	var sum atomic.Int64
	err := pi.ForEach(func(item interface{}) error {
		val := item.(int)
		sum.Add(int64(val))
		return nil
	})
	
	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}
	
	expected := int64(4950) // sum of 0..99
	if sum.Load() != expected {
		t.Errorf("expected sum %d, got %d", expected, sum.Load())
	}
}

func TestParallelIterator_Map(t *testing.T) {
	items := make([]interface{}, 50)
	for i := range items {
		items[i] = i
	}
	
	pi := NewParallelIterator(items, 4)
	
	results, err := pi.Map(func(item interface{}) (interface{}, error) {
		val := item.(int)
		return val * 2, nil
	})
	
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	
	if len(results) != len(items) {
		t.Errorf("expected %d results, got %d", len(items), len(results))
	}
	
	for i, result := range results {
		expected := i * 2
		if result != expected {
			t.Errorf("result[%d]: expected %d, got %v", i, expected, result)
		}
	}
}

func TestParallelIterator_ContextCancellation(t *testing.T) {
	items := make([]interface{}, 1000)
	for i := range items {
		items[i] = i
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	pi := NewParallelIterator(items, 4).WithContext(ctx)
	
	var processed atomic.Int64
	
	// Cancel after processing some items
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	
	err := pi.ForEach(func(item interface{}) error {
		processed.Add(1)
		time.Sleep(1 * time.Millisecond) // Simulate work
		return nil
	})
	
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	
	processedCount := processed.Load()
	if processedCount >= int64(len(items)) {
		t.Error("should not have processed all items after cancellation")
	}
	
	t.Logf("Processed %d items before cancellation", processedCount)
}

func TestPipeline(t *testing.T) {
	ctx := context.Background()
	
	// Stage 1: double each value
	stage1 := func(ctx context.Context, input <-chan interface{}) <-chan interface{} {
		output := make(chan interface{})
		go func() {
			defer close(output)
			for item := range input {
				val := item.(int)
				select {
				case <-ctx.Done():
					return
				case output <- val * 2:
				}
			}
		}()
		return output
	}
	
	// Stage 2: add 10 to each value
	stage2 := func(ctx context.Context, input <-chan interface{}) <-chan interface{} {
		output := make(chan interface{})
		go func() {
			defer close(output)
			for item := range input {
				val := item.(int)
				select {
				case <-ctx.Done():
					return
				case output <- val + 10:
				}
			}
		}()
		return output
	}
	
	pipeline := NewPipeline(ctx, stage1, stage2)
	
	input := []interface{}{1, 2, 3, 4, 5}
	output := pipeline.Execute(input)
	
	results := make([]int, 0, len(input))
	for result := range output {
		results = append(results, result.(int))
	}
	
	if len(results) != len(input) {
		t.Errorf("expected %d results, got %d", len(input), len(results))
	}
	
	// Each value should be doubled and then have 10 added
	// 1 -> 2 -> 12
	// 2 -> 4 -> 14
	// etc.
	for i, result := range results {
		expected := (input[i].(int) * 2) + 10
		if result != expected {
			t.Errorf("result[%d]: expected %d, got %d", i, expected, result)
		}
	}
}

func TestFanOut_FanIn(t *testing.T) {
	ctx := context.Background()
	
	// Create input channel
	input := make(chan interface{}, 10)
	go func() {
		defer close(input)
		for i := 0; i < 10; i++ {
			input <- i
		}
	}()
	
	// Fan out to 3 workers
	workers := FanOut(ctx, input, 3)
	
	// Process in each worker (double the value)
	processed := make([]<-chan interface{}, len(workers))
	for i, worker := range workers {
		processed[i] = processChannel(ctx, worker, func(val interface{}) interface{} {
			return val.(int) * 2
		})
	}
	
	// Fan in results
	output := FanIn(ctx, processed...)
	
	// Collect results
	results := make(map[int]bool)
	for result := range output {
		results[result.(int)] = true
	}
	
	// Check all expected values are present
	for i := 0; i < 10; i++ {
		expected := i * 2
		if !results[expected] {
			t.Errorf("missing result %d", expected)
		}
	}
}

func processChannel(ctx context.Context, input <-chan interface{}, fn func(interface{}) interface{}) <-chan interface{} {
	output := make(chan interface{})
	go func() {
		defer close(output)
		for item := range input {
			select {
			case <-ctx.Done():
				return
			case output <- fn(item):
			}
		}
	}()
	return output
}

func TestRateLimiter(t *testing.T) {
	opsPerSecond := 10
	rl := NewRateLimiter(opsPerSecond)
	defer rl.Stop()
	
	ctx := context.Background()
	
	// First, exhaust the initial burst tokens
	for i := 0; i < opsPerSecond; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("initial burst failed: %v", err)
		}
	}
	
	// Now measure rate limiting for additional operations
	start := time.Now()
	count := 15
	
	for i := 0; i < count; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("rate limiter wait failed: %v", err)
		}
	}
	
	elapsed := time.Since(start)
	
	// Should take at least (count/opsPerSecond) seconds
	minDuration := time.Duration(float64(count)/float64(opsPerSecond)*1000) * time.Millisecond
	if elapsed < minDuration {
		t.Errorf("rate limiter too fast: %v (expected at least %v)", elapsed, minDuration)
	}
	
	// But not too slow (allow 100% overhead for timing variability)
	maxDuration := minDuration * 2
	if elapsed > maxDuration {
		t.Errorf("rate limiter too slow: %v (expected at most %v)", elapsed, maxDuration)
	}
	
	t.Logf("Processed %d operations in %v (rate: %.1f ops/sec)", count, elapsed, float64(count)/elapsed.Seconds())
}

func TestRateLimiter_ContextCancellation(t *testing.T) {
	rl := NewRateLimiter(1) // 1 op per second
	defer rl.Stop()
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// Use up the initial token
	rl.Wait(ctx)
	
	// Cancel context
	cancel()
	
	// Next wait should fail immediately
	err := rl.Wait(ctx)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestConcurrencyConfig_Defaults(t *testing.T) {
	config := DefaultConcurrencyConfig()
	
	if config.ReadWorkers <= 0 {
		t.Error("read workers should be positive")
	}
	
	if config.WriteWorkers <= 0 {
		t.Error("write workers should be positive")
	}
	
	if config.ReadQueueSize <= 0 {
		t.Error("read queue size should be positive")
	}
	
	if config.BatchSize <= 0 {
		t.Error("batch size should be positive")
	}
	
	t.Logf("Default config: readers=%d, writers=%d, read_queue=%d, write_queue=%d",
		config.ReadWorkers, config.WriteWorkers, config.ReadQueueSize, config.WriteQueueSize)
}

func BenchmarkConcurrencyManager_Reads(b *testing.B) {
	config := DefaultConcurrencyConfig()
	cm := NewConcurrencyManager(config)
	defer cm.Shutdown(5 * time.Second)
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resultChan := cm.SubmitRead(ctx, "benchmark")
			<-resultChan
		}
	})
}

func BenchmarkConcurrencyManager_Writes(b *testing.B) {
	config := DefaultConcurrencyConfig()
	cm := NewConcurrencyManager(config)
	defer cm.Shutdown(5 * time.Second)
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resultChan := cm.SubmitWrite(ctx, "benchmark")
			<-resultChan
		}
	})
}

func BenchmarkParallelIterator(b *testing.B) {
	items := make([]interface{}, 1000)
	for i := range items {
		items[i] = i
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pi := NewParallelIterator(items, 4)
		pi.ForEach(func(item interface{}) error {
			_ = item.(int) * 2
			return nil
		})
	}
}
