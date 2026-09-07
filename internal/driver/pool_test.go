package driver

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPoolRejectsCanceledContextWithAvailableSlot(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second} {
		s := &server{busyTimeout: timeout}
		pool := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 100 {
			if err := s.acquire(ctx, pool); !errors.Is(err, context.Canceled) {
				t.Fatalf("timeout=%s: got %v", timeout, err)
			}
			if len(pool) != 0 {
				t.Fatal("canceled request consumed a slot")
			}
		}
	}
}

func TestPoolContextDeadlinePrecedesBusyTimeout(t *testing.T) {
	s := &server{busyTimeout: time.Second}
	pool := make(chan struct{}, 1)
	pool <- struct{}{}
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		err := s.acquire(ctx, pool)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context deadline, got %v", err)
		}
	}
	if len(pool) != 1 {
		t.Fatal("waiting request changed pool occupancy")
	}
}

func TestPoolWaiterUsesReleasedSlot(t *testing.T) {
	s := &server{busyTimeout: time.Second}
	pool := make(chan struct{}, 1)
	pool <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.acquire(ctx, pool) }()
	s.release(pool)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s.release(pool)
	if len(pool) != 0 {
		t.Fatal("slot leaked")
	}
}

func BenchmarkPoolUncontended(b *testing.B) {
	s := &server{busyTimeout: time.Second}
	pool := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := s.acquire(ctx, pool); err != nil {
			b.Fatal(err)
		}
		s.release(pool)
	}
}
