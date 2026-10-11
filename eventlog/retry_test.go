package eventlog

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonWaldherr/tinySQL/driver"
)

func TestRetryConflictBackoff(t *testing.T) {
	for _, succeed := range []bool{false, true} {
		attempts := 0
		value, err := retryConflict(t.Context(), func() (int, error) {
			attempts++
			if succeed && attempts == 4 {
				return attempts, nil
			}
			return attempts, driver.ErrTransactionConflict
		})
		if attempts != 4 || value != 4 {
			t.Fatalf("retry result: attempts=%d, value=%d", attempts, value)
		}
		if succeed && err != nil || !succeed && !errors.Is(err, driver.ErrTransactionConflict) {
			t.Fatalf("retry error: %v", err)
		}
	}
}

func TestRetryConflictStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempts := 0
	_, err := retryConflict(ctx, func() (int, error) {
		attempts++
		cancel()
		return attempts, driver.ErrTransactionConflict
	})
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("canceled retry: attempts=%d, err=%v", attempts, err)
	}
}
