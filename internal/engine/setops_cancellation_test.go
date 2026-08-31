package engine

import (
	"context"
	"errors"
	"testing"
)

// The set-operation materializers run after their SELECT terms have already
// produced rows. They therefore need their own checks rather than relying on
// the child SELECT executors to notice a cancelled request.
func TestSetOperationMaterializersHonorCanceledContext(t *testing.T) {
	values := make([]Row, 128)
	aliased := make([]Row, 128)
	for i := range values {
		values[i] = Row{"value": i}
		aliased[i] = Row{"other_value": i}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "union-all-append",
			run: func(ctx context.Context) error {
				_, err := appendRowsWithContext(ctx, values[:1], values[1:])
				return err
			},
		},
		{
			name: "align",
			run: func(ctx context.Context) error {
				_, err := alignSetOperationRowsWithContext(ctx, aliased, []string{"other_value"}, []string{"value"})
				return err
			},
		},
		{
			name: "distinct",
			run: func(ctx context.Context) error {
				_, err := distinctSetOperationRowsWithContext(ctx, values, []string{"value"})
				return err
			},
		},
		{
			name: "except",
			run: func(ctx context.Context) error {
				_, err := exceptRowsWithContext(ctx, values, values, []string{"value"})
				return err
			},
		},
		{
			name: "intersect",
			run: func(ctx context.Context) error {
				_, err := intersectRowsWithContext(ctx, values, values, []string{"value"})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		})
	}
}

func TestAppendSetOperationRowsPreservesOrder(t *testing.T) {
	left := []Row{{"value": "left"}}
	right := []Row{{"value": "middle"}, {"value": "right"}}

	got, err := appendRowsWithContext(context.Background(), left, right)
	if err != nil {
		t.Fatalf("appendRowsWithContext error = %v", err)
	}
	if len(got) != 3 || got[0]["value"] != "left" || got[1]["value"] != "middle" || got[2]["value"] != "right" {
		t.Fatalf("rows = %#v, want left/middle/right", got)
	}
}
