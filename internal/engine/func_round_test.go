package engine

import (
	"testing"
)

func TestEvalRound(t *testing.T) {
	tests := []struct {
		name     string
		args     []Expr
		row      Row
		expected any
		wantErr  bool
	}{
		{
			name: "Basic rounding",
			args: []Expr{
				&Literal{Val: 3.14159},
				&Literal{Val: 2},
			},
			expected: 3.14,
		},
		{
			name: "Round integer",
			args: []Expr{
				&Literal{Val: 123},
				&Literal{Val: 1},
			},
			expected: 123.0,
		},
		{
			name: "Round with string decimals (coerced)",
			args: []Expr{
				&Literal{Val: 3.14159},
				&Literal{Val: "2"},
			},
			expected: 3.14,
		},
		{
			name: "Round NULL value",
			args: []Expr{
				&Literal{Val: nil},
				&Literal{Val: 2},
			},
			expected: nil,
		},
		{
			name: "Round NULL decimals",
			args: []Expr{
				&Literal{Val: 3.14159},
				&Literal{Val: nil},
			},
			expected: nil,
		},
		{
			name: "Round invalid decimals type",
			args: []Expr{
				&Literal{Val: 3.14159},
				&Literal{Val: "invalid"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalRound(ExecEnv{}, tt.args, tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("evalRound() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("evalRound() = %v, want %v", got, tt.expected)
			}
		})
	}
}
