package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_server_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

func TestEqualStringSlices(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, false}, // Order matters now
		{[]string{"a"}, []string{"a", "b"}, false},
		{[]string{"a", "b"}, []string{"a"}, false},
		{[]string{}, []string{}, true},
	}
	for _, tt := range tests {
		if got := equalStringSlices(tt.a, tt.b); got != tt.want {
			t.Errorf("equalStringSlices(%v, %v) = %v; want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
