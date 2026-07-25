package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildTinySqlPage(t *testing.T) {
	// Generous on purpose. This shells out to the real toolchain, the link step
	// is not cached, and "go test ./..." runs several of these concurrently, so a
	// tight budget fails on a busy machine while the build itself is fine. The
	// bound is here to catch a hung toolchain, not to police build speed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_tinysqlpage_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}
