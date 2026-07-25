// This file is excluded from the wasm build (it shells out to
// "go build" which doesn't make sense inside a WASM runtime).
//go:build !wasm

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildQueryFilesWasm(t *testing.T) {
	// Generous on purpose. This shells out to the real toolchain, the link step
	// is not cached, and "go test ./..." runs several of these concurrently, so a
	// tight budget fails on a busy machine while the build itself is fine. The
	// bound is here to catch a hung toolchain, not to police build speed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_query_files_wasm_bin")
	defer os.Remove(out)

	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", out, ".")
	cmd.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm")
	if outp, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build (GOOS=js GOARCH=wasm) failed: %v\n%s", err, string(outp))
	}

	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat wasm binary: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("wasm binary is empty")
	}
	t.Logf("WASM binary size: %d bytes", info.Size())
}
