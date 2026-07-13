package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunJSONIncludesInactivePlaceWhenRequested(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out, "json", true); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), `"Cologne"`) {
		t.Fatalf("JSON output does not include inactive place: %s", out.String())
	}
}

func TestRunRejectsUnknownFormat(t *testing.T) {
	if err := run(context.Background(), &bytes.Buffer{}, "xml", false); err == nil {
		t.Fatal("expected unsupported-format error")
	}
}
