package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"testing"
)

func TestGunzipText(t *testing.T) {
	var input bytes.Buffer
	for _, part := range []string{"abc", "def"} {
		w := gzip.NewWriter(&input)
		if _, err := w.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	read := func(ctx context.Context, limit int) (string, error) {
		return gunzipText(ctx, bytes.NewReader(input.Bytes()), limit)
	}
	if got, err := read(t.Context(), 6); err != nil || got != "abcdef" {
		t.Fatalf("exact limit: got %q, %v", got, err)
	}
	if got, err := read(t.Context(), 5); err == nil || got != "" {
		t.Fatalf("over limit: got %q, %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := read(ctx, 6); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: got %v", err)
	}
	input.Truncate(input.Len() - 1)
	if _, err := read(t.Context(), 6); err == nil {
		t.Fatal("expected truncated gzip trailer to fail")
	}
}
