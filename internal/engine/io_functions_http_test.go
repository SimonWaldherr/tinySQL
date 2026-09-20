//go:build !tinygo.wasm && !baremetal && !no_http

package engine

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// These tests exercise fetchHTTP/evalHTTPFunc directly, so they carry the
// same build constraint as io_functions_http.go itself (split out of
// web_functions_test.go, which covers the unrelated URL/HTML-template
// functions that stay available in a -tags no_http build).
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestFetchHTTPBoundsResponseAndReusesClient(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.String() != "https://example.com/data" {
			t.Fatalf("request URL = %q", req.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", 65))),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}
	for i := 0; i < 2; i++ {
		body, err := fetchHTTP(context.Background(), client, "https://example.com/data", 128)
		if err != nil || len(body) != 65 {
			t.Fatalf("fetch %d body/error = %d/%v", i, len(body), err)
		}
	}
	if calls != 2 {
		t.Fatalf("transport calls = %d, want 2", calls)
	}
	if _, err := fetchHTTP(context.Background(), client, "https://example.com/data", 64); err == nil || !strings.Contains(err.Error(), "exceeds 64 bytes") {
		t.Fatalf("oversized response error = %v", err)
	}
	if _, err := fetchHTTP(context.Background(), client, "file:///tmp/data", 64); err == nil {
		t.Fatal("fetchHTTP accepted a non-HTTP URL")
	}
}

func TestFetchHTTPHonorsContext(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fetchHTTP(ctx, client, "https://example.com/data", 64); err == nil {
		t.Fatal("fetchHTTP ignored canceled context")
	}
}
