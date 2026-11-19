//go:build !tinygo.wasm && !baremetal && !no_http && !tinysql_minimal

package engine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// These tests exercise fetchHTTP/evalHTTPFunc directly, so they carry the
// same build constraint as io_functions_http.go itself (split out of
// web_functions_test.go, which covers the unrelated URL/HTML-template
// functions that stay available in a -tags no_http build).
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDisableSQLHostIO(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE inputs (path TEXT)")
	execSQL(t, db, "INSERT INTO inputs VALUES (NULL)")
	execSQL(t, db, "CREATE VIEW contents AS SELECT FILE(path) AS content FROM inputs")
	execSQL(t, db, "CREATE TABLE copies (body TEXT)")
	execSQL(t, db, "CREATE TRIGGER fetch_after_insert AFTER INSERT ON inputs BEGIN INSERT INTO copies VALUES (HTTP(NEW.path)); END")

	// NULL arguments exercise dispatch without touching the filesystem or network.
	warm := mustParse("SELECT FILE(path) AS content FROM inputs")
	if _, err := Execute(t.Context(), db, "default", warm); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, "SELECT HTTP(NULL)")
	DisableSQLHostIO()
	t.Cleanup(func() { sqlHostIODisabled.Store(false) })
	for _, stmt := range []Statement{
		warm,
		mustParse("SELECT HTTP(NULL)"),
		mustParse("SELECT content FROM contents"),
		mustParse("INSERT INTO inputs VALUES (NULL)"),
	} {
		if _, err := Execute(t.Context(), db, "default", stmt); err == nil || !strings.Contains(err.Error(), errSQLHostIODisabled.Error()) {
			t.Errorf("%T: expected host I/O denial, got %v", stmt, err)
		}
	}
	if rs := execSQL(t, db, "SELECT path FROM inputs"); len(rs.Rows) != 1 {
		t.Fatalf("trigger failure left %d inputs, want 1", len(rs.Rows))
	}
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
		t.Fatal("canceled request reached the transport")
		return nil, context.Canceled
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fetchHTTP(ctx, client, "https://example.com/data", 64); !errors.Is(err, context.Canceled) {
		t.Fatalf("fetchHTTP lost cancellation error: %v", err)
	}
}

type httpReadFunc func([]byte) (int, error)

func (f httpReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestFetchHTTPPreservesCancellationDuringIO(t *testing.T) {
	for _, stage := range []string{"transport", "body"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if stage == "transport" {
					cancel()
					return nil, req.Context().Err()
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(httpReadFunc(func([]byte) (int, error) {
						cancel()
						return 0, req.Context().Err()
					})),
					Header:  make(http.Header),
					Request: req,
				}, nil
			})}
			if _, err := fetchHTTP(ctx, client, "https://example.com/data", 64); !errors.Is(err, context.Canceled) {
				t.Fatalf("fetchHTTP lost cancellation during %s: %v", stage, err)
			}
		})
	}
}

func TestFetchHTTPAcceptsNilContext(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}
	if body, err := fetchHTTP(nil, client, "https://example.com/data", 64); err != nil || body != "ok" {
		t.Fatalf("fetchHTTP(nil) = %q, %v", body, err)
	}
}
