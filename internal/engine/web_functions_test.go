//go:build !tinygo.wasm && !baremetal

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func executeWebScalar(t testing.TB, sql string) any {
	t.Helper()
	statement, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse SQL: %v", err)
	}
	result, err := Execute(context.Background(), storage.NewDB(), "default", statement)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	return result.Rows[0]["value"]
}

func TestURLWebFunctions(t *testing.T) {
	parsed := executeWebScalar(t, `SELECT URL_PARSE('https://example.com/a%20b?q=hello+world#part') AS value`)
	var parts map[string]string
	if err := json.Unmarshal([]byte(parsed.(string)), &parts); err != nil {
		t.Fatalf("URL_PARSE JSON: %v", err)
	}
	if parts["scheme"] != "https" || parts["host"] != "example.com" || parts["path"] != "/a b" || parts["fragment"] != "part" {
		t.Fatalf("URL_PARSE = %#v", parts)
	}
	if got := executeWebScalar(t, `SELECT URL_QUERY_GET('https://example.com/?q=hello+world', 'q') AS value`); got != "hello world" {
		t.Fatalf("URL_QUERY_GET = %v", got)
	}
	if got := executeWebScalar(t, `SELECT URL_RESOLVE('https://example.com/a/b/', '../c?q=1') AS value`); got != "https://example.com/a/c?q=1" {
		t.Fatalf("URL_RESOLVE = %v", got)
	}
	if got := executeWebScalar(t, `SELECT URL_PATH_ENCODE('a/b c') AS value`); got != "a%2Fb%20c" {
		t.Fatalf("URL_PATH_ENCODE = %v", got)
	}
	if got := executeWebScalar(t, `SELECT URL_PATH_DECODE('a%2Fb%20c') AS value`); got != "a/b c" {
		t.Fatalf("URL_PATH_DECODE = %v", got)
	}
}

func TestHTMLTemplateEscapesContextuallyAndCaches(t *testing.T) {
	const source = `<a href="{{.url}}">{{.name}}</a>`
	query := `SELECT HTML_TEMPLATE('<a href="{{.url}}">{{.name}}</a>', '{"url":"javascript:alert(1)","name":"<Admin>"}') AS value`
	got := executeWebScalar(t, query).(string)
	if got != `<a href="#ZgotmplZ">&lt;Admin&gt;</a>` {
		t.Fatalf("HTML_TEMPLATE = %q", got)
	}
	if escaped := executeWebScalar(t, `SELECT HTML_ESCAPE('<b>"x" & y</b>') AS value`); escaped != `&lt;b&gt;&#34;x&#34; &amp; y&lt;/b&gt;` {
		t.Fatalf("HTML_ESCAPE = %q", escaped)
	}
	first, err := htmlTemplates.get(source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := htmlTemplates.get(source)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("template cache returned different compiled templates")
	}
}

func TestHTMLTemplateCacheConcurrent(t *testing.T) {
	const source = `<p>{{.value}}</p>`
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parsed, err := htmlTemplates.get(source)
			if err != nil {
				errs <- err
				return
			}
			var out strings.Builder
			errs <- parsed.Execute(&out, map[string]string{"value": "ok"})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
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
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fetchHTTP(ctx, client, "https://example.com/data", 64); err == nil {
		t.Fatal("fetchHTTP ignored canceled context")
	}
}

func BenchmarkWebScalarFunctions(b *testing.B) {
	db := storage.NewDB()
	benchmarks := map[string]string{
		"url_parse":     `SELECT URL_PARSE('https://example.com/a/b?q=value#part') AS value`,
		"html_template": `SELECT HTML_TEMPLATE('<p>{{.name}}</p>', '{"name":"Ada"}') AS value`,
	}
	for name, query := range benchmarks {
		statement, err := NewParser(query).ParseStatement()
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := Execute(context.Background(), db, "default", statement)
				if err != nil {
					b.Fatal(fmt.Errorf("execute: %w", err))
				}
				if len(result.Rows) != 1 {
					b.Fatalf("execute: rows=%d", len(result.Rows))
				}
			}
		})
	}
}
