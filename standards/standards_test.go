package standards

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProblemDefaultsAndMediaTypes(t *testing.T) {
	p := NewProblem(http.StatusBadRequest, "", "bad input", "/api/query")
	if p.Type != "about:blank" || p.Title != "Bad Request" || p.Status != http.StatusBadRequest {
		t.Fatalf("unexpected problem: %#v", p)
	}
	if p.Error != "bad input" {
		t.Fatalf("expected compatibility error field, got %q", p.Error)
	}
	if MediaTypeProblemJSON != "application/problem+json" {
		t.Fatalf("unexpected problem media type: %s", MediaTypeProblemJSON)
	}
}

func TestSQLStateWrapping(t *testing.T) {
	err := WithSQLState(SQLStateSyntaxError, errors.New("parse failed"))
	if got := SQLState(err); got != SQLStateSyntaxError {
		t.Fatalf("SQLState = %q, want %q", got, SQLStateSyntaxError)
	}
	if !errors.Is(err, errors.Unwrap(err)) {
		t.Fatalf("expected wrapped error")
	}
}

func TestFormatTimeUsesUTCAndRFC3339(t *testing.T) {
	loc := time.FixedZone("test", 2*60*60)
	ts := time.Date(2026, 7, 5, 12, 30, 0, 0, loc)
	if got := FormatTime(ts); got != "2026-07-05T10:30:00Z" {
		t.Fatalf("FormatTime = %q", got)
	}
}

func TestProblemWriterAndSQLStateEdgeCases(t *testing.T) {
	problem := NewProblem(http.StatusUnprocessableEntity, "", "invalid row", "/rows/1")
	recorder := httptest.NewRecorder()
	WriteProblem(recorder, problem)
	if recorder.Code != http.StatusUnprocessableEntity || recorder.Header().Get("Content-Type") != MediaTypeProblemJSON || !strings.Contains(recorder.Body.String(), `"detail":"invalid row"`) {
		t.Fatalf("WriteProblem response = status:%d headers:%v body:%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}

	if _, err := ParseTime("not-a-time"); err == nil {
		t.Fatal("ParseTime accepted invalid input")
	}
	if (&SQLStateError{}).Error() != "" || (&SQLStateError{}).Unwrap() != nil {
		t.Fatal("empty SQLStateError should be safe to inspect")
	}
	if WithSQLState("", nil) != nil || SQLState(errors.New("plain")) != "" {
		t.Fatal("SQLSTATE helpers handled nil or plain errors incorrectly")
	}
}
