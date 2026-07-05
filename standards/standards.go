// Package standards centralizes interoperability constants and helpers used by
// tinySQL and applications built on top of it.
package standards

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	// RFC 8259 JSON media type, sent with an explicit UTF-8 charset for HTTP.
	MediaTypeJSON = "application/json; charset=utf-8"

	// RFC 9457 Problem Details for HTTP APIs.
	MediaTypeProblemJSON = "application/problem+json"

	// RFC 4180 CSV and common tab-separated values media types.
	MediaTypeCSV = "text/csv; charset=utf-8"
	MediaTypeTSV = "text/tab-separated-values; charset=utf-8"

	// XML 1.0 and ECMA-376 / ISO/IEC 29500 spreadsheet exports.
	MediaTypeXML  = "application/xml; charset=utf-8"
	MediaTypeXLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

	// SQLSTATE class/code values from ISO/IEC 9075 and common vendor practice.
	SQLStateSuccessfulCompletion = "00000"
	SQLStateWarning              = "01000"
	SQLStateNoData               = "02000"
	SQLStateDataException        = "22000"
	SQLStateInvalidDatetime      = "22007"
	SQLStateInvalidParameter     = "22023"
	SQLStateSyntaxError          = "42601"
	SQLStateUndefinedTable       = "42P01"
	SQLStateIntegrityViolation   = "23000"
	SQLStateInvalidAuthorization = "28000"
	SQLStateTransactionRollback  = "40000"
	SQLStateInternalError        = "XX000"
)

// FormatTime formats timestamps for public APIs using RFC 3339 in UTC.
func FormatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// ParseTime parses API timestamps using RFC 3339.
func ParseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339, strings.TrimSpace(value))
}

// Problem is an RFC 9457 Problem Details response. Error is a compatibility
// extension for older tinySQL/dbweb clients that read JSON responses via
// data.error.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
	Error    string `json:"error,omitempty"`
}

// NewProblem creates a Problem with a stable default type URI.
func NewProblem(status int, title, detail, instance string) Problem {
	if title == "" {
		title = http.StatusText(status)
	}
	return Problem{
		Type:     "about:blank",
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
		Error:    detail,
	}
}

// WriteProblem writes an RFC 9457 problem+json response.
func WriteProblem(w http.ResponseWriter, problem Problem) {
	w.Header().Set("Content-Type", MediaTypeProblemJSON)
	w.WriteHeader(problem.Status)
	_ = json.NewEncoder(w).Encode(problem)
}

// SQLStateError attaches a SQLSTATE code to an underlying error.
type SQLStateError struct {
	State string
	Err   error
}

func (e *SQLStateError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	if e.State == "" {
		return e.Err.Error()
	}
	return e.State + ": " + e.Err.Error()
}

func (e *SQLStateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// WithSQLState wraps err with a SQLSTATE code. Nil errors stay nil.
func WithSQLState(state string, err error) error {
	if err == nil {
		return nil
	}
	if state == "" {
		state = SQLStateInternalError
	}
	return &SQLStateError{State: state, Err: err}
}

// SQLState returns the SQLSTATE attached to err, if present.
func SQLState(err error) string {
	var stateErr *SQLStateError
	if errors.As(err, &stateErr) {
		return stateErr.State
	}
	return ""
}
