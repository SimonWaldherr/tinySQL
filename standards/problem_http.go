//go:build !tinygo.wasm && !baremetal

package standards

import (
	"encoding/json"
	"net/http"
)

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
