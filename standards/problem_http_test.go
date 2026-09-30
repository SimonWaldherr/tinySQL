//go:build !tinygo.wasm && !baremetal && !tinysql_minimal

package standards

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProblemWriter(t *testing.T) {
	problem := NewProblem(http.StatusUnprocessableEntity, "", "invalid row", "/rows/1")
	recorder := httptest.NewRecorder()
	WriteProblem(recorder, problem)
	if recorder.Code != http.StatusUnprocessableEntity || recorder.Header().Get("Content-Type") != MediaTypeProblemJSON || !strings.Contains(recorder.Body.String(), `"detail":"invalid row"`) {
		t.Fatalf("WriteProblem response = status:%d headers:%v body:%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
}
