//go:build goexperiment.jsonv2

package exporter

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

type depthAwareJSON struct{}

func (depthAwareJSON) MarshalJSONTo(e *jsontext.Encoder) error {
	return e.WriteToken(jsontext.Int(int64(e.StackDepth())))
}

func TestJSONStreamPreservesCustomMarshalerContext(t *testing.T) {
	for _, value := range []any{depthAwareJSON{}, []any{depthAwareJSON{}}, map[string]any{"nested": depthAwareJSON{}}} {
		rs := &engine.ResultSet{Cols: []string{"x"}, Rows: []engine.Row{{"x": 1}, {"x": value}, {"x": 2}}}
		checkJSONStreamCompatibility(t, rs)
	}
}

func TestJSONStreamPreservesRawJSONSemantics(t *testing.T) {
	rows := []engine.Row{
		{"v": json.RawMessage(` { "a" : 1, "a" : 2, "html":"<x>&" } `)},
		{"v": json.RawMessage([]byte{'"', 0xff, '"'})},
		{"v": map[string]any{"nilSlice": []any(nil), "nilMap": map[string]any(nil)}},
	}
	rs := &engine.ResultSet{Cols: []string{"v"}, Rows: rows}
	checkJSONStreamCompatibility(t, rs)
}

func TestJSONStreamPreservesPrimitiveValues(t *testing.T) {
	rows := []engine.Row{}
	for _, value := range []any{
		nil, true, false, "é漢🙂\x00<&\"\\\n\u2028\u2029", "\xff",
		int64(math.MaxInt64), uint64(math.MaxUint64), uintptr(123),
		math.SmallestNonzeroFloat64, math.MaxFloat64, math.Copysign(0, -1), float32(1.2),
		[]byte(nil), []byte{}, []byte{0, 255}, map[string]string(nil), map[string]string{"b": "<x>", "a": "é"},
	} {
		rows = append(rows, engine.Row{"v": value})
	}
	checkJSONStreamCompatibility(t, &engine.ResultSet{Cols: []string{"v"}, Rows: rows})
}

func checkJSONStreamCompatibility(t *testing.T, rs *engine.ResultSet) {
	t.Helper()
	for _, pretty := range []bool{false, true} {
		for _, binaryMode := range []string{"", "legacy-string"} {
			opts := Options{PrettyJSON: pretty, JSONBinaryMode: binaryMode}
			var got, want bytes.Buffer
			if err := exportJSONLegacy(&want, rs, opts); err != nil {
				t.Fatal(err)
			}
			if err := ExportJSON(&got, rs, opts); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want.Bytes()) {
				t.Fatalf("pretty=%v binary=%q: got %s, want %s", pretty, binaryMode, got.Bytes(), want.Bytes())
			}
		}
	}
}
