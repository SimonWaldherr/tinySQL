package exporter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestJSONExportsMatchIndependentRows(t *testing.T) {
	result := &engine.ResultSet{
		Cols: []string{"ID", "NÄME", "payload", "meta", "items", "ID"},
		Rows: []engine.Row{
			{"id": 1, "näme": "<Alice> & \"Bob\"", "payload": []byte{0, 255}, "meta": map[string]any{"nested": true}, "items": []any{1, nil, "x"}},
			{"id": 2, "näme": nil, "payload": []byte{}, "items": json.RawMessage(`[false,{"x":1}]`)},
			{"id": 3, "näme": "\xffUnicode 日本語"},
			nil,
		},
	}
	originalRows := make([]engine.Row, len(result.Rows))
	for i, row := range result.Rows {
		originalRows[i] = maps.Clone(row)
	}
	for _, binaryMode := range []string{"", "legacy-string"} {
		for _, pretty := range []bool{false, true} {
			opts := Options{PrettyJSON: pretty, JSONBinaryMode: binaryMode}
			rows := make([]map[string]any, len(result.Rows))
			for i, source := range result.Rows {
				row := make(map[string]any, len(result.Cols))
				for _, column := range result.Cols {
					row[column] = jsonValue(source[strings.ToLower(column)], opts)
				}
				rows[i] = row
			}
			for _, ndjson := range []bool{false, true} {
				var want, got bytes.Buffer
				encoder := json.NewEncoder(&want)
				if ndjson {
					for _, row := range rows {
						if err := encoder.Encode(row); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					if pretty {
						encoder.SetIndent("", "  ")
					}
					if err := encoder.Encode(rows); err != nil {
						t.Fatal(err)
					}
				}
				export := ExportJSON
				if ndjson {
					export = ExportNDJSON
				}
				if err := export(&got, result, opts); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got.Bytes(), want.Bytes()) {
					t.Fatalf("ndjson=%v pretty=%v binary=%q:\n got %s\nwant %s", ndjson, pretty, binaryMode, got.Bytes(), want.Bytes())
				}
			}
		}
	}
	if !reflect.DeepEqual(result.Rows, originalRows) {
		t.Fatal("export changed the source row slice")
	}
}

var errJSONExportWrite = errors.New("JSON export write failure")

type jsonExportLimitedWriter struct {
	bytes.Buffer
	remaining int
}

func (w *jsonExportLimitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		n, _ := w.Buffer.Write(p[:w.remaining])
		w.remaining = 0
		return n, errJSONExportWrite
	}
	w.remaining -= len(p)
	return w.Buffer.Write(p)
}

// Defining WriteString keeps io.WriteString from bypassing the limited Write.
func (w *jsonExportLimitedWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func TestJSONExportsPreserveWriterErrors(t *testing.T) {
	for _, export := range []struct {
		name string
		fn   func(io.Writer, *engine.ResultSet, Options) error
	}{{"json", ExportJSON}, {"ndjson", ExportNDJSON}} {
		for _, pretty := range []bool{false, true} {
			opts := Options{PrettyJSON: pretty}
			result := makeSample()
			var complete bytes.Buffer
			if err := export.fn(&complete, result, opts); err != nil {
				t.Fatal(err)
			}
			for _, limit := range []int{0, 1, complete.Len() / 2, complete.Len() - 1} {
				writer := &jsonExportLimitedWriter{remaining: limit}
				if err := export.fn(writer, result, opts); !errors.Is(err, errJSONExportWrite) {
					t.Fatalf("%s pretty=%v limit=%d: error=%v", export.name, pretty, limit, err)
				}
				if !bytes.Equal(writer.Bytes(), complete.Bytes()[:limit]) {
					t.Fatalf("%s pretty=%v limit=%d: incorrect output prefix", export.name, pretty, limit)
				}
			}
		}
	}
}

func TestJSONExportsPreserveMarshalErrors(t *testing.T) {
	result := &engine.ResultSet{Cols: []string{"value"}, Rows: []engine.Row{{"value": 1}, {"value": make(chan int)}, {"value": 3}}}
	for _, export := range []func(io.Writer, *engine.ResultSet, Options) error{ExportJSON, ExportNDJSON} {
		for _, pretty := range []bool{false, true} {
			var output bytes.Buffer
			err := export(&output, result, Options{PrettyJSON: pretty})
			var unsupported *json.UnsupportedTypeError
			if !errors.As(err, &unsupported) {
				t.Fatalf("expected unsupported-type error, got %v", err)
			}
			if strings.Contains(output.String(), "3") {
				t.Fatalf("export wrote a row after the failing record: %q", output.String())
			}
		}
	}
}
