//go:build goexperiment.jsonv2

package exporter

import (
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"math"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func exportJSON(w io.Writer, rs *engine.ResultSet, opts Options) error {
	writer := &jsonStreamWriter{Writer: w}
	err := writeJSONStream(writer, rs, opts)
	if writer.err != nil {
		return writer.err
	}
	return err
}

// Preserve the exact writer error returned by the legacy encoders. jsontext
// otherwise wraps it with streaming context, changing sentinel comparisons.
type jsonStreamWriter struct {
	io.Writer
	err error
}

func (w *jsonStreamWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	if err != nil && w.err == nil {
		w.err = err
	}
	return n, err
}

// writeJSONStream uses v1-compatible options for key ordering, escaping and
// custom marshalers.
func writeJSONStream(w io.Writer, rs *engine.ResultSet, opts Options) error {
	options := []jsontext.Options{json.DefaultOptionsV1()}
	if opts.PrettyJSON {
		options = append(options, jsontext.WithIndent("  "))
	}
	enc := jsontext.NewEncoder(w, options...)
	if err := enc.WriteToken(jsontext.BeginArray); err != nil {
		return err
	}
	keys := resultColumnKeys(rs.Cols)
	row := make(map[string]any, len(rs.Cols))
	for _, r := range rs.Rows {
		fillJSONRowMap(row, r, rs.Cols, keys, opts)
		if jsonStreamSimpleRow(row) {
			if err := jsonv2.MarshalEncode(enc, row); err != nil {
				return err
			}
		} else {
			// Marshal unknown values as a standalone row: a custom
			// MarshalJSONTo method may inspect its encoder's stack.
			data, err := json.Marshal(row)
			if err != nil {
				return err
			}
			if err := enc.WriteValue(data); err != nil {
				return err
			}
		}
	}
	return enc.WriteToken(jsontext.EndArray)
}

// Limit direct streaming to known scalar values and the standard BLOB envelope.
// Unknown types can implement context-sensitive JSON methods or contain cycles.
func jsonStreamSimpleRow(row map[string]any) bool {
	for _, value := range row {
		switch v := value.(type) {
		case nil, bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, []byte, map[string]string:
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return false
			}
		case float32:
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return false
			}
		default:
			return false
		}
	}
	return true
}
