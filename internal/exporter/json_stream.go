package exporter

import (
	"encoding/json"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// exportJSONLegacy encodes complete rows before writing. It also supports the
// nojsonv2 build, and serves as a benchmark baseline for the streaming encoder.
func exportJSONLegacy(w io.Writer, rs *engine.ResultSet, opts Options) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	keys := resultColumnKeys(rs.Cols)
	m := make(map[string]any, len(rs.Cols))
	for i, r := range rs.Rows {
		fillJSONRowMap(m, r, rs.Cols, keys, opts)
		var b []byte
		var err error
		if opts.PrettyJSON {
			b, err = json.MarshalIndent(m, "  ", "  ")
		} else {
			b, err = json.Marshal(m)
		}
		if err != nil {
			return err
		}
		sep := ""
		if i > 0 {
			sep = ","
		}
		if opts.PrettyJSON {
			sep += "\n  "
		}
		if _, err := io.WriteString(w, sep); err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	if opts.PrettyJSON && len(rs.Rows) > 0 {
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]\n")
	return err
}
