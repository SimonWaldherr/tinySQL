//go:build !goexperiment.jsonv2

package exporter

import (
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func exportJSON(w io.Writer, rs *engine.ResultSet, opts Options) error {
	return exportJSONLegacy(w, rs, opts)
}
