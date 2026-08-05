//go:build !sqliteimport && !js && !wasm && !baremetal

package tiles_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonWaldherr/tinySQL/tiles"
)

func TestImportReportsMissingSQLiteTag(t *testing.T) {
	_, err := tiles.ImportMBTiles(context.Background(), "not-opened.mbtiles", "not-written.ttiles", nil)
	if !errors.Is(err, tiles.ErrSQLiteImportUnavailable) {
		t.Fatalf("ImportMBTiles error=%v, want ErrSQLiteImportUnavailable", err)
	}
}
