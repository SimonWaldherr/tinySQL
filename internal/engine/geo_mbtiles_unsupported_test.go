//go:build !sqliteimport || js || wasm || baremetal

package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Without the sqliteimport build tag, the MBTiles SQL functions must still
// exist (so a query fails with a clear, actionable error rather than
// "unknown function") rather than being silently absent from the registry.
func TestMBTilesFunctionsRequireSqliteimportTag(t *testing.T) {
	db := storage.NewDB()
	execExpectError(t, db, `SELECT MBTILES_TILE('nonexistent.mbtiles', 1, 0, 0) AS v`)
	execExpectError(t, db, `SELECT * FROM MBTILES_TILES('nonexistent.mbtiles')`)
	execExpectError(t, db, `SELECT * FROM MBTILES_METADATA('nonexistent.mbtiles')`)
}
