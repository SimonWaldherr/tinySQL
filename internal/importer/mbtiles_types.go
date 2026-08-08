package importer

// MBTiles option and result types.
//
// These live outside the sqliteimport-gated files so callers can name them (and
// build) on every target, including TinyGo/WASM where the SQLite driver is
// unavailable and the operations themselves return an error.

// ExportMBTilesOptions configures a tileset export.
type ExportMBTilesOptions struct {
	// TilesTable is the tinySQL table holding zoom_level/tile_column/tile_row/
	// tile_data. Defaults to "tiles".
	TilesTable string

	// MetadataTable holds name/value metadata rows. Defaults to
	// TilesTable + "_metadata", which is where ImportMBTiles puts it. A missing
	// table is not an error; Metadata below still applies.
	MetadataTable string

	// Metadata entries are written after the metadata table's rows, overriding
	// any key of the same name. Use it to supply or correct the keys the
	// specification requires.
	Metadata map[string]string

	// BatchSize bounds how many tiles are inserted per transaction. Zero uses a
	// default. Tiles are streamed, so a tileset larger than memory can be
	// written.
	BatchSize int

	// TileRowIsTMS declares what the source tile_row column already contains.
	// The specification stores TMS (row 0 = southernmost); if the tinySQL table
	// holds XYZ rows instead, set this false and the rows are flipped on the way
	// out. Defaults to true, matching a tileset that came from ImportMBTiles.
	TileRowIsTMS bool
}

// ExportMBTilesResult reports what was written.
type ExportMBTilesResult struct {
	TilesWritten    int64
	MetadataWritten int64
	// MinZoom and MaxZoom are the observed zoom range, also written to metadata
	// when the caller supplied neither. Both are -1 for an empty tileset.
	MinZoom int
	MaxZoom int
}

// OpenMBTilesOptions configures opening an .mbtiles file for querying in place.
type OpenMBTilesOptions struct {
	// TilesTable and MetadataTable name the tinySQL tables the tileset is
	// exposed as. They default to "tiles" and "tiles_metadata".
	TilesTable    string
	MetadataTable string

	// MaxTiles bounds how many tiles are admitted, guarding against
	// accidentally materializing a continental tileset. Zero means unlimited.
	MaxTiles int64

	// Zooms restricts the import to these zoom levels. Empty means all. This is
	// the practical way to work with a large tileset in place: overview zooms
	// are a tiny fraction of the tile count.
	Zooms []int

	// WithoutTileData omits tile_data, keeping only the addressing columns and
	// per-tile size/hash. A tile *index* of even a huge tileset is small, which
	// makes coverage and integrity queries cheap.
	WithoutTileData bool
}

// OpenMBTilesResult reports what an in-place open exposed.
type OpenMBTilesResult struct {
	TilesExposed    int64
	MetadataExposed int64
	MinZoom         int
	MaxZoom         int
	// Truncated is true when MaxTiles stopped the read before the source was
	// exhausted, so the exposed table is a partial view.
	Truncated bool
}

// mbtilesDefaultBatchSize is the tile batch size used by ImportMBTiles,
// OpenMBTiles and ExportMBTiles when the caller leaves BatchSize unset.
const mbtilesDefaultBatchSize = 1000

// mbtilesDefaultTableNames fills in "tiles" and "<tiles>_metadata" for
// whichever of the two names the caller left blank.
func mbtilesDefaultTableNames(tiles, metadata string) (string, string) {
	if tiles == "" {
		tiles = "tiles"
	}
	if metadata == "" {
		metadata = tiles + "_metadata"
	}
	return tiles, metadata
}
