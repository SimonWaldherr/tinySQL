package tinysql

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// VectorCacheConfig configures the optional process-wide VEC_SEARCH result
// cache and its opt-in analytics ring buffer.
type VectorCacheConfig = engine.VectorCacheConfig

// VectorCacheStats reports bounded result-cache state and, when enabled, the
// recent query window. It is a snapshot and safe to serialize for monitoring.
type VectorCacheStats = engine.VectorCacheStats

// VectorQueryEvent describes one recent VEC_SEARCH shape without exposing its
// raw vector contents.
type VectorQueryEvent = engine.VectorQueryEvent

// ConfigureVectorCache replaces the process-wide VEC_SEARCH result-cache and
// analytics configuration. A zero ResultCacheEntries disables result caching.
func ConfigureVectorCache(cfg VectorCacheConfig) { engine.ConfigureVectorCache(cfg) }

// VectorCacheAnalytics returns a point-in-time cache and optional query-window
// snapshot. It does not expose raw query-vector contents.
func VectorCacheAnalytics() VectorCacheStats { return engine.VectorCacheAnalytics() }

// DefaultVectorCacheConfig returns bounded cache/analytics defaults with both
// optional features disabled until explicitly enabled by the caller.
func DefaultVectorCacheConfig() VectorCacheConfig { return engine.DefaultVectorCacheConfig() }

// RAGPreFilter is the public before-ranking authorization/metadata boundary
// for RAG_SEARCH, HYBRID_SEARCH, VEC_SEARCH_FILTERED, and
// FTS_SEARCH_FILTERED. It deliberately represents stable application IDs,
// never mutable physical table-row offsets.
//
// Set IDColumn when AllowedRowIDs refers to a column other than a single-column
// PRIMARY KEY. Equals is an AND of exact metadata values (for example tenant,
// visibility, or document type); it is intersected with AllowedRowIDs when
// both are present. See RAGPreFilterJSON for the SQL options payload.
type RAGPreFilter struct {
	IDColumn      string            `json:"id_column,omitempty"`
	AllowedRowIDs []any             `json:"allowed_row_ids"`
	Equals        map[string]any    `json:"equals"`
	Spatial       *RAGSpatialFilter `json:"spatial,omitempty"`
}

// RAGSpatialFilter limits retrieval to georeferenced rows before ranking.
// Set GeometryColumn and exactly one of BBox or Center+RadiusMeters. Coordinates
// are WGS84 x/y: BBox is [west,south,east,north], Center is [lon,lat]. BBox
// matches geometry extents; radius measures from geometry centroids.
type RAGSpatialFilter struct {
	GeometryColumn string    `json:"geometry_column"`
	BBox           []float64 `json:"bbox,omitempty"`
	Center         []float64 `json:"center,omitempty"`
	RadiusMeters   float64   `json:"radius_meters,omitempty"`
}

// RAGPreFilterJSON encodes filter as the explicit {"pre_filter": ...} JSON
// options object accepted by TinySQL's filtered retrieval functions. It is
// useful for database/sql callers that bind the options JSON as one parameter:
//
//	options, err := tinysql.RAGPreFilterJSON(tinysql.RAGPreFilter{
//		Equals: map[string]any{"tenant_id": "acme"},
//	})
//	rows, err := db.QueryContext(ctx, `SELECT * FROM HYBRID_SEARCH(?, ?, ?, ?, ?, ?, ?)`,
//		"chunks", "embedding", "search_text", query, vector, 8, options)
//
// Unlike an outer WHERE, this boundary is applied before vector/FTS candidate
// selection, reciprocal-rank fusion, and neighbor-context expansion.
func RAGPreFilterJSON(filter RAGPreFilter) (string, error) {
	if filter.AllowedRowIDs == nil && len(filter.Equals) == 0 && filter.Spatial == nil {
		return "", fmt.Errorf("RAG pre-filter requires AllowedRowIDs, at least one Equals value, or Spatial")
	}
	if filter.AllowedRowIDs == nil && strings.TrimSpace(filter.IDColumn) != "" {
		return "", fmt.Errorf("RAG pre-filter IDColumn requires AllowedRowIDs")
	}
	if spatial := filter.Spatial; spatial != nil {
		if strings.TrimSpace(spatial.GeometryColumn) == "" {
			return "", fmt.Errorf("RAG spatial pre-filter requires GeometryColumn")
		}
		bboxMode := spatial.BBox != nil
		radiusMode := spatial.Center != nil || spatial.RadiusMeters != 0
		if bboxMode == radiusMode {
			return "", fmt.Errorf("RAG spatial pre-filter requires exactly one of BBox or Center with RadiusMeters")
		}
		finite := func(values []float64) bool {
			for _, value := range values {
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return false
				}
			}
			return true
		}
		if bboxMode && (len(spatial.BBox) != 4 || !finite(spatial.BBox)) {
			return "", fmt.Errorf("RAG spatial pre-filter BBox must contain four finite values")
		}
		if bboxMode && (spatial.BBox[0] < -180 || spatial.BBox[0] > 180 || spatial.BBox[2] < -180 || spatial.BBox[2] > 180 || spatial.BBox[1] < -90 || spatial.BBox[3] > 90 || spatial.BBox[1] > spatial.BBox[3]) {
			return "", fmt.Errorf("RAG spatial pre-filter BBox requires WGS84 west/east and ordered south/north bounds")
		}
		if radiusMode && (len(spatial.Center) != 2 || !finite(spatial.Center) || spatial.RadiusMeters <= 0 || math.IsNaN(spatial.RadiusMeters) || math.IsInf(spatial.RadiusMeters, 0)) {
			return "", fmt.Errorf("RAG spatial pre-filter requires a finite two-value Center and positive finite RadiusMeters")
		}
		if radiusMode && (spatial.Center[0] < -180 || spatial.Center[0] > 180 || spatial.Center[1] < -90 || spatial.Center[1] > 90) {
			return "", fmt.Errorf("RAG spatial pre-filter Center requires WGS84 longitude/latitude")
		}
	}

	// A pointer to the slice preserves the important distinction between a nil
	// AllowedRowIDs (not supplied) and []any{} (an explicit deny-all ACL), while
	// omitting unrelated null fields from the generated SQL options object.
	type encodedFilter struct {
		IDColumn      string            `json:"id_column,omitempty"`
		AllowedRowIDs *[]any            `json:"allowed_row_ids,omitempty"`
		Equals        map[string]any    `json:"equals,omitempty"`
		Spatial       *RAGSpatialFilter `json:"spatial,omitempty"`
	}
	var allowedRowIDs *[]any
	if filter.AllowedRowIDs != nil {
		allowedRowIDs = &filter.AllowedRowIDs
	}
	value := struct {
		PreFilter encodedFilter `json:"pre_filter"`
	}{PreFilter: encodedFilter{
		IDColumn:      filter.IDColumn,
		AllowedRowIDs: allowedRowIDs,
		Equals:        filter.Equals,
		Spatial:       filter.Spatial,
	}}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode RAG pre-filter: %w", err)
	}
	return string(raw), nil
}
