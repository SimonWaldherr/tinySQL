package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Minimal KML structs
type kml struct {
	XMLName    xml.Name       `xml:"kml"`
	Document   *kmlDoc        `xml:"Document"`
	Placemarks []kmlPlacemark `xml:"Placemark"`
}

type kmlDoc struct {
	Placemarks []kmlPlacemark `xml:"Placemark"`
}

type kmlPlacemark struct {
	Name        string         `xml:"name"`
	Description string         `xml:"description"`
	Point       *kmlPoint      `xml:"Point"`
	LineString  *kmlLineString `xml:"LineString"`
	Polygon     *kmlPolygon    `xml:"Polygon"`
}

type kmlPoint struct {
	Coordinates string `xml:"coordinates"`
}
type kmlLineString struct {
	Coordinates string `xml:"coordinates"`
}
type kmlPolygon struct {
	OuterBoundary kmlOuter `xml:"outerBoundaryIs"`
}
type kmlOuter struct {
	LinearRing kmlLinearRing `xml:"LinearRing"`
}
type kmlLinearRing struct {
	Coordinates string `xml:"coordinates"`
}

// parseCoordinates parses KML coordinate text: lon,lat[,alt] pairs separated by spaces or newlines
func parseCoordinates(s string) [][]float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Fields(s)
	coords := make([][]float64, 0, len(parts))
	for _, p := range parts {
		comps := strings.Split(strings.TrimSpace(p), ",")
		if len(comps) < 2 {
			continue
		}
		// lon, lat
		var lon, lat float64
		fmt.Sscanf(comps[0], "%f", &lon)
		fmt.Sscanf(comps[1], "%f", &lat)
		coords = append(coords, []float64{lon, lat})
	}
	return coords
}

// ImportKML imports KML Placemarks into a tinySQL table. Names/descriptions become properties.
func ImportKML(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	br := bufio.NewReader(src)
	dec := xml.NewDecoder(br)
	var root kml
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("decode kml: %w", err)
	}

	var placemarks []kmlPlacemark
	if root.Document != nil && len(root.Document.Placemarks) > 0 {
		placemarks = root.Document.Placemarks
	} else if len(root.Placemarks) > 0 {
		placemarks = root.Placemarks
	} else {
		return nil, fmt.Errorf("no placemarks found in KML")
	}

	features := make([]map[string]any, 0, len(placemarks))
	for _, p := range placemarks {
		props := map[string]any{"name": p.Name, "description": p.Description}
		var geom any
		if p.Point != nil {
			coords := parseCoordinates(p.Point.Coordinates)
			if len(coords) > 0 {
				geom = map[string]any{"type": "Point", "coordinates": coords[0]}
			}
		} else if p.LineString != nil {
			coords := parseCoordinates(p.LineString.Coordinates)
			geom = map[string]any{"type": "LineString", "coordinates": coords}
		} else if p.Polygon != nil {
			coords := parseCoordinates(p.Polygon.OuterBoundary.LinearRing.Coordinates)
			geom = map[string]any{"type": "Polygon", "coordinates": []any{coords}}
		}
		features = append(features, map[string]any{"type": "Feature", "properties": props, "geometry": geom})
	}

	if len(features) == 0 {
		return nil, fmt.Errorf("no features extracted from KML")
	}

	// Marshal to FeatureCollection and delegate to ImportGeoJSON
	fc := map[string]any{"type": "FeatureCollection", "features": features}
	b, err := json.Marshal(fc)
	if err != nil {
		return nil, fmt.Errorf("marshal featurecollection: %w", err)
	}

	return ImportGeoJSON(ctx, db, tenant, tableName, bytesNewReader(b), opts)
}

// bytesNewReader here to avoid adding bytes import; simple wrapper
func bytesNewReader(b []byte) *strings.Reader { return strings.NewReader(string(b)) }
