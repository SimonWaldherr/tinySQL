package importer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type kml struct {
	XMLName    xml.Name       `xml:"kml"`
	Document   *kmlDoc        `xml:"Document"`
	Placemarks []kmlPlacemark `xml:"Placemark"`
}

type kmlDoc struct {
	Placemarks []kmlPlacemark `xml:"Placemark"`
	Folders    []kmlFolder    `xml:"Folder"`
}

type kmlFolder struct {
	Placemarks []kmlPlacemark `xml:"Placemark"`
	Folders    []kmlFolder    `xml:"Folder"`
}

type kmlPlacemark struct {
	Name          string            `xml:"name"`
	Description   string            `xml:"description"`
	StyleURL      string            `xml:"styleUrl"`
	ExtendedData  *kmlExtendedData  `xml:"ExtendedData"`
	Point         *kmlPoint         `xml:"Point"`
	LineString    *kmlLineString    `xml:"LineString"`
	Polygon       *kmlPolygon       `xml:"Polygon"`
	MultiGeometry *kmlMultiGeometry `xml:"MultiGeometry"`
}

type kmlPoint struct {
	Coordinates string `xml:"coordinates"`
}
type kmlLineString struct {
	Coordinates string `xml:"coordinates"`
}
type kmlPolygon struct {
	OuterBoundary   kmlBoundary   `xml:"outerBoundaryIs"`
	InnerBoundaries []kmlBoundary `xml:"innerBoundaryIs"`
}
type kmlBoundary struct {
	LinearRing kmlLinearRing `xml:"LinearRing"`
}
type kmlLinearRing struct {
	Coordinates string `xml:"coordinates"`
}
type kmlMultiGeometry struct {
	Points          []kmlPoint         `xml:"Point"`
	LineStrings     []kmlLineString    `xml:"LineString"`
	Polygons        []kmlPolygon       `xml:"Polygon"`
	MultiGeometries []kmlMultiGeometry `xml:"MultiGeometry"`
}
type kmlExtendedData struct {
	Data       []kmlData       `xml:"Data"`
	SchemaData []kmlSchemaData `xml:"SchemaData"`
}
type kmlData struct {
	Name        string `xml:"name,attr"`
	DisplayName string `xml:"displayName"`
	Value       string `xml:"value"`
}
type kmlSchemaData struct {
	SchemaURL  string          `xml:"schemaUrl,attr"`
	SimpleData []kmlSimpleData `xml:"SimpleData"`
}
type kmlSimpleData struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
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
		lon, errLon := strconv.ParseFloat(strings.TrimSpace(comps[0]), 64)
		lat, errLat := strconv.ParseFloat(strings.TrimSpace(comps[1]), 64)
		if errLon != nil || errLat != nil {
			continue
		}
		coord := []float64{lon, lat}
		if len(comps) > 2 {
			if alt, err := strconv.ParseFloat(strings.TrimSpace(comps[2]), 64); err == nil {
				coord = append(coord, alt)
			}
		}
		coords = append(coords, coord)
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

	placemarks := collectKMLPlacemarks(root)
	if len(placemarks) == 0 {
		return nil, fmt.Errorf("no placemarks found in KML")
	}

	features := make([]map[string]any, 0, len(placemarks))
	for _, p := range placemarks {
		props := kmlProperties(p)
		geom := kmlPlacemarkGeometry(p)
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

func collectKMLPlacemarks(root kml) []kmlPlacemark {
	placemarks := append([]kmlPlacemark{}, root.Placemarks...)
	if root.Document != nil {
		placemarks = append(placemarks, root.Document.Placemarks...)
		for _, folder := range root.Document.Folders {
			placemarks = append(placemarks, collectKMLFolderPlacemarks(folder)...)
		}
	}
	return placemarks
}

func collectKMLFolderPlacemarks(folder kmlFolder) []kmlPlacemark {
	placemarks := append([]kmlPlacemark{}, folder.Placemarks...)
	for _, child := range folder.Folders {
		placemarks = append(placemarks, collectKMLFolderPlacemarks(child)...)
	}
	return placemarks
}

func kmlProperties(p kmlPlacemark) map[string]any {
	props := make(map[string]any)
	if p.Name != "" {
		props["name"] = p.Name
	}
	if p.Description != "" {
		props["description"] = p.Description
	}
	if p.StyleURL != "" {
		props["styleUrl"] = p.StyleURL
	}
	if p.ExtendedData != nil {
		for _, item := range p.ExtendedData.Data {
			if item.Name == "" {
				continue
			}
			props[item.Name] = item.Value
			if item.DisplayName != "" {
				props[item.Name+"_displayName"] = item.DisplayName
			}
		}
		for _, schema := range p.ExtendedData.SchemaData {
			if schema.SchemaURL != "" {
				props["schemaUrl"] = schema.SchemaURL
			}
			for _, item := range schema.SimpleData {
				if item.Name != "" {
					props[item.Name] = strings.TrimSpace(item.Value)
				}
			}
		}
	}
	return props
}

func kmlPlacemarkGeometry(p kmlPlacemark) any {
	if p.Point != nil {
		return kmlPointGeometry(*p.Point)
	}
	if p.LineString != nil {
		return kmlLineStringGeometry(*p.LineString)
	}
	if p.Polygon != nil {
		return kmlPolygonGeometry(*p.Polygon)
	}
	if p.MultiGeometry != nil {
		return kmlMultiGeometryValue(*p.MultiGeometry)
	}
	return nil
}

func kmlPointGeometry(point kmlPoint) any {
	coords := parseCoordinates(point.Coordinates)
	if len(coords) == 0 {
		return nil
	}
	return map[string]any{"type": "Point", "coordinates": coords[0]}
}

func kmlLineStringGeometry(line kmlLineString) any {
	coords := parseCoordinates(line.Coordinates)
	if len(coords) == 0 {
		return nil
	}
	return map[string]any{"type": "LineString", "coordinates": coords}
}

func kmlPolygonGeometry(poly kmlPolygon) any {
	outer := parseCoordinates(poly.OuterBoundary.LinearRing.Coordinates)
	if len(outer) == 0 {
		return nil
	}
	rings := make([]any, 0, 1+len(poly.InnerBoundaries))
	rings = append(rings, outer)
	for _, inner := range poly.InnerBoundaries {
		hole := parseCoordinates(inner.LinearRing.Coordinates)
		if len(hole) > 0 {
			rings = append(rings, hole)
		}
	}
	return map[string]any{"type": "Polygon", "coordinates": rings}
}

func kmlMultiGeometryValue(multi kmlMultiGeometry) any {
	geometries := make([]any, 0, len(multi.Points)+len(multi.LineStrings)+len(multi.Polygons)+len(multi.MultiGeometries))
	for _, point := range multi.Points {
		if geom := kmlPointGeometry(point); geom != nil {
			geometries = append(geometries, geom)
		}
	}
	for _, line := range multi.LineStrings {
		if geom := kmlLineStringGeometry(line); geom != nil {
			geometries = append(geometries, geom)
		}
	}
	for _, poly := range multi.Polygons {
		if geom := kmlPolygonGeometry(poly); geom != nil {
			geometries = append(geometries, geom)
		}
	}
	for _, child := range multi.MultiGeometries {
		if geom := kmlMultiGeometryValue(child); geom != nil {
			geometries = append(geometries, geom)
		}
	}
	if len(geometries) == 0 {
		return nil
	}
	return map[string]any{"type": "GeometryCollection", "geometries": geometries}
}

func bytesNewReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
