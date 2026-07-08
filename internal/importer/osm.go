package importer

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type osmTag struct {
	K string `xml:"k,attr"`
	V string `xml:"v,attr"`
}

type osmNd struct {
	Ref int64 `xml:"ref,attr"`
}

type osmMember struct {
	Type string `xml:"type,attr" json:"type"`
	Ref  int64  `xml:"ref,attr" json:"ref"`
	Role string `xml:"role,attr" json:"role,omitempty"`
}

type osmNode struct {
	ID        int64    `xml:"id,attr"`
	Lat       float64  `xml:"lat,attr"`
	Lon       float64  `xml:"lon,attr"`
	Version   int64    `xml:"version,attr"`
	Timestamp string   `xml:"timestamp,attr"`
	UID       int64    `xml:"uid,attr"`
	User      string   `xml:"user,attr"`
	Tags      []osmTag `xml:"tag"`
}

type osmWay struct {
	ID        int64    `xml:"id,attr"`
	Version   int64    `xml:"version,attr"`
	Timestamp string   `xml:"timestamp,attr"`
	UID       int64    `xml:"uid,attr"`
	User      string   `xml:"user,attr"`
	Nds       []osmNd  `xml:"nd"`
	Tags      []osmTag `xml:"tag"`
}

type osmRelation struct {
	ID        int64       `xml:"id,attr"`
	Version   int64       `xml:"version,attr"`
	Timestamp string      `xml:"timestamp,attr"`
	UID       int64       `xml:"uid,attr"`
	User      string      `xml:"user,attr"`
	Members   []osmMember `xml:"member"`
	Tags      []osmTag    `xml:"tag"`
}

type osmCoord struct {
	Lon float64
	Lat float64
}

// ImportOSM imports OSM XML (.osm or .osm.xml) into one table. Nodes, ways and
// relations share a normalized row shape; ways get GeoJSON LineString/Polygon
// geometry when referenced node coordinates are present earlier in the file.
func ImportOSM(
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

	colNames := []string{"osm_type", "osm_id", "version", "timestamp", "uid", "user", "lat", "lon", "tags", "refs", "members", "geometry_type", "geometry"}
	colTypes := []storage.ColType{
		storage.TextType,
		storage.Int64Type,
		storage.Int64Type,
		storage.TextType,
		storage.Int64Type,
		storage.TextType,
		storage.Float64Type,
		storage.Float64Type,
		storage.JsonType,
		storage.JsonType,
		storage.JsonType,
		storage.TextType,
		storage.GeometryType,
	}
	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	dec := xml.NewDecoder(src)
	nodeCoords := make(map[int64]osmCoord)
	rows := make([][]any, 0)

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse osm xml: %w", err)
		}

		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		switch start.Name.Local {
		case "node":
			var n osmNode
			if err := dec.DecodeElement(&n, &start); err != nil {
				return nil, fmt.Errorf("decode osm node: %w", err)
			}
			nodeCoords[n.ID] = osmCoord{Lon: n.Lon, Lat: n.Lat}
			rows = append(rows, osmNodeRow(n))
		case "way":
			var w osmWay
			if err := dec.DecodeElement(&w, &start); err != nil {
				return nil, fmt.Errorf("decode osm way: %w", err)
			}
			rows = append(rows, osmWayRow(w, nodeCoords))
		case "relation":
			var r osmRelation
			if err := dec.DecodeElement(&r, &start); err != nil {
				return nil, fmt.Errorf("decode osm relation: %w", err)
			}
			rows = append(rows, osmRelationRow(r))
		}
	}

	if err := insertTypedRows(ctx, db, tenant, tableName, colNames, colTypes, rows, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

func osmNodeRow(n osmNode) []any {
	geom := map[string]any{"type": "Point", "coordinates": []float64{n.Lon, n.Lat}}
	return []any{"node", n.ID, n.Version, n.Timestamp, n.UID, n.User, n.Lat, n.Lon, osmTagsJSON(n.Tags), nil, nil, "Point", marshalJSONValue(geom)}
}

func osmWayRow(w osmWay, nodeCoords map[int64]osmCoord) []any {
	refs := make([]int64, len(w.Nds))
	coords := make([][]float64, 0, len(w.Nds))
	for i, nd := range w.Nds {
		refs[i] = nd.Ref
		if c, ok := nodeCoords[nd.Ref]; ok {
			coords = append(coords, []float64{c.Lon, c.Lat})
		}
	}

	var geomType string
	var geom any
	if len(coords) == len(refs) && len(coords) >= 2 {
		closed := len(coords) >= 4 && coords[0][0] == coords[len(coords)-1][0] && coords[0][1] == coords[len(coords)-1][1]
		if closed {
			geomType = "Polygon"
			geom = map[string]any{"type": geomType, "coordinates": []any{coords}}
		} else {
			geomType = "LineString"
			geom = map[string]any{"type": geomType, "coordinates": coords}
		}
	}

	return []any{"way", w.ID, w.Version, w.Timestamp, w.UID, w.User, nil, nil, osmTagsJSON(w.Tags), marshalJSONValue(refs), nil, geomType, marshalJSONValue(geom)}
}

func osmRelationRow(r osmRelation) []any {
	return []any{"relation", r.ID, r.Version, r.Timestamp, r.UID, r.User, nil, nil, osmTagsJSON(r.Tags), nil, marshalJSONValue(r.Members), "", nil}
}

func osmTagsJSON(tags []osmTag) json.RawMessage {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.K] = tag.V
	}
	return marshalJSONValue(m)
}
