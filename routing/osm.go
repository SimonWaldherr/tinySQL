package routing

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// ReadOSM reads an OSM XML snapshot. Definitions may appear in any order;
// coordinate and topology validation happens during Build. PBF needs an
// external conversion to OSM XML or a tinySQL OSM table first.
func ReadOSM(ctx context.Context, src io.Reader) (Data, error) {
	var data Data
	decoder := xml.NewDecoder(src)
	root := false
	type tag struct {
		K string `xml:"k,attr"`
		V string `xml:"v,attr"`
	}
	tags := func(values []tag) map[string]string {
		m := make(map[string]string, len(values))
		for _, v := range values {
			m[v.K] = v.V
		}
		return m
	}
	for {
		if err := check(ctx); err != nil {
			return Data{}, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Data{}, err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if !root {
			if start.Name.Local != "osm" {
				return Data{}, fmt.Errorf("routing: expected OSM XML snapshot")
			}
			root = true
			continue
		}
		switch start.Name.Local {
		case "node":
			var n struct {
				ID   int64    `xml:"id,attr"`
				Lat  *float64 `xml:"lat,attr"`
				Lon  *float64 `xml:"lon,attr"`
				Tags []tag    `xml:"tag"`
			}
			if err := decoder.DecodeElement(&n, &start); err != nil {
				return Data{}, err
			}
			if n.Lat == nil || n.Lon == nil {
				return Data{}, fmt.Errorf("node %d: missing coordinates", n.ID)
			}
			data.Nodes = append(data.Nodes, Node{n.ID, Point{*n.Lon, *n.Lat}, tags(n.Tags)})
		case "way":
			var w struct {
				ID   int64 `xml:"id,attr"`
				Refs []struct {
					Ref int64 `xml:"ref,attr"`
				} `xml:"nd"`
				Tags []tag `xml:"tag"`
			}
			if err := decoder.DecodeElement(&w, &start); err != nil {
				return Data{}, err
			}
			way := Way{ID: w.ID, Tags: tags(w.Tags)}
			for _, ref := range w.Refs {
				way.Nodes = append(way.Nodes, ref.Ref)
			}
			data.Ways = append(data.Ways, way)
		case "relation":
			var r struct {
				ID      int64 `xml:"id,attr"`
				Members []struct {
					Type string `xml:"type,attr"`
					Ref  int64  `xml:"ref,attr"`
					Role string `xml:"role,attr"`
				} `xml:"member"`
				Tags []tag `xml:"tag"`
			}
			if err := decoder.DecodeElement(&r, &start); err != nil {
				return Data{}, err
			}
			rel := Relation{ID: r.ID, Tags: tags(r.Tags)}
			for _, m := range r.Members {
				rel.Members = append(rel.Members, Member{m.Type, m.Ref, m.Role})
			}
			data.Relations = append(data.Relations, rel)
		default:
			if err := decoder.Skip(); err != nil {
				return Data{}, err
			}
		}
	}
	if !root {
		return Data{}, fmt.Errorf("routing: empty OSM snapshot")
	}
	return data, nil
}

// FromDB builds from the normalized table produced by importer.ImportOSM.
// Execute takes a consistent snapshot; later DB writes do not mutate a Router.
func FromDB(ctx context.Context, db *tinysql.DB, tenant, table string, profile Profile, opts Options) (*Router, error) {
	if table == "" || strings.IndexFunc(table, func(r rune) bool {
		return !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
	}) >= 0 {
		return nil, fmt.Errorf("routing: table must be a simple SQL identifier")
	}
	stmt, err := tinysql.ParseSQL("SELECT osm_type, osm_id, lat, lon, tags, refs, members FROM " + table)
	if err != nil {
		return nil, err
	}
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return nil, err
	}
	var data Data
	decode := func(v any, out any) error {
		if v == nil {
			return nil
		}
		var raw []byte
		switch x := v.(type) {
		case string:
			raw = []byte(x)
		case []byte:
			raw = x
		case json.RawMessage:
			raw = x
		default:
			var e error
			raw, e = json.Marshal(v)
			if e != nil {
				return e
			}
		}
		if len(strings.TrimSpace(string(raw))) == 0 {
			return nil
		}
		return json.Unmarshal(raw, out)
	}
	for _, row := range result.Rows {
		id, err := strconv.ParseInt(fmt.Sprint(row["osm_id"]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("routing: invalid OSM id: %w", err)
		}
		var tags map[string]string
		if err := decode(row["tags"], &tags); err != nil {
			return nil, err
		}
		switch row["osm_type"] {
		case "node":
			lat, e := strconv.ParseFloat(fmt.Sprint(row["lat"]), 64)
			if e != nil {
				return nil, e
			}
			lon, e := strconv.ParseFloat(fmt.Sprint(row["lon"]), 64)
			if e != nil {
				return nil, e
			}
			data.Nodes = append(data.Nodes, Node{id, Point{lon, lat}, tags})
		case "way":
			w := Way{ID: id, Tags: tags}
			if err := decode(row["refs"], &w.Nodes); err != nil {
				return nil, err
			}
			data.Ways = append(data.Ways, w)
		case "relation":
			r := Relation{ID: id, Tags: tags}
			if err := decode(row["members"], &r.Members); err != nil {
				return nil, err
			}
			data.Relations = append(data.Relations, r)
		}
	}
	return Build(ctx, data, profile, opts)
}
