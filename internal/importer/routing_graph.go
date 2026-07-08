package importer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportRoutingGraph imports routing graph data from JSON or CSV. JSON may be
// either {"nodes":[...],"edges":[...]} or a plain edge array. CSV is treated as
// an edge list using common source/target/cost/distance/duration/mode columns.
func ImportRoutingGraph(
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

	data, err := io.ReadAll(src)
	if err != nil {
		return nil, fmt.Errorf("read routing graph: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("routing graph input is empty")
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		if looksLikeRoutingGraphNDJSON(trimmed) {
			return importRoutingGraphNDJSON(ctx, db, tenant, tableName, trimmed, opts)
		}
		return importRoutingGraphJSON(ctx, db, tenant, tableName, trimmed, opts)
	}
	return importRoutingGraphCSV(ctx, db, tenant, tableName, bytes.NewReader(data), opts)
}

func importRoutingGraphJSON(ctx context.Context, db *storage.DB, tenant, tableName string, data []byte, opts *ImportOptions) (*ImportResult, error) {
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("parse routing graph json: %w", err)
	}

	nodes := make([]map[string]any, 0)
	edges := make([]map[string]any, 0)
	switch v := payload.(type) {
	case map[string]any:
		nodes = mapSlice(v["nodes"])
		edges = mapSlice(v["edges"])
	case []any:
		edges = mapSlice(v)
	default:
		return nil, fmt.Errorf("routing graph JSON must be an object or array")
	}

	if len(nodes) > 0 {
		if err := insertRoutingNodes(ctx, db, tenant, tableName+"_nodes", nodes, opts); err != nil {
			return nil, err
		}
	}
	return insertRoutingEdges(ctx, db, tenant, tableName, edges, opts)
}

func importRoutingGraphNDJSON(ctx context.Context, db *storage.DB, tenant, tableName string, data []byte, opts *ImportOptions) (*ImportResult, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)

	nodes := make([]map[string]any, 0)
	edges := make([]map[string]any, 0)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("parse routing graph ndjson line %d: %w", lineNo, err)
		}
		switch kind := routingGraphRecordKind(record); kind {
		case "node":
			nodes = append(nodes, unwrapRoutingGraphRecord(record, "node"))
		case "edge":
			edges = append(edges, unwrapRoutingGraphRecord(record, "edge"))
		default:
			return nil, fmt.Errorf("routing graph ndjson line %d is neither node nor edge", lineNo)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read routing graph ndjson: %w", err)
	}

	if len(nodes) > 0 {
		if err := insertRoutingNodes(ctx, db, tenant, tableName+"_nodes", nodes, opts); err != nil {
			return nil, err
		}
	}
	return insertRoutingEdges(ctx, db, tenant, tableName, edges, opts)
}

func importRoutingGraphCSV(ctx context.Context, db *storage.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	r := csv.NewReader(src)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read routing graph csv header: %w", err)
	}
	header = sanitizeColumnNames(header)
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[strings.ToLower(name)] = i
	}

	edges := make([]map[string]any, 0)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read routing graph csv: %w", err)
		}
		edge := make(map[string]any)
		props := make(map[string]any)
		for i, name := range header {
			var val string
			if i < len(rec) {
				val = rec[i]
			}
			switch strings.ToLower(name) {
			case "id", "edge_id", "source", "from", "source_id", "target", "to", "target_id", "mode", "geometry":
				edge[name] = val
			case "cost", "weight", "distance", "duration":
				edge[name] = parseGraphFloat(val)
			default:
				props[name] = val
			}
		}
		normalizeCSVEdgeKeys(edge, idx)
		if len(props) > 0 {
			edge["properties"] = props
		}
		edges = append(edges, edge)
	}

	return insertRoutingEdges(ctx, db, tenant, tableName, edges, opts)
}

func insertRoutingNodes(ctx context.Context, db *storage.DB, tenant, tableName string, nodes []map[string]any, opts *ImportOptions) error {
	colNames := []string{"node_id", "lat", "lon", "properties", "geometry_type", "geometry"}
	colTypes := []storage.ColType{storage.TextType, storage.Float64Type, storage.Float64Type, storage.JsonType, storage.TextType, storage.GeometryType}
	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	rows := make([][]any, 0, len(nodes))
	for _, node := range nodes {
		id := graphString(node, "id", "node_id")
		lat := graphFloat(node, "lat", "latitude", "y")
		lon := graphFloat(node, "lon", "lng", "longitude", "x")
		props := graphProperties(node, "id", "node_id", "lat", "latitude", "y", "lon", "lng", "longitude", "x", "properties")
		if p, ok := node["properties"]; ok {
			props = mergeGraphProperties(props, p)
		}
		geom := map[string]any{"type": "Point", "coordinates": []float64{lon, lat}}
		rows = append(rows, []any{id, lat, lon, marshalJSONValue(props), "Point", marshalJSONValue(geom)})
	}

	return insertTypedRows(ctx, db, tenant, tableName, colNames, colTypes, rows, opts, result)
}

func insertRoutingEdges(ctx context.Context, db *storage.DB, tenant, tableName string, edges []map[string]any, opts *ImportOptions) (*ImportResult, error) {
	colNames := []string{"edge_id", "source", "target", "cost", "distance", "duration", "mode", "properties", "geometry_type", "geometry"}
	colTypes := []storage.ColType{storage.TextType, storage.TextType, storage.TextType, storage.Float64Type, storage.Float64Type, storage.Float64Type, storage.TextType, storage.JsonType, storage.TextType, storage.GeometryType}
	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	rows := make([][]any, 0, len(edges))
	for _, edge := range edges {
		geom, geomType := routingGeometry(edge["geometry"])
		props := graphProperties(edge, "id", "edge_id", "source", "from", "source_id", "target", "to", "target_id", "cost", "weight", "distance", "duration", "mode", "geometry", "properties")
		if p, ok := edge["properties"]; ok {
			props = mergeGraphProperties(props, p)
		}
		rows = append(rows, []any{
			graphString(edge, "id", "edge_id"),
			graphString(edge, "source", "from", "source_id"),
			graphString(edge, "target", "to", "target_id"),
			graphFloat(edge, "cost", "weight"),
			graphFloat(edge, "distance"),
			graphFloat(edge, "duration"),
			graphString(edge, "mode"),
			marshalJSONValue(props),
			geomType,
			marshalJSONValue(geom),
		})
	}

	if err := insertTypedRows(ctx, db, tenant, tableName, colNames, colTypes, rows, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

func mapSlice(v any) []map[string]any {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func looksLikeRoutingGraphNDJSON(data []byte) bool {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	records := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if line[0] != '{' {
			return false
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			return false
		}
		records++
	}
	return records > 1
}

func routingGraphRecordKind(record map[string]any) string {
	if _, ok := record["node"].(map[string]any); ok {
		return "node"
	}
	if _, ok := record["edge"].(map[string]any); ok {
		return "edge"
	}
	kind := strings.ToLower(graphString(record, "type", "record_type", "kind"))
	switch kind {
	case "node", "nodes", "vertex", "vertices":
		return "node"
	case "edge", "edges", "link", "links":
		return "edge"
	}
	if hasAnyGraphKey(record, "source", "from", "source_id") && hasAnyGraphKey(record, "target", "to", "target_id") {
		return "edge"
	}
	if hasAnyGraphKey(record, "lat", "latitude", "y") && hasAnyGraphKey(record, "lon", "lng", "longitude", "x") {
		return "node"
	}
	return ""
}

func unwrapRoutingGraphRecord(record map[string]any, key string) map[string]any {
	if inner, ok := record[key].(map[string]any); ok {
		return inner
	}
	return record
}

func hasAnyGraphKey(record map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := record[key]; ok {
			return true
		}
	}
	return false
}

func graphString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func graphFloat(m map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			switch x := v.(type) {
			case float64:
				return x
			case float32:
				return float64(x)
			case int:
				return float64(x)
			case int64:
				return float64(x)
			case json.Number:
				f, _ := x.Float64()
				return f
			case string:
				return parseGraphFloat(x)
			}
		}
	}
	return 0
}

func parseGraphFloat(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func graphProperties(m map[string]any, excluded ...string) map[string]any {
	skip := make(map[string]bool, len(excluded))
	for _, key := range excluded {
		skip[key] = true
	}
	props := make(map[string]any)
	for key, val := range m {
		if !skip[key] {
			props[key] = val
		}
	}
	return props
}

func mergeGraphProperties(props map[string]any, extra any) map[string]any {
	if props == nil {
		props = make(map[string]any)
	}
	if extraMap, ok := extra.(map[string]any); ok {
		for key, val := range extraMap {
			props[key] = val
		}
	}
	return props
}

func routingGeometry(v any) (any, string) {
	switch g := v.(type) {
	case nil:
		return nil, ""
	case string:
		var decoded any
		if json.Unmarshal([]byte(g), &decoded) == nil {
			return routingGeometry(decoded)
		}
		return nil, ""
	case map[string]any:
		if typ, ok := g["type"].(string); ok {
			return g, typ
		}
	}
	return v, ""
}

func normalizeCSVEdgeKeys(edge map[string]any, idx map[string]int) {
	aliases := map[string][]string{
		"id":       {"edge_id"},
		"source":   {"from", "source_id"},
		"target":   {"to", "target_id"},
		"cost":     {"weight"},
		"distance": {"distance"},
		"duration": {"duration"},
	}
	for canonical, keys := range aliases {
		if _, ok := edge[canonical]; ok {
			continue
		}
		for _, key := range keys {
			if _, present := idx[key]; present {
				edge[canonical] = edge[key]
				break
			}
		}
	}
}
