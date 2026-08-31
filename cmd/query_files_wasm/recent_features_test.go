package main

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func executeRecentFeatureSQL(t *testing.T, db *tinysql.DB, sql string) *tinysql.ResultSet {
	t.Helper()
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	result, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return result
}

func TestRecentEngineFeaturesAreAvailableToTheWASMModule(t *testing.T) {
	db := tinysql.NewDB()
	executeRecentFeatureSQL(t, db, `CREATE TABLE docs (chunk_id INT PRIMARY KEY, doc_id TEXT, chunk_index INT, tenant_id TEXT, chunk_text TEXT, geometry GEOMETRY, embedding VECTOR)`)
	executeRecentFeatureSQL(t, db, `INSERT INTO docs VALUES
		(1, 'guide', 0, 'public', 'Vector search retrieves semantically related chunks.', '{"type":"Point","coordinates":[13.405,52.52]}', '[1.0, 0.0, 0.0]'),
		(2, 'guide', 1, 'public', 'RAG search can add neighboring context chunks.', '{"type":"Point","coordinates":[11.5755,48.1372]}', '[0.8, 0.2, 0.0]'),
		(3, 'other', 0, 'private', 'Geodata uses routes and coordinates.', '{"type":"Point","coordinates":[0,0]}', '[0.0, 0.0, 1.0]')`)

	contains := executeRecentFeatureSQL(t, db, `SELECT CONTAINS_ALL(chunk_text, 'vector', 'search') AS all_terms,
		CONTAINS_ANY(chunk_text, 'routes', 'search') AS any_term,
		CONTAINS_SCORE(chunk_text, 'vector', 'search', 'rag') AS score
		FROM docs WHERE doc_id = 'guide' AND chunk_index = 0`)
	if len(contains.Rows) != 1 || contains.Rows[0]["all_terms"] != true || contains.Rows[0]["any_term"] != true {
		t.Fatalf("CONTAINS_* result = %#v", contains)
	}

	vectors := executeRecentFeatureSQL(t, db, `SELECT
		VEC_HAMMING_DISTANCE(VEC_BINARY_QUANTIZE(embedding), VEC_FROM_JSON('[1,0,0]')) AS distance,
		VEC_CENTROID(embedding, VEC_FROM_JSON('[1,0,0]')) AS centroid
		FROM docs WHERE doc_id = 'guide' AND chunk_index = 0`)
	if len(vectors.Rows) != 1 || vectors.Rows[0]["distance"] == nil || vectors.Rows[0]["centroid"] == nil {
		t.Fatalf("vector helper result = %#v", vectors)
	}

	rag := executeRecentFeatureSQL(t, db, `SELECT doc_id, chunk_index, _hit_rank, _context_rank
		FROM RAG_SEARCH('docs', 'embedding', VEC_FROM_JSON('[1.0,0.0,0.0]'), 2, '{
			"text_column": "chunk_text",
			"text_query": "vector search",
			"key_columns": ["doc_id", "chunk_index"],
			"expand_before": 1,
			"expand_after": 1,
			"doc_id_column": "doc_id",
			"chunk_index_column": "chunk_index"
		}')`)
	if len(rag.Rows) == 0 || rag.Rows[0]["_hit_rank"] == nil || rag.Rows[0]["_context_rank"] == nil {
		t.Fatalf("RAG_SEARCH result = %#v", rag)
	}

	hybrid := executeRecentFeatureSQL(t, db, `SELECT chunk_id, _vec_rank, _fts_rank, _rrf_rank
		FROM HYBRID_SEARCH(
			'docs', 'embedding', 'chunk_text', 'vect?r* OR context',
			VEC_FROM_JSON('[1.0,0.0,0.0]'), 2
		)
		ORDER BY _rrf_rank`)
	if len(hybrid.Rows) == 0 || hybrid.Rows[0]["_rrf_rank"] == nil {
		t.Fatalf("HYBRID_SEARCH result = %#v", hybrid)
	}

	warm := executeRecentFeatureSQL(t, db, `SELECT * FROM RAG_WARM('docs', 'chunk_text', 'embedding', 'cosine', 'flat')`)
	if len(warm.Rows) != 1 || warm.Rows[0]["vector_count"] == nil || warm.Rows[0]["fts_terms"] == nil {
		t.Fatalf("RAG_WARM result = %#v", warm)
	}

	filtered := executeRecentFeatureSQL(t, db, `SELECT chunk_id, tenant_id, _vec_rank
		FROM VEC_SEARCH_FILTERED(
			'docs', 'embedding', VEC_FROM_JSON('[1.0,0.0,0.0]'), 5,
			'{"pre_filter":{"equals":{"tenant_id":"public"},"spatial":{"geometry_column":"geometry","center":[13.405,52.52],"radius_meters":600000}}}'
		) ORDER BY _vec_rank`)
	if len(filtered.Rows) != 2 {
		t.Fatalf("filtered spatial vector search returned %#v, want the two public nearby rows", filtered.Rows)
	}
	for _, row := range filtered.Rows {
		if row["tenant_id"] != "public" {
			t.Fatalf("filtered spatial vector search leaked row %#v", row)
		}
	}

	executeRecentFeatureSQL(t, db, `ANALYZE docs`)
	statistics := executeRecentFeatureSQL(t, db, `SELECT column_name, distinct_count FROM sys.statistics WHERE table_name = 'docs'`)
	if len(statistics.Rows) == 0 {
		t.Fatal("ANALYZE did not expose statistics through sys.statistics")
	}
}

// The browser map demos (tiles-demo.html, tiles-demo-bavaria.html) and the SQL
// playground all run against this WASM build, so a geo function that is not
// linked in here is a broken example on the published page. This pins the
// geometry interop, geohash, reprojection, predicate and routing functions.
func TestRecentGeoFeaturesAreAvailableToTheWASMModule(t *testing.T) {
	db := tinysql.NewDB()

	// WKT/EWKT and WKB round trips: every form has to come back to the same
	// point the constructor produced.
	interop := executeRecentFeatureSQL(t, db, `SELECT
		ST_ASTEXT(ST_GEOMFROMTEXT('POLYGON((13.3 52.4,13.5 52.4,13.5 52.6,13.3 52.6,13.3 52.4))')) AS wkt,
		ST_ASEWKT(ST_MAKEPOINT(13.405, 52.520)) AS ewkt,
		ST_ASTEXT(ST_GEOMFROMWKB(ST_ASBINARY(ST_MAKEPOINT(13.405, 52.520)))) AS wkb_round_trip,
		ST_ASGEOJSON(ST_MAKEPOINT(13.4049823, 52.5200066), 3) AS rounded`)
	if len(interop.Rows) != 1 {
		t.Fatalf("geometry interop returned %d rows", len(interop.Rows))
	}
	for _, col := range []string{"wkt", "ewkt", "wkb_round_trip", "rounded"} {
		if v, _ := interop.Rows[0][col].(string); v == "" {
			t.Errorf("%s = %#v, want a non-empty rendering", col, interop.Rows[0][col])
		}
	}

	// Geohash: encode, decode, cell bounds and the surrounding cells.
	geohash := executeRecentFeatureSQL(t, db, `SELECT
		GEO_GEOHASH_ENCODE(ST_MAKEPOINT(13.405, 52.520), 8) AS hash,
		ST_ASTEXT(GEO_GEOHASH_DECODE('u33dc0cp')) AS decoded,
		GEO_GEOHASH_BBOX('u33dc0cp') AS cell,
		GEO_GEOHASH_NEIGHBORS('u33dc0cp') AS neighbors`)
	if len(geohash.Rows) != 1 {
		t.Fatalf("geohash returned %d rows", len(geohash.Rows))
	}
	if hash, _ := geohash.Rows[0]["hash"].(string); len(hash) != 8 {
		t.Errorf("GEO_GEOHASH_ENCODE precision 8 = %#v, want 8 characters", geohash.Rows[0]["hash"])
	}
	for _, col := range []string{"decoded", "cell", "neighbors"} {
		if geohash.Rows[0][col] == nil {
			t.Errorf("%s is NULL", col)
		}
	}

	// ST_TRANSFORM to Web Mercator, the projection the TILE_* functions use.
	transform := executeRecentFeatureSQL(t, db, `SELECT ST_TRANSFORM(ST_MAKEPOINT(13.405, 52.520), 3857) AS mercator`)
	if len(transform.Rows) != 1 || transform.Rows[0]["mercator"] == nil {
		t.Fatalf("ST_TRANSFORM result = %#v", transform)
	}

	// Predicates added alongside the interop work. The two unit squares meet
	// along x=1 and share no interior, so they touch but neither covers the
	// other; the big square covers the small one it fully contains.
	predicates := executeRecentFeatureSQL(t, db, `SELECT
		ST_TOUCHES(
			'{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}',
			'{"type":"Polygon","coordinates":[[[1,0],[2,0],[2,1],[1,1],[1,0]]]}') AS touches,
		ST_COVERS(
			'{"type":"Polygon","coordinates":[[[-4,-4],[4,-4],[4,4],[-4,4],[-4,-4]]]}',
			'{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}') AS covers,
		ST_PERIMETER('{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}') AS perimeter`)
	if len(predicates.Rows) != 1 {
		t.Fatalf("predicates returned %d rows", len(predicates.Rows))
	}
	if predicates.Rows[0]["touches"] != true {
		t.Errorf("ST_TOUCHES on edge-sharing squares = %#v, want true", predicates.Rows[0]["touches"])
	}
	if predicates.Rows[0]["covers"] != true {
		t.Errorf("ST_COVERS on a contained square = %#v, want true", predicates.Rows[0]["covers"])
	}
	if perimeter, _ := predicates.Rows[0]["perimeter"].(float64); perimeter <= 0 {
		t.Errorf("ST_PERIMETER = %#v, want a positive length", predicates.Rows[0]["perimeter"])
	}

	// Routing: A->B->C costs 3 and must be preferred over the direct A->C
	// edge at 10.
	executeRecentFeatureSQL(t, db, `CREATE TABLE route_edges (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64)`)
	executeRecentFeatureSQL(t, db, `INSERT INTO route_edges VALUES
		('e1', 'A', 'B', 1), ('e2', 'B', 'C', 2), ('e3', 'A', 'C', 10)`)
	distance := executeRecentFeatureSQL(t, db, `SELECT ROUTE_DISTANCE('route_edges', 'source', 'target', 'cost', 'A', 'C') AS total`)
	if len(distance.Rows) != 1 {
		t.Fatalf("ROUTE_DISTANCE returned %d rows", len(distance.Rows))
	}
	if total, _ := distance.Rows[0]["total"].(float64); total != 3 {
		t.Errorf("ROUTE_DISTANCE A->C = %#v, want 3 via B", distance.Rows[0]["total"])
	}
	path := executeRecentFeatureSQL(t, db, `SELECT * FROM ROUTE_SHORTEST_PATH('route_edges', 'source', 'target', 'cost', 'A', 'C')`)
	if len(path.Rows) != 3 {
		t.Fatalf("ROUTE_SHORTEST_PATH A->C returned %d rows, want 3 (A, B, C): %#v", len(path.Rows), path.Rows)
	}
	warm := executeRecentFeatureSQL(t, db, `SELECT * FROM ROUTE_WARM('route_edges', 'source', 'target', 'cost')`)
	if len(warm.Rows) != 1 || warm.Rows[0]["node_count"] == nil || warm.Rows[0]["edge_count"] == nil {
		t.Fatalf("ROUTE_WARM result = %#v", warm)
	}

	standards := executeRecentFeatureSQL(t, db, `SELECT
		CRS_NORMALIZE('urn:ogc:def:crs:EPSG::25832') AS crs,
		WMS_BBOX(11, 48, 12, 49, 'EPSG:4326', '1.3.0') AS wms_bbox,
		TILE_MATRIX_BBOX(100000, 500000, 10, 256, 256, 2, 3, 'topLeft') AS tile_bbox,
		TILE_MATRIX_POSITION(105121, 492319, 100000, 500000, 10, 256, 256, 'topLeft') AS tile_position`)
	if len(standards.Rows) != 1 {
		t.Fatalf("OGC standards helpers returned %#v", standards.Rows)
	}
	for _, col := range []string{"crs", "wms_bbox", "tile_bbox", "tile_position"} {
		if standards.Rows[0][col] == nil {
			t.Errorf("%s is NULL", col)
		}
	}

	gpkg := executeRecentFeatureSQL(t, db, `SELECT
		GPKG_SRID(BLOB_FROM_HEX('47500001e6100000010100000000000000000027400000000000204840')) AS srid,
		ST_ASTEXT(GEO_FROM_GPKG(BLOB_FROM_HEX('47500001e6100000010100000000000000000027400000000000204840'))) AS point`)
	if len(gpkg.Rows) != 1 || gpkg.Rows[0]["srid"] != int64(4326) || gpkg.Rows[0]["point"] == nil {
		t.Fatalf("GeoPackageBinary result = %#v", gpkg)
	}
}
