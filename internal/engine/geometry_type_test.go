package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeometryColumnTypeRoundTrips(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE places (id INT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO places VALUES (1, '{"type":"Point","coordinates":[13.405,52.52]}')`)

	rs := execSQL(t, db, `SELECT geom FROM places WHERE id = 1`)
	stored, ok := rs.Rows[0]["geom"].(string)
	if !ok {
		t.Fatalf("geom column is %T, want string (GEOMETRY must not silently become DECIMAL)", rs.Rows[0]["geom"])
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(stored), &object); err != nil {
		t.Fatalf("stored geometry is not valid JSON: %v", err)
	}
	if object["type"] != "Point" {
		t.Errorf("stored geometry type = %v, want Point", object["type"])
	}
}

func TestGeometryColumnRejectsNonGeometryValues(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE places (id INT, geom GEOMETRY)`)
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO places VALUES (1, 123)`)); err == nil {
		t.Errorf("inserting a bare number into a GEOMETRY column succeeded, want an error")
	}
}

func TestGeometryColumnRejectsFeature(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE places (id INT, geom GEOMETRY)`)
	feature := `{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[1,2]}}`
	_, err := Execute(context.Background(), db, "default", mustParse(
		`INSERT INTO places VALUES (1, '`+feature+`')`))
	if err == nil {
		t.Fatalf("inserting a Feature into a GEOMETRY column succeeded, want an error")
	}
}

func TestCastToGeometry(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT CAST('{"type":"Point","coordinates":[1,2]}' AS GEOMETRY) AS g`)
	g, ok := rs.Rows[0]["g"].(string)
	if !ok {
		t.Fatalf("CAST(... AS GEOMETRY) result is %T, want string", rs.Rows[0]["g"])
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(g), &object); err != nil {
		t.Fatalf("cast result is not valid JSON: %v", err)
	}
	if object["type"] != "Point" {
		t.Errorf("cast result type = %v, want Point", object["type"])
	}
}

// TestCastGeometryToTextDoesNotMangle guards a real bug that would exist if
// GEOMETRY were stored as json.RawMessage/[]byte instead of a plain string:
// castValue's TEXT case is fmt.Sprintf("%v", val), which prints a []byte as
// a decimal-numbers-in-brackets dump ("[123 34 116 ...]"), not readable
// text. Storing GEOMETRY as string is what keeps this working.
func TestCastGeometryToTextDoesNotMangle(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE places (id INT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO places VALUES (1, '{"type":"Point","coordinates":[1,2]}')`)
	rs := execSQL(t, db, `SELECT CAST(geom AS TEXT) AS g FROM places WHERE id = 1`)
	g, ok := rs.Rows[0]["g"].(string)
	if !ok {
		t.Fatalf("CAST(geom AS TEXT) result is %T, want string", rs.Rows[0]["g"])
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(g), &object); err != nil {
		t.Fatalf("CAST(geom AS TEXT) produced unreadable text %q: %v", g, err)
	}
}

func TestGeometryCanonicalizationIsKeyOrderIndependent(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE places (id INT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO places VALUES (1, '{"type":"Point","coordinates":[1,2]}')`)
	execSQL(t, db, `INSERT INTO places VALUES (2, '{"coordinates":[1,2],"type":"Point"}')`)

	rs := execSQL(t, db, `SELECT id, geom FROM places ORDER BY id`)
	a, _ := rs.Rows[0]["geom"].(string)
	b, _ := rs.Rows[1]["geom"].(string)
	if a == "" || b == "" {
		t.Fatalf("expected both rows to have a stored geometry string, got %q and %q", a, b)
	}
	if a != b {
		t.Errorf("canonicalization is not key-order independent: %q != %q", a, b)
	}
}
