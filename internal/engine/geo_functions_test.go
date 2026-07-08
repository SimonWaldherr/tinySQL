package engine

import (
	"context"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoDistanceCoordinates(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT GEO_DISTANCE(52.5200, 13.4050, 48.1372, 11.5755) AS dist
	`))
	if err != nil {
		t.Fatalf("GEO_DISTANCE failed: %v", err)
	}
	dist, ok := rs.Rows[0]["dist"].(float64)
	if !ok {
		t.Fatalf("dist = %T, want float64", rs.Rows[0]["dist"])
	}
	if math.Abs(dist-504000) > 3000 {
		t.Fatalf("Berlin-Munich distance = %v, want about 504km", dist)
	}
}

func TestGeoPointAccessorsAndDistanceAliases(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			ST_X(ST_MakePoint(13.4050, 52.5200)) AS lon,
			ST_Y(ST_MakePoint(13.4050, 52.5200)) AS lat,
			ST_DISTANCE(ST_MakePoint(13.4050, 52.5200), ST_MakePoint(11.5755, 48.1372)) AS dist,
			ST_DWITHIN(ST_MakePoint(13.4050, 52.5200), ST_MakePoint(11.5755, 48.1372), 600000) AS close
	`))
	if err != nil {
		t.Fatalf("geo aliases failed: %v", err)
	}
	expectFloat(t, rs.Rows[0]["lon"], 13.4050, 1e-9, "ST_X")
	expectFloat(t, rs.Rows[0]["lat"], 52.5200, 1e-9, "ST_Y")
	dist := rs.Rows[0]["dist"].(float64)
	if math.Abs(dist-504000) > 3000 {
		t.Fatalf("ST_DISTANCE = %v, want about 504km", dist)
	}
	if got, ok := rs.Rows[0]["close"].(bool); !ok || !got {
		t.Fatalf("ST_DWITHIN = %#v, want true", rs.Rows[0]["close"])
	}
}

func TestGeoWithinBBoxOnTableGeometry(t *testing.T) {
	db := storage.NewDB()
	for _, sql := range []string{
		`CREATE TABLE places (name TEXT, geometry JSON)`,
		`INSERT INTO places VALUES ('Berlin', GEO_POINT(13.4050, 52.5200))`,
		`INSERT INTO places VALUES ('Zurich', GEO_POINT(8.5417, 47.3769))`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err != nil {
			t.Fatalf("%s failed: %v", sql, err)
		}
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT name FROM places
		WHERE GEO_WITHIN_BBOX(geometry, 13.0, 52.0, 14.0, 53.0)
	`))
	if err != nil {
		t.Fatalf("GEO_WITHIN_BBOX failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Berlin" {
		t.Fatalf("unexpected bbox rows: %#v", rs.Rows)
	}
}
