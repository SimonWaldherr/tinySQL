package engine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeIndexSQL(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return rs
}

func TestCompositeSecondaryIndexPointAndPrefixSeek(t *testing.T) {
	db := storage.NewDB()
	executeIndexSQL(t, db, `CREATE TABLE map (zoom_level INT, tile_column INT, tile_row INT, tile_id TEXT)`)
	executeIndexSQL(t, db, `INSERT INTO map VALUES (9, 270, 174, 'a'), (9, 270, 175, 'b'), (9, 271, 174, 'c'), (10, 540, 350, 'd')`)
	executeIndexSQL(t, db, `CREATE UNIQUE INDEX idx_map_zxy ON map(zoom_level, tile_column, tile_row)`)

	table, err := db.Get("default", "map")
	if err != nil {
		t.Fatal(err)
	}
	idx := table.FindSecondaryIndex([]string{"zoom_level", "tile_column", "tile_row"})
	if idx == nil || len(idx.Entries) != 4 {
		t.Fatalf("materialized index = %#v", idx)
	}

	point := executeIndexSQL(t, db, `SELECT tile_id FROM map WHERE zoom_level = 9 AND tile_column = 270 AND tile_row = 175`)
	if len(point.Rows) != 1 || point.Rows[0]["tile_id"] != "b" {
		t.Fatalf("point lookup = %#v", point.Rows)
	}
	noHit := executeIndexSQL(t, db, `SELECT tile_id FROM map WHERE zoom_level = 9 AND tile_column = 270 AND tile_row = 999`)
	if len(noHit.Rows) != 0 {
		t.Fatalf("negative lookup = %#v", noHit.Rows)
	}
	prefix := executeIndexSQL(t, db, `SELECT tile_id FROM map WHERE zoom_level = 9 AND tile_column = 270`)
	if len(prefix.Rows) != 2 || prefix.Rows[0]["tile_id"] != "a" || prefix.Rows[1]["tile_id"] != "b" {
		t.Fatalf("prefix lookup = %#v", prefix.Rows)
	}

	explain := executeIndexSQL(t, db, `EXPLAIN SELECT tile_id FROM map WHERE zoom_level = 9 AND tile_column = 270 AND tile_row = 175`)
	found := false
	for _, row := range explain.Rows {
		if row["operation"] == "INDEX POINT SEEK" && strings.Contains(row["detail"].(string), "index=idx_map_zxy") && strings.Contains(row["detail"].(string), "covering_index=false") {
			found = true
		}
	}
	if !found {
		t.Fatalf("EXPLAIN did not report composite index seek: %#v", explain.Rows)
	}

	analyze := executeIndexSQL(t, db, `EXPLAIN ANALYZE SELECT tile_id FROM map WHERE zoom_level = 9 AND tile_column = 270 AND tile_row = 175`)
	if len(analyze.Rows) == 0 || analyze.Rows[len(analyze.Rows)-1]["operation"] != "ANALYZE" {
		t.Fatalf("EXPLAIN ANALYZE result = %#v", analyze.Rows)
	}
}

func TestSecondaryIndexUniqueInvalidationAndSnapshotPersistence(t *testing.T) {
	db := storage.NewDB()
	executeIndexSQL(t, db, `CREATE TABLE map (zoom_level INT, tile_column INT, tile_row INT, tile_id TEXT)`)
	executeIndexSQL(t, db, `INSERT INTO map VALUES (1, 2, 3, 'one')`)
	executeIndexSQL(t, db, `CREATE UNIQUE INDEX idx_map_zxy ON map(zoom_level, tile_column, tile_row)`)
	stmt, err := NewParser(`INSERT INTO map VALUES (1, 2, 3, 'duplicate')`).ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("duplicate composite key unexpectedly inserted")
	}
	executeIndexSQL(t, db, `INSERT INTO map VALUES (1, 2, 4, 'two')`)
	executeIndexSQL(t, db, `DELETE FROM map WHERE tile_id = 'one'`)
	got := executeIndexSQL(t, db, `SELECT tile_id FROM map WHERE zoom_level = 1 AND tile_column = 2 AND tile_row = 4`)
	if len(got.Rows) != 1 || got.Rows[0]["tile_id"] != "two" {
		t.Fatalf("lookup after invalidation = %#v", got.Rows)
	}

	path := filepath.Join(t.TempDir(), "snapshot.gob")
	if err := storage.SaveToFile(db, path); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Close releases the WAL handle LoadFromFile attaches; without this the
	// TempDir cleanup fails on Windows (file in use).
	defer reopened.Close()
	table, err := reopened.Get("default", "map")
	if err != nil {
		t.Fatal(err)
	}
	if table.FindSecondaryIndex([]string{"zoom_level", "tile_column", "tile_row"}) == nil {
		t.Fatal("secondary index missing after snapshot reopen")
	}
	got = executeIndexSQL(t, reopened, `SELECT tile_id FROM map WHERE zoom_level = 1 AND tile_column = 2 AND tile_row = 4`)
	if len(got.Rows) != 1 || got.Rows[0]["tile_id"] != "two" {
		t.Fatalf("reopened lookup = %#v", got.Rows)
	}
}
