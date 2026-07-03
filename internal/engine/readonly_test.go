package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestReadOnlyModeRejectsMutations(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kv (k TEXT, v INT)`)
	execSQL(t, db, `INSERT INTO kv (k, v) VALUES ('a', 1)`)

	db.SetReadOnly(true)

	blocked := []string{
		`INSERT INTO kv (k, v) VALUES ('b', 2)`,
		`UPDATE kv SET v = 9 WHERE k = 'a'`,
		`DELETE FROM kv WHERE k = 'a'`,
		`CREATE TABLE other (id INT)`,
		`DROP TABLE kv`,
		`ALTER TABLE kv ADD COLUMN extra TEXT`,
	}
	for _, sql := range blocked {
		_, err := Execute(context.Background(), db, "default", mustParse(sql))
		if err == nil {
			t.Errorf("expected read-only error for %s", sql)
			continue
		}
		if !strings.Contains(err.Error(), "read-only") {
			t.Errorf("expected read-only error for %s, got: %v", sql, err)
		}
	}

	// Reads still work.
	rs := execSQL(t, db, `SELECT v FROM kv WHERE k = 'a'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}

	// Toggling back re-enables writes.
	db.SetReadOnly(false)
	execSQL(t, db, `INSERT INTO kv (k, v) VALUES ('b', 2)`)
	if !db.HealthCheck().OK {
		t.Fatal("health check failed")
	}
}

func TestReadOnlyModeAllowsVectorSearch(t *testing.T) {
	db := storage.NewDB()
	setupWarmTable(t, db, 20)

	// Warm indexes, then freeze.
	execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'cosine', 'ivf')`)
	db.SetReadOnly(true)

	rs := execSQL(t, db, `SELECT id FROM VEC_SEARCH('docs', 'emb', '[3.0, 4.0, 5.0]', 5, 'cosine', 'ivf')`)
	if len(rs.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rs.Rows))
	}
	// VEC_WARM itself is a read (cache/index build), so it must still work.
	execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'l2', 'hnsw')`)
}

func TestOpenDBReadOnlyConfig(t *testing.T) {
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeMemory, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.IsReadOnly() {
		t.Fatal("expected IsReadOnly() = true")
	}
	if !db.HealthCheck().ReadOnly {
		t.Fatal("expected HealthCheck().ReadOnly = true")
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE x (id INT)`)); err == nil {
		t.Fatal("expected read-only error")
	}
}
