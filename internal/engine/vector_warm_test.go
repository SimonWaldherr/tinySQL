package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupWarmTable(t *testing.T, db *storage.DB, rows int) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE docs (id INT, body TEXT, emb VECTOR)`)
	for i := 0; i < rows; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO docs (id, body, emb) VALUES (%d, 'doc %d', '[%d.0, %d.0, %d.0]')`,
			i, i, i, i+1, i+2))
	}
}

func TestVecWarmBuildsIndex(t *testing.T) {
	db := storage.NewDB()
	setupWarmTable(t, db, 30)

	rs := execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'cosine', 'hnsw')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 stats row, got %d", len(rs.Rows))
	}
	row := rs.Rows[0]
	expectInt(t, row["row_count"], 30, "row_count")
	expectInt(t, row["vector_count"], 30, "vector_count")
	expectInt(t, row["dims"], 3, "dims")
	if row["index_mode"] != "hnsw" {
		t.Fatalf("index_mode = %v, want hnsw", row["index_mode"])
	}

	// The HNSW index must now be cached at the current table version.
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	colIdx, err := table.ColIndex("emb")
	if err != nil {
		t.Fatal(err)
	}
	key := vecIndexCacheKey{tenant: "default", table: table.Name, colIdx: colIdx, metric: "cosine"}
	vecHNSWCacheMu.RLock()
	idx := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idx == nil || idx.version != table.Version {
		t.Fatalf("expected warmed HNSW index at version %d, got %+v", table.Version, idx)
	}

	// A subsequent search must succeed and use the warmed structures.
	rs = execSQL(t, db, `SELECT id, _vec_rank FROM VEC_SEARCH('docs', 'emb', '[5.0, 6.0, 7.0]', 3, 'cosine', 'hnsw')`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 results, got %d", len(rs.Rows))
	}
}

func TestVecWarmIVFAndDefaults(t *testing.T) {
	db := storage.NewDB()
	setupWarmTable(t, db, 12)

	// Defaults: cosine metric, flat index (cache + norms only).
	rs := execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb')`)
	expectInt(t, rs.Rows[0]["vector_count"], 12, "vector_count")

	// IVF with non-cosine metric.
	rs = execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'l2', 'ivf')`)
	if rs.Rows[0]["metric"] != "l2" || rs.Rows[0]["index_mode"] != "ivf" {
		t.Fatalf("unexpected stats row: %+v", rs.Rows[0])
	}
}

func TestVecWarmMixedDimensionalityReported(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT, body TEXT, emb VECTOR)`)
	// 5 rows with the "old" 3-dim embedding, then 2 rows with a "new" 4-dim
	// embedding — simulating an embedding-model migration mid-flight.
	for i := 0; i < 5; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO docs (id, body, emb) VALUES (%d, 'doc %d', '[%d.0, %d.0, %d.0]')`,
			i, i, i, i+1, i+2))
	}
	for i := 5; i < 7; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO docs (id, body, emb) VALUES (%d, 'doc %d', '[%d.0, %d.0, %d.0, %d.0]')`,
			i, i, i, i+1, i+2, i+3))
	}

	rs := execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'cosine', 'hnsw')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 stats row, got %d", len(rs.Rows))
	}
	row := rs.Rows[0]
	expectInt(t, row["row_count"], 7, "row_count")
	expectInt(t, row["vector_count"], 7, "vector_count")
	expectInt(t, row["dims"], 3, "dims")
	expectInt(t, row["distinct_dims"], 2, "distinct_dims")
	expectInt(t, row["excluded_rows"], 2, "excluded_rows")
}

func TestVecWarmErrors(t *testing.T) {
	db := storage.NewDB()
	setupWarmTable(t, db, 3)

	for _, sql := range []string{
		`SELECT * FROM VEC_WARM('missing', 'emb')`,
		`SELECT * FROM VEC_WARM('docs', 'missing')`,
		`SELECT * FROM VEC_WARM('docs', 'emb', 'bogus')`,
		`SELECT * FROM VEC_WARM('docs', 'emb', 'cosine', 'bogus')`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err == nil {
			t.Errorf("expected error for %s", sql)
		}
	}
}
