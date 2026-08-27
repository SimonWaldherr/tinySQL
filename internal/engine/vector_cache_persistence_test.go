package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecColumnCacheExtendsAppendOnlyWithoutCopyingBaseData(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO docs VALUES (1, '[1,0,0]'), (2, '[0,1,0]')`)
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	col, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	purgeVectorCachesFor("default", "docs")
	base := getVecColumnCache("default", table, col, true)
	if len(base.segments) != 1 || len(base.segments[0].data) == 0 {
		t.Fatalf("unexpected base cache: %#v", base)
	}
	baseData := &base.segments[0].data[0]

	execSQL(t, db, `INSERT INTO docs VALUES (3, '[0,0,1]')`)
	extended := getVecColumnCache("default", table, col, true)
	if extended.rowCount() != 3 {
		t.Fatalf("cache rows = %d, want 3", extended.rowCount())
	}
	if len(extended.segments) != 2 {
		t.Fatalf("append cache segments = %d, want base + tail", len(extended.segments))
	}
	if &extended.segments[0].data[0] != baseData {
		t.Fatal("append-only cache extension copied the existing vector buffer")
	}
	if got := extended.vector(2); len(got) != 3 || got[2] != 1 {
		t.Fatalf("appended vector = %v, want [0 0 1]", got)
	}

	rs := execSQL(t, db, `SELECT id FROM VEC_SEARCH('docs', 'embedding', '[0,0,1]', 1, 'cosine', 'flat')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("append-only cache search result = %#v, want id 3", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 3, "append-only cache search result")
}

func TestVecColumnCacheRebuildsAfterStructuralMutation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO docs VALUES (1, '[1,0]'), (2, '[0,1]')`)
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	col, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	purgeVectorCachesFor("default", "docs")
	base := getVecColumnCache("default", table, col, true)
	baseData := &base.segments[0].data[0]

	execSQL(t, db, `UPDATE docs SET embedding = '[0,0]' WHERE id = 1`)
	rebuilt := getVecColumnCache("default", table, col, true)
	if len(rebuilt.segments) != 1 {
		t.Fatalf("structural mutation must rebuild one compact segment, got %d", len(rebuilt.segments))
	}
	if &rebuilt.segments[0].data[0] == baseData {
		t.Fatal("structural mutation reused stale vector data")
	}
	if got := rebuilt.vector(0); len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("rebuilt vector = %v, want [0 0]", got)
	}
}

func TestVecColumnCacheAddsNormsWithoutCopyingVectors(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO docs VALUES (1, '[3,4]')`)
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	col, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	purgeVectorCachesFor("default", "docs")
	withoutNorms := getVecColumnCache("default", table, col, false)
	baseData := &withoutNorms.segments[0].data[0]
	withNorms := getVecColumnCache("default", table, col, true)
	if !withNorms.normsReady || &withNorms.segments[0].data[0] != baseData {
		t.Fatal("adding cosine norms must retain the packed vector segment")
	}
	if got := withNorms.normAt(0); got != 5 {
		t.Fatalf("cached norm = %v, want 5", got)
	}
}

func TestVecHNSWIndexSurvivesSnapshotReopen(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR)`)
	for i := 0; i < 48; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO docs VALUES (%d, '[%d,%d,%d]')`, i, i%5, (i*3)%7, (i*11)%13))
	}
	execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
	before := execSQL(t, db, `SELECT id, _vec_rank FROM VEC_SEARCH('docs', 'embedding', '[1,3,11]', 1, 'cosine', 'hnsw')`)
	if len(before.Rows) != 1 {
		t.Fatalf("source HNSW query returned %#v", before.Rows)
	}
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	col, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	key := vecPersistentHNSWKey(col, "cosine")
	if stored := table.VectorIndexes[key]; stored == nil || stored.BuiltRows != 48 {
		t.Fatalf("warmed HNSW was not persisted on table: %#v", stored)
	}

	data, err := storage.SaveToBytes(db)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.LoadFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	reopenedTable, err := reopened.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	if stored := reopenedTable.VectorIndexes[key]; stored == nil || stored.BuiltRows != 48 {
		t.Fatalf("persisted HNSW missing after reopen: %#v", stored)
	}

	purgeVectorCachesFor("default", "docs")
	hydrated := false
	vecPersistentHNSWHydrateHook = func() { hydrated = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = nil })
	cache := getVecColumnCache("default", reopenedTable, col, true)
	idx, err := getVecHNSWIndex(context.Background(), "default", reopenedTable, col, "cosine", 3, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !hydrated {
		t.Fatal("reopened HNSW did not hydrate persisted topology")
	}
	if len(idx.levels) != 48 {
		t.Fatalf("hydrated HNSW rows = %d, want 48", len(idx.levels))
	}
	rs := execSQL(t, reopened, `SELECT id FROM VEC_SEARCH('docs', 'embedding', '[1,3,11]', 1, 'cosine', 'hnsw')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("hydrated HNSW result = %#v", rs.Rows)
	}
	beforeID, err := toInt(before.Rows[0]["id"])
	if err != nil {
		t.Fatal(err)
	}
	expectInt(t, rs.Rows[0]["id"], beforeID, "hydrated HNSW must retain source graph result")
}
