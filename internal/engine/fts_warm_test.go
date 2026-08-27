package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSWarmBuildsSearchCompatibleCache(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE warm_docs (id INT, title TEXT, body TEXT)`)
	execSQL(t, db, `INSERT INTO warm_docs VALUES (1, 'Introduction', 'ordinary background text')`)
	execSQL(t, db, `INSERT INTO warm_docs VALUES (2, 'Needle', 'the retrieval needle is here')`)
	execSQL(t, db, `INSERT INTO warm_docs VALUES (3, 'Reference', 'more ordinary background text')`)

	warm := execSQL(t, db, `SELECT * FROM FTS_WARM('warm_docs', 'title', 'body')`)
	if len(warm.Rows) != 1 {
		t.Fatalf("FTS_WARM returned %d rows, want 1", len(warm.Rows))
	}
	stats := warm.Rows[0]
	if stats["table_name"] != "warm_docs" {
		t.Fatalf("table_name = %v, want warm_docs", stats["table_name"])
	}
	if stats["columns"] != "title,body" {
		t.Fatalf("columns = %q, want title,body", stats["columns"])
	}
	expectInt(t, stats["row_count"], 3, "row_count")
	expectInt(t, stats["valid_docs"], 3, "valid_docs")
	if terms, err := toInt(stats["term_count"]); err != nil || terms == 0 {
		t.Fatalf("term_count = %v, want positive integer (err=%v)", stats["term_count"], err)
	}
	if postings, err := toInt(stats["posting_count"]); err != nil || postings == 0 {
		t.Fatalf("posting_count = %v, want positive integer (err=%v)", stats["posting_count"], err)
	}
	if tokens, err := toInt(stats["token_count"]); err != nil || tokens == 0 {
		t.Fatalf("token_count = %v, want positive integer (err=%v)", stats["token_count"], err)
	}
	if avg, ok := stats["avg_doc_len"].(float64); !ok || avg <= 0 {
		t.Fatalf("avg_doc_len = %v, want positive float64", stats["avg_doc_len"])
	}

	table, err := db.Get("default", "warm_docs")
	if err != nil {
		t.Fatal(err)
	}
	titleIdx, err := table.ColIndex("title")
	if err != nil {
		t.Fatal(err)
	}
	bodyIdx, err := table.ColIndex("body")
	if err != nil {
		t.Fatal(err)
	}
	key := ftsDocCacheKey{
		tenant: "default",
		table:  table.Name,
		cols:   ftsColsCacheKey([]int{titleIdx, bodyIdx}),
	}
	ftsDocCacheMu.RLock()
	entry, ok := ftsDocCache[key]
	ftsDocCacheMu.RUnlock()
	if !ok || entry.table != table || entry.version != table.Version {
		t.Fatalf("expected a current FTS cache entry for %+v, got %+v", key, entry)
	}
	if len(entry.docs) != 3 || entry.numDocs != 3 {
		t.Fatalf("warm cache docs = %d/%d, want 3/3", len(entry.docs), entry.numDocs)
	}

	// FTS_SEARCH with the same ordered columns must hit precisely the warmed
	// cache entry rather than build a second, subtly different index.
	search := execSQL(t, db, `SELECT id, _fts_rank FROM FTS_SEARCH('warm_docs', 'needle', 3, 'title', 'body')`)
	if len(search.Rows) != 1 || search.Rows[0]["id"] != 2 {
		t.Fatalf("FTS_SEARCH after warm returned %#v, want id 2", search.Rows)
	}
	ftsDocCacheMu.RLock()
	after := ftsDocCache[key]
	ftsDocCacheMu.RUnlock()
	if len(after.docs) == 0 || &after.docs[0] != &entry.docs[0] {
		t.Fatal("FTS_SEARCH did not reuse the warmed document cache")
	}

	// An append advances table.Version. FTS_WARM must extend/update the cache
	// before traffic arrives rather than leave the next search to discover it.
	execSQL(t, db, `INSERT INTO warm_docs VALUES (4, 'Later', 'another needle occurrence')`)
	warm = execSQL(t, db, `SELECT * FROM FTS_WARM('warm_docs', 'title', 'body')`)
	expectInt(t, warm.Rows[0]["row_count"], 4, "row_count after append")
	expectInt(t, warm.Rows[0]["valid_docs"], 4, "valid_docs after append")
	ftsDocCacheMu.RLock()
	after = ftsDocCache[key]
	ftsDocCacheMu.RUnlock()
	if after.version != table.Version || len(after.docs) != 4 {
		t.Fatalf("extended cache = version %d, docs %d; want version %d, docs 4", after.version, len(after.docs), table.Version)
	}
}

func TestFTSWarmUsesFTSSearchDefaultColumnSemantics(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE warm_default (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO warm_default VALUES (1, 'default column cache')`)

	// No optional columns means every column, as in FTS_SEARCH.
	stats := execSQL(t, db, `SELECT * FROM FTS_WARM('warm_default')`).Rows[0]
	if stats["columns"] != "id,body" {
		t.Fatalf("default FTS_WARM columns = %q, want id,body", stats["columns"])
	}

	// FTS_SEARCH historically ignores an unknown explicit column and falls
	// back to every column. Preserve that compatibility in the warm-up API so
	// a warm call never prepares a different key from the corresponding query.
	stats = execSQL(t, db, `SELECT * FROM FTS_WARM('warm_default', 'missing')`).Rows[0]
	if stats["columns"] != "id,body" {
		t.Fatalf("fallback FTS_WARM columns = %q, want id,body", stats["columns"])
	}
}

func TestFTSWarmErrors(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE warm_errors (body TEXT)`)
	for _, sql := range []string{
		`SELECT * FROM FTS_WARM('missing')`,
		`SELECT * FROM FTS_WARM(42)`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err == nil {
			t.Errorf("expected FTS_WARM error for %s", sql)
		}
	}
}
