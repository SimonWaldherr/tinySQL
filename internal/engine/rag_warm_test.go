package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGWarmBuildsVectorAndFTSCaches(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE warm_rag (id TEXT PRIMARY KEY, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO warm_rag VALUES
		('a','offline routing maps','[1,0,0]'),
		('b','hybrid semantic retrieval','[0,1,0]'),
		('c','embedded golang database','[0,0,1]')`)
	t.Cleanup(func() {
		purgeVectorCachesFor("default", "warm_rag")
		purgeFTSCachesFor("default", "warm_rag")
	})

	rs := execSQL(t, db, `SELECT * FROM RAG_WARM(
		'warm_rag','body','embedding','cosine','hnsw')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("RAG_WARM rows = %#v", rs.Rows)
	}
	result := rs.Rows[0]
	if result["row_count"] != 3 || result["vector_count"] != 3 || result["dims"] != 3 || result["fts_valid_docs"] != 3 {
		t.Fatalf("RAG_WARM stats = %#v", result)
	}
	if terms, ok := result["fts_terms"].(int); !ok || terms < 6 {
		t.Fatalf("RAG_WARM fts_terms = %v", result["fts_terms"])
	}

	table, err := db.Get("default", "warm_rag")
	if err != nil {
		t.Fatal(err)
	}
	vectorColumn, _ := table.ColIndex("embedding")
	vectorKey := vecIndexCacheKey{tenant: "default", table: table.Name, colIdx: vectorColumn, metric: "cosine"}
	vecHNSWCacheMu.RLock()
	vectorIndex := vecHNSWCache[vectorKey]
	vecHNSWCacheMu.RUnlock()
	if vectorIndex == nil || vectorIndex.version != table.Version {
		t.Fatalf("RAG_WARM did not populate current HNSW index: %#v", vectorIndex)
	}

	textColumn, _ := table.ColIndex("body")
	fts := getFTSDocCache("default", table, []int{textColumn})
	if fts.numDocs != 3 {
		t.Fatalf("RAG_WARM FTS cache has %d docs", fts.numDocs)
	}
}

func TestRAGWarmRebuildsAfterCorpusMutation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE warm_rag_update (id TEXT, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO warm_rag_update VALUES ('a','first document','[1,0]')`)
	query := `SELECT * FROM RAG_WARM('warm_rag_update','body','embedding')`
	if got := execSQL(t, db, query).Rows[0]["row_count"]; got != 1 {
		t.Fatalf("first row_count = %v", got)
	}
	execSQL(t, db, `INSERT INTO warm_rag_update VALUES ('b','second document','[0,1]')`)
	result := execSQL(t, db, query).Rows[0]
	if result["row_count"] != 2 || result["fts_valid_docs"] != 2 {
		t.Fatalf("updated RAG_WARM stats = %#v", result)
	}
}
