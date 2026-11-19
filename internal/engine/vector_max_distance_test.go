package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVectorMaxDistance(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE chunks (id INT PRIMARY KEY, doc TEXT, chunk_no INT, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO chunks VALUES
		(1,'a',0,'near','[1,0]'),
		(2,'a',1,'neighbor','[4,4]'),
		(3,'b',0,'needle','[9,0]'),
		(4,'c',0,'near','[1,0]'),
		(5,'d',0,'tie','[4,4]'),
		(6,'e',0,'other','[0,1]')`)

	t.Run("inclusive L2 in output units after metadata filter", func(t *testing.T) {
		rs := execSQL(t, db, `SELECT id, _vec_distance, _vec_rank FROM VEC_SEARCH_FILTERED('chunks','embedding','[1,0]',10,
			'{"metric":"l2","max_distance":5,"pre_filter":{"allowed_row_ids":[2,3,5]}}') ORDER BY _vec_rank`)
		want := []Row{
			{"id": 2, "_vec_distance": 5.0, "_vec_rank": 1},
			{"id": 5, "_vec_distance": 5.0, "_vec_rank": 2},
		}
		for i := range rs.Rows {
			rs.Rows[i]["id"] = expectAsInt(t, rs.Rows[i]["id"])
		}
		if !reflect.DeepEqual(rs.Rows, want) {
			t.Fatalf("got %#v, want %#v", rs.Rows, want)
		}
	})

	t.Run("signed dot distance", func(t *testing.T) {
		rs := execSQL(t, db, `SELECT id, _vec_distance FROM RAG_SEARCH('chunks','embedding','[1,0]',10,'{"metric":"dot","max_distance":-9}')`)
		if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 3 || rs.Rows[0]["_vec_distance"] != -9.0 {
			t.Fatalf("got %#v, want row 3 exactly at distance -9", rs.Rows)
		}
	})

	t.Run("context can include farther neighbors", func(t *testing.T) {
		rs := execSQL(t, db, `SELECT id, _context_offset FROM RAG_SEARCH('chunks','embedding','[1,0]',10,
			'{"metric":"l2","max_distance":0,"expand_after":1,"doc_id_column":"doc","chunk_index_column":"chunk_no",
			  "pre_filter":{"allowed_row_ids":[1,2,3]}}') ORDER BY _context_rank`)
		if len(rs.Rows) != 2 || expectAsInt(t, rs.Rows[0]["id"]) != 1 || expectAsInt(t, rs.Rows[1]["id"]) != 2 ||
			expectAsInt(t, rs.Rows[1]["_context_offset"]) != 1 {
			t.Fatalf("got %#v, want one hit and its allowed neighbor", rs.Rows)
		}
	})

	t.Run("hybrid text candidates remain eligible", func(t *testing.T) {
		rs := execSQL(t, db, `SELECT id, _vec_rank, _fts_rank FROM HYBRID_SEARCH('chunks','embedding','body','needle','[1,0]',4,
			'{"metric":"l2","max_distance":0}') ORDER BY id`)
		if len(rs.Rows) != 3 || expectAsInt(t, rs.Rows[1]["id"]) != 3 ||
			rs.Rows[1]["_vec_rank"] != nil || expectAsInt(t, rs.Rows[1]["_fts_rank"]) != 1 {
			t.Fatalf("got %#v, want row 3 retained by the lexical branch", rs.Rows)
		}
	})

	t.Run("cache and ANN keep the same candidate ranking", func(t *testing.T) {
		ConfigureVectorCache(VectorCacheConfig{ResultCacheEntries: 32})
		t.Cleanup(func() { ConfigureVectorCache(DefaultVectorCacheConfig()) })
		for _, index := range []string{"flat", "ivf", "hnsw"} {
			prefix := fmt.Sprintf(`SELECT id, _vec_rank FROM RAG_SEARCH('chunks','embedding','[1,0]',4,'{"metric":"l2","index":"%s"`, index)
			all := execSQL(t, db, prefix+`}') ORDER BY _vec_rank`)
			limited := execSQL(t, db, prefix+`,"max_distance":1}') ORDER BY _vec_rank`)
			control := execSQL(t, db, prefix+`}') WHERE _vec_distance <= 1 ORDER BY _vec_rank`)
			unlimited := execSQL(t, db, prefix+`,"max_distance":null}') ORDER BY _vec_rank`)
			if len(limited.Rows) == 0 || len(limited.Rows) >= len(all.Rows) ||
				!reflect.DeepEqual(limited.Rows, control.Rows) || !reflect.DeepEqual(unlimited.Rows, all.Rows) {
				t.Fatalf("%s: limited %v, control %v, null limit %v, unrestricted %v", index, limited.Rows, control.Rows, unlimited.Rows, all.Rows)
			}
		}
		if hits := VectorCacheAnalytics().Hits; hits != 9 {
			t.Fatalf("got %d cache hits, want 9", hits)
		}
	})

	t.Run("validation", func(t *testing.T) {
		for _, value := range []string{`"near"`, "-1", "1e400"} {
			execExpectError(t, db, `SELECT id FROM RAG_SEARCH('chunks','embedding','[1,0]',10,'{"max_distance":`+value+`}')`)
		}
	})
}

// Compare two SQL queries with identical vector ranking and two surviving
// hits. The cutoff avoids formatting the other 126 rows in the top-k window.
func BenchmarkVectorMaxDistance(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("cutoff_chunks", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "doc_id", Type: storage.TextType},
		{Name: "chunk_index", Type: storage.IntType},
		{Name: "heading", Type: storage.TextType},
		{Name: "chunk_text", Type: storage.TextType},
		{Name: "search_text", Type: storage.TextType},
		{Name: "category", Type: storage.TextType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	for i := range 512 {
		embedding := make([]float64, 96)
		embedding[0] = float64(i)
		table.Rows = append(table.Rows, []any{i, "doc", i, "Heading", "Chunk text", "Search terms", "guide", embedding})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	const base = `SELECT id, _vec_distance FROM RAG_SEARCH('cutoff_chunks','embedding',`
	query := base + `'[` + strings.Repeat("0,", 95) + `0]',128,'{"metric":"l2"`
	b.Run("WHERE", func(b *testing.B) {
		runRAGBench(b, db, query+`}') WHERE _vec_distance <= 1`, 2)
	})
	b.Run("max_distance", func(b *testing.B) {
		runRAGBench(b, db, query+`,"max_distance":1}')`, 2)
	})
}
